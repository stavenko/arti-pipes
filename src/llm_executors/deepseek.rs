//! DeepSeek LLM provider

use crate::error::{ExecutionError, Result};
use crate::llm_executors::types::{
    ChatMessage, ExecutionResult, JsonSchemaRequest, LlmType, Output, OutputMetadata,
    ResponseFormatRequest, Token, TokenStream,
};
use futures::StreamExt;
use reqwest::Client;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::time::Instant;
use tokio::sync::mpsc;
use tokio_stream::wrappers::UnboundedReceiverStream;

/// Configuration for DeepSeek LLM
#[derive(Debug, Clone)]
pub struct DeepSeekConfig {
    pub api_base: String,
    pub api_key: String,
    pub model: String,
    pub reasoning: bool,
}

/// Builder for DeepSeek executor
pub struct DeepSeekBuilder {
    api_base: Option<String>,
    api_key: Option<String>,
    model: Option<String>,
    reasoning: bool,
}

impl DeepSeekBuilder {
    pub fn new() -> Self {
        Self {
            api_base: None,
            api_key: None,
            model: None,
            reasoning: false,
        }
    }

    pub fn api_base(mut self, api_base: impl Into<String>) -> Self {
        self.api_base = Some(api_base.into());
        self
    }

    pub fn api_key(mut self, api_key: impl Into<String>) -> Self {
        self.api_key = Some(api_key.into());
        self
    }

    pub fn model(mut self, model: impl Into<String>) -> Self {
        self.model = Some(model.into());
        self
    }

    pub fn reasoning(mut self, reasoning: bool) -> Self {
        self.reasoning = reasoning;
        self
    }

    pub fn build(self) -> DeepSeek {
        DeepSeek {
            client: Client::new(),
            config: DeepSeekConfig {
                api_base: self.api_base.expect("api_base is required"),
                api_key: self.api_key.unwrap_or_default(),
                model: self.model.expect("model is required"),
                reasoning: self.reasoning,
            },
        }
    }
}

impl Default for DeepSeekBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// DeepSeek chat completion request
#[derive(Debug, Serialize)]
struct DeepSeekChatCompletionRequest {
    model: String,
    messages: Vec<ChatMessage>,
    #[serde(skip_serializing_if = "Option::is_none")]
    response_format: Option<ResponseFormatRequest>,
    stream: bool,
}

/// DeepSeek-specific streaming delta that includes reasoning
#[derive(Debug, Deserialize, Default)]
struct DeepSeekDelta {
    /// Reasoning content (Ollama uses 'reasoning', official DeepSeek API uses 'reasoning_content')
    #[serde(default, alias = "reasoning_content")]
    reasoning: Option<String>,
    #[serde(default)]
    content: Option<String>,
}

/// DeepSeek-specific streaming choice
#[derive(Debug, Deserialize)]
struct DeepSeekChoice {
    delta: DeepSeekDelta,
    #[allow(dead_code)]
    finish_reason: Option<String>,
}

/// DeepSeek-specific streaming chunk
#[derive(Debug, Deserialize)]
struct DeepSeekChunk {
    choices: Vec<DeepSeekChoice>,
}

/// Parse DeepSeek SSE line
fn parse_deepseek_sse_line(line: &str) -> Option<DeepSeekChunk> {
    let line = line.trim();
    if let Some(data) = line.strip_prefix("data: ") {
        if data == "[DONE]" {
            return None;
        }
        serde_json::from_str(data).ok()
    } else {
        None
    }
}

/// DeepSeek LLM provider
pub struct DeepSeek {
    client: Client,
    config: DeepSeekConfig,
}

impl DeepSeek {
    /// Create a new builder for DeepSeek
    pub fn builder() -> DeepSeekBuilder {
        DeepSeekBuilder::new()
    }

    fn build_url(&self) -> String {
        let base = self.config.api_base.trim_end_matches('/');
        format!("{}/chat/completions", base)
    }
}

impl LlmType for DeepSeek {
    async fn execute_raw(&self, prompt: String) -> Result<ExecutionResult<String>> {
        self.execute_internal(prompt, None).await
    }

    async fn execute<T: JsonSchema>(&self, prompt: String) -> Result<ExecutionResult<String>> {
        let schema = schemars::schema_for!(T);
        let schema_value = serde_json::to_value(&schema).map_err(|e| {
            ExecutionError::Serialization(format!("Failed to serialize schema: {}", e))
        })?;

        let response_format = ResponseFormatRequest {
            r#type: "json_schema".to_string(),
            json_schema: Some(JsonSchemaRequest {
                name: "response".to_string(),
                schema: schema_value,
                strict: true,
            }),
        };

        self.execute_internal(prompt, Some(response_format)).await
    }
}

impl DeepSeek {
    async fn execute_internal(
        &self,
        prompt: String,
        response_format: Option<ResponseFormatRequest>,
    ) -> Result<ExecutionResult<String>> {
        let url = self.build_url();
        let model = self.config.model.clone();
        let api_key = self.config.api_key.clone();
        let client = self.client.clone();
        let reasoning_enabled = self.config.reasoning;

        let (thinking_tx, thinking_rx) = mpsc::unbounded_channel::<Result<Token>>();
        let (content_tx, content_rx) = mpsc::unbounded_channel::<Result<Token>>();
        let thinking_stream: TokenStream = Box::pin(UnboundedReceiverStream::new(thinking_rx));
        let content_stream: TokenStream = Box::pin(UnboundedReceiverStream::new(content_rx));

        let output_handle = tokio::spawn(async move {
            let start_time = Instant::now();

            let messages = vec![ChatMessage::user(prompt)];
            let mut thinking_token_index = 0;
            let mut content_token_index = 0;

            let request_body = DeepSeekChatCompletionRequest {
                model: model.clone(),
                messages,
                response_format,
                stream: true,
            };

            let mut request = client.post(&url).json(&request_body);

            if !api_key.is_empty() {
                request = request.header("Authorization", format!("Bearer {}", api_key));
            }

            let response = request
                .send()
                .await
                .map_err(|e| ExecutionError::ModelExecution(e.to_string()))?;

            if !response.status().is_success() {
                let status = response.status();
                let text = response.text().await.unwrap_or_default();
                return Err(ExecutionError::ModelExecution(format!(
                    "HTTP {}: {}",
                    status, text
                )));
            }

            let mut stream_response = response.bytes_stream();
            let mut response_content = String::new();
            let mut buffer = String::new();

            while let Some(chunk_result) = stream_response.next().await {
                let chunk =
                    chunk_result.map_err(|e| ExecutionError::ModelExecution(e.to_string()))?;

                buffer.push_str(&String::from_utf8_lossy(&chunk));

                while let Some(newline_pos) = buffer.find('\n') {
                    let line = buffer[..newline_pos].to_string();
                    buffer = buffer[newline_pos + 1..].to_string();

                    if let Some(parsed) = parse_deepseek_sse_line(&line) {
                        for choice in parsed.choices {
                            if reasoning_enabled {
                                if let Some(reasoning) = choice.delta.reasoning {
                                    if !reasoning.is_empty() {
                                        let token = Token {
                                            content: reasoning,
                                            index: thinking_token_index,
                                        };
                                        thinking_token_index += 1;
                                        let _ = thinking_tx.send(Ok(token));
                                    }
                                }
                            }

                            if let Some(content) = choice.delta.content {
                                if !content.is_empty() {
                                    response_content.push_str(&content);
                                    let token = Token {
                                        content,
                                        index: content_token_index,
                                    };
                                    content_token_index += 1;
                                    let _ = content_tx.send(Ok(token));
                                }
                            }
                        }
                    }
                }
            }

            let generation_time_ms = start_time.elapsed().as_millis() as u64;

            let metadata = OutputMetadata {
                total_tokens: content_token_index,
                generation_time_ms,
                model_id: model,
            };

            Ok(Output::new(response_content, metadata))
        });

        Ok(ExecutionResult {
            thinking_stream,
            content_stream,
            output: output_handle,
        })
    }
}
