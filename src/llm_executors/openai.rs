//! OpenAI LLM provider

use crate::error::{ExecutionError, Result};
use crate::executor::{ExecutionResult, Token, TokenStream};
use crate::llm_executors::types::{ChatMessage, JsonSchemaRequest, ResponseFormatRequest};
use crate::platform::spawn_output;
use crate::transport::{
    run_chat_completion, CompletionOptions, DefaultTransport, HttpRequest, HttpTransport,
};
use schemars::JsonSchema;
use serde::Serialize;
use tokio::sync::mpsc;
use tokio_stream::wrappers::UnboundedReceiverStream;

/// Configuration for OpenAI LLM
#[derive(Debug, Clone)]
pub struct OpenAIConfig {
    pub api_base: String,
    pub api_key: String,
    pub model: String,
    pub reasoning_effort: Option<String>,
    pub max_tokens: Option<u32>,
}

/// Builder for OpenAI executor
pub struct OpenAIBuilder {
    api_base: Option<String>,
    api_key: Option<String>,
    model: Option<String>,
    reasoning_effort: Option<String>,
    max_tokens: Option<u32>,
}

impl OpenAIBuilder {
    pub fn new() -> Self {
        Self {
            api_base: None,
            api_key: None,
            model: None,
            reasoning_effort: None,
            max_tokens: None,
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

    pub fn reasoning_effort(mut self, effort: impl Into<String>) -> Self {
        self.reasoning_effort = Some(effort.into());
        self
    }

    /// Maximum number of tokens to generate in the completion.
    pub fn max_tokens(mut self, max_tokens: u32) -> Self {
        self.max_tokens = Some(max_tokens);
        self
    }

    fn into_config(self) -> OpenAIConfig {
        OpenAIConfig {
            api_base: self.api_base.expect("api_base is required"),
            api_key: self.api_key.unwrap_or_default(),
            model: self.model.expect("model is required"),
            reasoning_effort: self.reasoning_effort,
            max_tokens: self.max_tokens,
        }
    }

    /// Build the executor with the platform's default transport.
    pub fn build(self) -> OpenAI {
        OpenAI {
            transport: DefaultTransport::new(),
            config: self.into_config(),
        }
    }

    /// Build the executor with a custom transport (e.g. a mock in tests).
    pub fn build_with_transport<T: HttpTransport>(self, transport: T) -> OpenAI<T> {
        OpenAI {
            transport,
            config: self.into_config(),
        }
    }
}

impl Default for OpenAIBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// OpenAI-specific chat completion request with `reasoning_effort` field
#[derive(Debug, Serialize)]
struct OpenAIChatCompletionRequest {
    model: String,
    messages: Vec<ChatMessage>,
    #[serde(skip_serializing_if = "Option::is_none")]
    response_format: Option<ResponseFormatRequest>,
    stream: bool,
    /// OpenAI o1/o3: reasoning effort ("low", "medium", "high")
    #[serde(skip_serializing_if = "Option::is_none")]
    reasoning_effort: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    max_tokens: Option<u32>,
}

/// OpenAI LLM provider, generic over the HTTP transport.
///
/// Defaults to the platform's [`DefaultTransport`]; tests can inject a mock by
/// building via [`OpenAIBuilder::build_with_transport`].
#[derive(Clone)]
pub struct OpenAI<T = DefaultTransport> {
    transport: T,
    config: OpenAIConfig,
}

impl OpenAI<DefaultTransport> {
    /// Create a new builder for OpenAI
    pub fn builder() -> OpenAIBuilder {
        OpenAIBuilder::new()
    }
}

impl<T> OpenAI<T> {
    fn build_url(&self) -> String {
        let base = self.config.api_base.trim_end_matches('/');
        format!("{}/chat/completions", base)
    }
}

impl<T: HttpTransport> crate::executor::PromptExecutor for OpenAI<T> {
    async fn execute_raw(&self, prompt: String) -> Result<ExecutionResult<String>> {
        self.execute_internal(prompt, None).await
    }

    async fn execute<S: JsonSchema>(&self, prompt: String) -> Result<ExecutionResult<String>> {
        let schema = schemars::schema_for!(S);
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

impl<T: HttpTransport> OpenAI<T> {
    async fn execute_internal(
        &self,
        prompt: String,
        response_format: Option<ResponseFormatRequest>,
    ) -> Result<ExecutionResult<String>> {
        let request_body = OpenAIChatCompletionRequest {
            model: self.config.model.clone(),
            messages: vec![ChatMessage::user(prompt)],
            response_format,
            stream: true,
            reasoning_effort: self.config.reasoning_effort.clone(),
            max_tokens: self.config.max_tokens,
        };
        let body = serde_json::to_value(&request_body)
            .map_err(|e| ExecutionError::Serialization(e.to_string()))?;
        let request = HttpRequest::new(
            self.build_url(),
            Some(self.config.api_key.clone()),
            body,
        );

        let (thinking_tx, thinking_rx) = mpsc::unbounded_channel::<Result<Token>>();
        let (content_tx, content_rx) = mpsc::unbounded_channel::<Result<Token>>();
        let thinking_stream: TokenStream = Box::pin(UnboundedReceiverStream::new(thinking_rx));
        let content_stream: TokenStream = Box::pin(UnboundedReceiverStream::new(content_rx));

        let output = spawn_output(run_chat_completion(
            self.transport.clone(),
            request,
            self.config.model.clone(),
            CompletionOptions {
                emit_reasoning: true,
                fallback_to_thinking: false,
            },
            thinking_tx,
            content_tx,
        ));

        Ok(ExecutionResult {
            thinking_stream,
            content_stream,
            output,
        })
    }
}
