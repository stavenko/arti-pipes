//! Common types for LLM providers

use crate::error::Result;
use futures::Stream;
use serde::{Deserialize, Serialize};
use std::pin::Pin;

/// A single token from the model's streaming response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Token {
    pub content: String,
    pub index: usize,
}

/// Metadata about the output generation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OutputMetadata {
    pub total_tokens: usize,
    pub generation_time_ms: u64,
    pub model_id: String,
}

/// Output wrapper that contains the parsed result and metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Output<T> {
    pub result: T,
    pub metadata: OutputMetadata,
}

impl<T> Output<T> {
    pub fn new(result: T, metadata: OutputMetadata) -> Self {
        Self { result, metadata }
    }
}

/// Type alias for the token stream
pub type TokenStream = Pin<Box<dyn Stream<Item = Result<Token>> + Send>>;

/// Result of agent execution containing both streams and final output future
pub struct ExecutionResult<T> {
    /// Stream of thinking/reasoning tokens
    pub thinking_stream: TokenStream,
    /// Stream of content/response tokens
    pub content_stream: TokenStream,
    /// Final output after all tokens are processed
    pub output: tokio::task::JoinHandle<Result<Output<T>>>,
}

// === SSE Parsing Types ===

/// Delta content in streaming response
#[derive(Debug, Deserialize, Default)]
pub struct StreamDelta {
    #[serde(default)]
    pub content: Option<String>,
    /// Reasoning content (used by Ollama's OpenAI-compatible API for Qwen3, etc.)
    #[serde(default)]
    pub reasoning: Option<String>,
}

/// Choice in streaming response
#[derive(Debug, Deserialize)]
pub struct StreamChoice {
    pub delta: StreamDelta,
    pub finish_reason: Option<String>,
}

/// Streaming chunk from SSE response
#[derive(Debug, Deserialize)]
pub struct StreamChunk {
    pub choices: Vec<StreamChoice>,
}

/// Parse SSE line into JSON value
pub fn parse_sse_line(line: &str) -> Option<StreamChunk> {
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

// === Chat Message Types ===

/// Chat message for API requests
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChatMessage {
    pub role: String,
    pub content: Option<String>,
}

impl ChatMessage {
    pub fn user(content: impl Into<String>) -> Self {
        Self {
            role: "user".to_string(),
            content: Some(content.into()),
        }
    }
}

/// JSON schema for structured output
#[derive(Debug, Clone, Serialize)]
pub struct JsonSchemaRequest {
    pub name: String,
    pub schema: serde_json::Value,
    pub strict: bool,
}

/// Response format for structured output
#[derive(Debug, Clone, Serialize)]
pub struct ResponseFormatRequest {
    pub r#type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub json_schema: Option<JsonSchemaRequest>,
}

// === LlmType Trait ===

/// Trait for LLM provider implementations
pub trait LlmType: Send + Sync {
    /// Execute a prompt and return raw string response with streaming
    fn execute_raw(
        &self,
        prompt: String,
    ) -> impl std::future::Future<Output = Result<ExecutionResult<String>>> + Send;

    /// Execute a prompt with JSON schema for structured output
    ///
    /// Generates JSON schema from type T and sends it to the model.
    /// Returns raw string response (model returns JSON but we don't parse it here).
    fn execute<T: schemars::JsonSchema>(
        &self,
        prompt: String,
    ) -> impl std::future::Future<Output = Result<ExecutionResult<String>>> + Send;
}
