//! Common types for LLM providers

use crate::error::Result;
use serde::{Deserialize, Serialize};

// Re-export types from executor module to avoid duplication
pub use crate::executor::{ExecutionResult, Output, OutputMetadata, Token, TokenStream};

// === SSE Parsing Types ===

/// Delta content in streaming response
#[derive(Debug, Deserialize, Default)]
pub struct StreamDelta {
    #[serde(default)]
    pub content: Option<String>,
    /// Reasoning content — llama.cpp sends `reasoning_content`, Ollama sends `reasoning`
    #[serde(default, alias = "reasoning_content")]
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

// LlmType trait removed - executors now implement PromptExecutor directly

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_sse_with_reasoning_field() {
        let line = r#"data: {"choices":[{"finish_reason":null,"index":0,"delta":{"reasoning":"thinking step"}}],"created":0,"id":"x","model":"m","object":"chat.completion.chunk"}"#;
        let chunk = parse_sse_line(line).unwrap();
        assert_eq!(chunk.choices[0].delta.reasoning.as_deref(), Some("thinking step"));
        assert_eq!(chunk.choices[0].delta.content, None);
    }

    #[test]
    fn parse_sse_with_reasoning_content_field() {
        let line = r#"data: {"choices":[{"finish_reason":null,"index":0,"delta":{"reasoning_content":"thinking step"}}],"created":0,"id":"x","model":"m","object":"chat.completion.chunk"}"#;
        let chunk = parse_sse_line(line).unwrap();
        assert_eq!(chunk.choices[0].delta.reasoning.as_deref(), Some("thinking step"));
        assert_eq!(chunk.choices[0].delta.content, None);
    }

    #[test]
    fn parse_sse_with_content_field() {
        let line = r#"data: {"choices":[{"finish_reason":null,"index":0,"delta":{"content":"hello"}}],"created":0,"id":"x","model":"m","object":"chat.completion.chunk"}"#;
        let chunk = parse_sse_line(line).unwrap();
        assert_eq!(chunk.choices[0].delta.content.as_deref(), Some("hello"));
        assert_eq!(chunk.choices[0].delta.reasoning, None);
    }

    #[test]
    fn parse_sse_done() {
        assert!(parse_sse_line("data: [DONE]").is_none());
    }
}
