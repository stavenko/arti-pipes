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

// LlmType trait removed - executors now implement PromptExecutor directly
