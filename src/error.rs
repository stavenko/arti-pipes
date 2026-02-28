//! Error types for pipeline execution

use thiserror::Error;

#[derive(Debug, Clone, Error)]
pub enum ExecutionError {
    #[error("Model execution failed: {0}")]
    ModelExecution(String),

    #[error("Output parsing failed: {0}")]
    OutputParsing(String),

    #[error("Stream interrupted: {0}")]
    StreamInterrupted(String),

    #[error("Timeout exceeded: {0}")]
    Timeout(String),

    #[error("Serialization error: {0}")]
    Serialization(String),

    #[error("IO error: {0}")]
    Io(String),

    #[error("Configuration error: {0}")]
    ConfigError(String),

    #[error("Tool not found: {0}")]
    ToolNotFound(String),

    #[error("Failed to parse tool input: {0}")]
    ToolInputParse(String),

    #[error("Failed to serialize tool output: {0}")]
    ToolOutputSerialize(String),

    #[error("Tool execution failed: {0}")]
    ToolExecutionFailed(String),
}

impl From<serde_json::Error> for ExecutionError {
    fn from(e: serde_json::Error) -> Self {
        ExecutionError::Serialization(e.to_string())
    }
}

impl From<std::io::Error> for ExecutionError {
    fn from(e: std::io::Error) -> Self {
        ExecutionError::Io(e.to_string())
    }
}

pub type Result<T> = std::result::Result<T, ExecutionError>;
