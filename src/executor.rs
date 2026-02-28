//! PromptExecutor trait and execution types

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

/// Result of prompt execution containing both streams and final output future
pub struct ExecutionResult<T> {
    /// Stream of thinking/reasoning tokens
    pub thinking_stream: TokenStream,
    /// Stream of content/response tokens
    pub content_stream: TokenStream,
    /// Final output after all tokens are processed
    pub output: tokio::task::JoinHandle<Result<Output<T>>>,
}

/// Trait for prompt execution strategies
///
/// This trait abstracts the execution of prompts, allowing different implementations
/// such as LLM API calls, mocking, caching, routing, etc.
pub trait PromptExecutor: Clone + Send + Sync + 'static {
    /// Execute a prompt and return raw string response with streaming
    fn execute_raw(
        &self,
        prompt: String,
    ) -> impl std::future::Future<Output = Result<ExecutionResult<String>>> + Send;

    /// Execute a prompt with JSON schema for structured output
    ///
    /// Generates JSON schema from type T and sends it to the executor.
    /// Returns raw string response (executor returns JSON but we don't parse it here).
    fn execute<T: schemars::JsonSchema>(
        &self,
        prompt: String,
    ) -> impl std::future::Future<Output = Result<ExecutionResult<String>>> + Send;
}
