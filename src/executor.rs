//! PromptExecutor trait and execution types

use crate::error::Result;
use crate::platform::OutputFuture;
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

/// Type alias for the token stream.
///
/// `Send` on native hosts so it can cross tokio threads; not `Send` in the
/// browser, where Fetch-backed streams live on the single JS event loop.
#[cfg(not(target_arch = "wasm32"))]
pub type TokenStream = Pin<Box<dyn Stream<Item = Result<Token>> + Send>>;

/// Type alias for the token stream.
#[cfg(target_arch = "wasm32")]
pub type TokenStream = Pin<Box<dyn Stream<Item = Result<Token>>>>;

/// Result of prompt execution containing both streams and final output future
pub struct ExecutionResult<T> {
    /// Stream of thinking/reasoning tokens
    pub thinking_stream: TokenStream,
    /// Stream of content/response tokens
    pub content_stream: TokenStream,
    /// Final output, resolved once all tokens have been processed.
    ///
    /// Backed by a spawned task that starts immediately (see
    /// [`crate::platform::spawn_output`]), so the streams flow before this is
    /// awaited.
    pub output: OutputFuture<T>,
}

/// Trait for prompt execution strategies (native hosts).
///
/// This trait abstracts the execution of prompts, allowing different implementations
/// such as LLM API calls, mocking, caching, routing, etc.
///
/// The native and wasm variants differ only in `Send` bounds: native futures
/// must be `Send` to run on the tokio runtime, while browser futures are
/// `!Send`.
#[cfg(not(target_arch = "wasm32"))]
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

/// Trait for prompt execution strategies (browser / `wasm32`).
///
/// Identical to the native [`PromptExecutor`] but without `Send` bounds, since
/// Fetch-backed execution futures cannot be `Send`.
#[cfg(target_arch = "wasm32")]
pub trait PromptExecutor: Clone + 'static {
    /// Execute a prompt and return raw string response with streaming
    fn execute_raw(
        &self,
        prompt: String,
    ) -> impl std::future::Future<Output = Result<ExecutionResult<String>>>;

    /// Execute a prompt with JSON schema for structured output
    fn execute<T: schemars::JsonSchema>(
        &self,
        prompt: String,
    ) -> impl std::future::Future<Output = Result<ExecutionResult<String>>>;
}
