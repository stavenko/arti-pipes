//! Prompt trait and execution events
//!
//! Defines the core Prompt trait and PromptExecutionEvent enum for pipeline execution.

use crate::error::ExecutionError;
use crate::executor::{PromptExecutor, Token};
use futures::stream::LocalBoxStream;

/// Name identifier for a prompt
pub type PromptName = String;

/// Events emitted during prompt execution
#[derive(Debug, Clone)]
pub enum PromptExecutionEvent {
    /// Prompt execution has been scheduled
    Scheduled(PromptName),
    /// A thinking/reasoning token was received
    ThinkingToken(Token),
    /// A content token was received
    ContentToken(Token),
    /// An error occurred during execution
    Error(ExecutionError),
    /// Execution completed with final output
    Completed(String),
}

/// Trait for prompt implementations
///
/// A Prompt represents a single LLM interaction that can be executed
/// within a pipeline node using a generic executor.
pub trait Prompt {
    /// The parsed output type from this prompt
    type Output;
    /// The context type this prompt operates on
    type Context;

    /// Returns the name of this prompt for identification
    fn name(&self) -> String;

    /// Update context with the prompt's output
    fn update_context(&self, context: Self::Context, data: Self::Output) -> Self::Context;

    /// Serialize the prompt to a string for execution
    fn serialize(&self) -> String;

    /// Execute the prompt using the provided executor and return a stream of events
    fn execute<E: PromptExecutor>(
        &self,
        executor: E,
    ) -> LocalBoxStream<'static, PromptExecutionEvent>;
}
