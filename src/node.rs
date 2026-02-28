//! Node trait, NodeRunner, NodeWrapper, NodeEvent
//!
//! Defines the core node abstractions for pipeline execution.

use crate::executor::PromptExecutor;
use crate::prompt::{Prompt, PromptExecutionEvent};
use futures::stream::LocalBoxStream;
use uuid::Uuid;

/// Events emitted during node execution
#[derive(Debug, Clone)]
pub enum NodeEvent<Context> {
    /// A prompt event occurred (with prompt execution ID)
    Prompt(Uuid, PromptExecutionEvent),
    /// Node execution completed with updated context
    Completed(Context),
}

/// Trait for node implementations
///
/// A Node represents a step in a pipeline that creates and executes a prompt,
/// then determines the next node based on context.
pub trait Node: Send + Sync {
    /// The prompt type this node creates
    type Prompt: Prompt;
    /// The executor type this node uses
    type Executor: PromptExecutor;
    /// Error type for this node
    type Error;
    /// Context type passed through the pipeline
    type Context: Clone + Send + 'static;

    /// Create a prompt from the current context
    fn prompt(&self, ctx: &Self::Context) -> Self::Prompt;

    /// Provide the executor instance for this node
    fn prompt_executor(&self) -> Self::Executor;

    /// Run the node and return a stream of events
    fn run(&self, context: Self::Context) -> LocalBoxStream<'static, NodeEvent<Self::Context>>;

    /// Select the next node based on context, or None if pipeline should end
    fn select_next_node(
        &self,
        context: &Self::Context,
    ) -> Option<Box<dyn NodeRunner<Self::Context>>>;
}

/// Trait for type-erased node execution
///
/// Allows nodes to be stored and executed dynamically without knowing
/// the concrete node type.
pub trait NodeRunner<Context>: Send + Sync {
    /// Run the node and return a stream of events
    fn run(&self, context: Context) -> LocalBoxStream<'static, NodeEvent<Context>>;

    /// Get the next node to execute, or None if pipeline should end
    fn next_node(&self, context: &Context) -> Option<Box<dyn NodeRunner<Context>>>;
}

/// Wrapper that implements NodeRunner for any Node
///
/// Provides the bridge between concrete Node implementations and
/// the type-erased NodeRunner trait.
pub struct NodeWrapper<N>(pub N);

impl<N> NodeWrapper<N> {
    pub fn new(node: N) -> Self {
        Self(node)
    }
}

impl<N> NodeRunner<N::Context> for NodeWrapper<N>
where
    N: Node + Send + Sync + 'static,
    N::Context: Clone + Send + 'static,
{
    fn run(&self, context: N::Context) -> LocalBoxStream<'static, NodeEvent<N::Context>> {
        self.0.run(context)
    }

    fn next_node(&self, context: &N::Context) -> Option<Box<dyn NodeRunner<N::Context>>> {
        self.0.select_next_node(context)
    }
}
