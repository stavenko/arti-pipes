//! Pipeline struct and run functions
//!
//! Provides the Pipeline struct and functions for executing node chains
//! without coupling to specific execution strategies.

use crate::node::{NodeEvent, NodeRunner};
use futures::stream::LocalBoxStream;
use futures::{SinkExt, StreamExt};

/// A pipeline that executes a chain of nodes
///
/// The pipeline starts with a first node and follows the chain
/// by calling `next_node` on each node until `None` is returned.
pub struct Pipeline<Context> {
    pub first_node: Box<dyn NodeRunner<Context>>,
}

impl<Context> Pipeline<Context> {
    /// Create a new pipeline with the given first node
    pub fn new(first_node: Box<dyn NodeRunner<Context>>) -> Self {
        Self { first_node }
    }
}

/// Run a pipeline and send events to a sink
///
/// Executes each node in sequence, sending all events to the provided sink.
/// Continues until a node returns `None` from `next_node`.
pub async fn run_pipeline_with_stream<Context>(
    pipeline: Pipeline<Context>,
    context: Context,
    mut tx: impl futures::Sink<NodeEvent<Context>> + Unpin,
) where
    Context: Clone + Send + 'static,
{
    let mut current_node: Option<Box<dyn NodeRunner<Context>>> = Some(pipeline.first_node);
    let mut current_context = context;

    while let Some(node) = current_node.take() {
        let mut stream = node.run(current_context.clone());

        while let Some(event) = stream.next().await {
            if let NodeEvent::Completed(ctx) = &event {
                current_context = ctx.clone();
            }
            let _ = tx.send(event).await;
        }

        current_node = node.next_node(&current_context);
    }
}

/// Run a pipeline and return a stream of events
///
/// Executes each node in sequence, yielding all events as a stream.
/// Continues until a node returns `None` from `next_node`.
pub fn run_pipeline<Context>(
    pipeline: Pipeline<Context>,
    context: Context,
) -> LocalBoxStream<'static, NodeEvent<Context>>
where
    Context: Clone + Send + 'static,
{
    let (tx, rx) = futures::channel::mpsc::unbounded();

    tokio::task::spawn_local(async move {
        run_pipeline_with_stream(pipeline, context, tx).await;
    });

    Box::pin(rx)
}
