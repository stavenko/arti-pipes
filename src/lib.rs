//! arti-pipes: Executor-agnostic prompt pipelines
//!
//! A system for chaining prompts with dynamic dispatch and node-based execution flow,
//! decoupled from specific execution strategies.
//!
//! ## Overview
//!
//! `arti-pipes` provides a flexible framework for building multi-step LLM workflows
//! (pipelines) without coupling to specific execution implementations. The library
//! separates the pipeline orchestration logic from the actual prompt execution,
//! allowing for different execution strategies such as:
//!
//! - LLM API calls (OpenAI, Anthropic, etc.)
//! - Mock executors for testing
//! - Caching layers
//! - Request routing
//! - Fallback strategies
//!
//! ## Architecture
//!
//! The library consists of three main layers:
//!
//! ### 1. Pipeline Layer ([`pipeline`])
//!
//! Orchestrates the execution of node chains. A [`Pipeline`](pipeline::Pipeline)
//! starts with a first node and follows the chain by calling `next_node` on each
//! node until `None` is returned.
//!
//! ### 2. Node Layer ([`node`])
//!
//! Represents individual steps in a pipeline. Each [`Node`](node::Node):
//! - Creates a [`Prompt`](prompt::Prompt) from context
//! - Provides a [`PromptExecutor`](executor::PromptExecutor) instance
//! - Executes and streams events
//! - Selects the next node (enables branching)
//!
//! ### 3. Executor Layer ([`executor`])
//!
//! Defines the [`PromptExecutor`](executor::PromptExecutor) trait that abstracts
//! prompt execution. Implementations handle the actual LLM interaction.
//!
//! ## Usage Example
//!
//! ```rust,ignore
//! use arti_pipes::{
//!     node::{Node, NodeEvent, NodeRunner, NodeWrapper},
//!     pipeline::{Pipeline, run_pipeline},
//!     prompt::{Prompt, PromptExecutionEvent},
//!     executor::PromptExecutor,
//! };
//!
//! // 1. Define your context type
//! #[derive(Clone)]
//! struct MyContext {
//!     input: String,
//!     output: Option<String>,
//! }
//!
//! // 2. Implement your executor
//! #[derive(Clone)]
//! struct MyExecutor;
//!
//! impl PromptExecutor for MyExecutor {
//!     async fn execute_raw(&self, prompt: String) -> Result<ExecutionResult<String>> {
//!         // Your execution logic here
//!     }
//!
//!     async fn execute<T: JsonSchema>(&self, prompt: String) -> Result<ExecutionResult<String>> {
//!         // Your structured execution logic here
//!     }
//! }
//!
//! // 3. Implement your prompt
//! struct MyPrompt {
//!     question: String,
//! }
//!
//! impl Prompt for MyPrompt {
//!     type Output = String;
//!     type Context = MyContext;
//!
//!     fn name(&self) -> String {
//!         "MyPrompt".to_string()
//!     }
//!
//!     fn serialize(&self) -> String {
//!         self.question.clone()
//!     }
//!
//!     fn update_context(&self, mut ctx: Self::Context, data: Self::Output) -> Self::Context {
//!         ctx.output = Some(data);
//!         ctx
//!     }
//!
//!     fn execute<E: PromptExecutor>(&self, executor: E) -> LocalBoxStream<'static, PromptExecutionEvent> {
//!         // Execute using the provided executor
//!     }
//! }
//!
//! // 4. Implement your node
//! struct MyNode;
//!
//! impl Node for MyNode {
//!     type Prompt = MyPrompt;
//!     type Executor = MyExecutor;
//!     type Error = String;
//!     type Context = MyContext;
//!
//!     fn prompt(&self, ctx: &Self::Context) -> Self::Prompt {
//!         MyPrompt { question: ctx.input.clone() }
//!     }
//!
//!     fn prompt_executor(&self) -> Self::Executor {
//!         MyExecutor
//!     }
//!
//!     fn run(&self, context: Self::Context) -> LocalBoxStream<'static, NodeEvent<Self::Context>> {
//!         // Node execution logic
//!     }
//!
//!     fn select_next_node(&self, _ctx: &Self::Context) -> Option<Box<dyn NodeRunner<Self::Context>>> {
//!         None // End of pipeline
//!     }
//! }
//!
//! // 5. Build and run the pipeline
//! #[tokio::main]
//! async fn main() {
//!     let context = MyContext {
//!         input: "What is 2+2?".to_string(),
//!         output: None,
//!     };
//!
//!     let pipeline = Pipeline::new(Box::new(NodeWrapper::new(MyNode)));
//!     let mut stream = run_pipeline(pipeline, context);
//!
//!     while let Some(event) = stream.next().await {
//!         match event {
//!             NodeEvent::Prompt(id, prompt_event) => {
//!                 // Handle prompt events
//!             }
//!             NodeEvent::Completed(ctx) => {
//!                 println!("Result: {:?}", ctx.output);
//!             }
//!         }
//!     }
//! }
//! ```
//!
//! ## Key Features
//!
//! - **Executor agnostic**: Pipeline logic is decoupled from execution strategy
//! - **Type-safe context flow**: Context types are checked at compile time
//! - **Dynamic branching**: Nodes can select the next node based on context
//! - **Streaming events**: Real-time token streaming for UI updates
//! - **Testable**: Easy to test with mock executors
//!
//! ## Module Overview
//!
//! - [`error`]: Error types for pipeline execution
//! - [`executor`]: Executor trait and execution types
//! - [`prompt`]: Prompt trait and execution events
//! - [`node`]: Node trait and node runner abstractions
//! - [`pipeline`]: Pipeline orchestration

pub mod error;
pub mod executor;
pub mod llm_executors;
pub mod node;
pub mod pipeline;
pub mod prompt;
