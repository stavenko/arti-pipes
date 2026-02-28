//! Integration test for tool system with calculator tool

use arti_pipes::{
    error::ExecutionError,
    executor::PromptExecutor,
    node::{Node, NodeEvent, NodeRunner, NodeWrapper},
    pipeline::{run_pipeline, Pipeline},
    prompt::{Prompt, PromptExecutionEvent},
    tool::{Tool, ToolCall},
    tool_registry::ToolRegistry,
};
use async_trait::async_trait;
use futures::{stream::LocalBoxStream, StreamExt};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

// ===== Context =====

#[derive(Clone, Debug)]
struct MathContext {
    expression: String,
    tool_calls: Option<Vec<ToolCall>>,
    result: Option<f64>,
}

// ===== Mock Executor =====

#[derive(Clone)]
struct MockExecutor;

impl PromptExecutor for MockExecutor {
    async fn execute_raw(
        &self,
        _prompt: String,
    ) -> Result<arti_pipes::executor::ExecutionResult<String>, ExecutionError> {
        Err(ExecutionError::ModelExecution(
            "Not implemented for this test".to_string(),
        ))
    }

    fn execute<T: schemars::JsonSchema>(
        &self,
        _prompt: String,
    ) -> impl std::future::Future<Output = Result<arti_pipes::executor::ExecutionResult<String>, ExecutionError>>
        + Send {
        async {
            Err(ExecutionError::ModelExecution(
                "Not implemented for this test".to_string(),
            ))
        }
    }
}

// ===== Calculator Tool =====

#[derive(Deserialize, JsonSchema)]
struct CalculatorInput {
    expression: String,
}

#[derive(Serialize, JsonSchema)]
struct CalculatorOutput {
    result: f64,
}

struct CalculatorTool;

#[async_trait]
impl Tool for CalculatorTool {
    type Input = CalculatorInput;
    type Output = CalculatorOutput;

    fn name(&self) -> &str {
        "calculator"
    }

    fn description(&self) -> &str {
        "Evaluates mathematical expressions like '123 * 456'"
    }

    async fn call(&self, input: Self::Input) -> Result<Self::Output, ExecutionError> {
        // Simple expression evaluator for basic arithmetic
        let result = eval_expression(&input.expression)?;
        Ok(CalculatorOutput { result })
    }
}

// Simple expression evaluator (supports only multiplication for this test)
fn eval_expression(expr: &str) -> Result<f64, ExecutionError> {
    let parts: Vec<&str> = expr.split('*').map(|s| s.trim()).collect();
    if parts.len() != 2 {
        return Err(ExecutionError::ToolExecutionFailed(
            "Only multiplication is supported in this test".to_string(),
        ));
    }

    let a: f64 = parts[0]
        .parse()
        .map_err(|e| ExecutionError::ToolExecutionFailed(format!("Failed to parse: {}", e)))?;
    let b: f64 = parts[1]
        .parse()
        .map_err(|e| ExecutionError::ToolExecutionFailed(format!("Failed to parse: {}", e)))?;

    Ok(a * b)
}

// ===== Math Prompt Node =====

struct MathPrompt {
    expression: String,
}

impl Prompt for MathPrompt {
    type Output = String;
    type Context = MathContext;

    fn name(&self) -> String {
        "MathPrompt".to_string()
    }

    fn serialize(&self) -> String {
        format!("Calculate: {}", self.expression)
    }

    fn update_context(&self, ctx: Self::Context, _data: Self::Output) -> Self::Context {
        ctx
    }

    fn execute<E: PromptExecutor>(
        &self,
        _executor: E,
    ) -> LocalBoxStream<'static, PromptExecutionEvent> {
        let expression = self.expression.clone();

        Box::pin(async_stream::stream! {
            yield PromptExecutionEvent::Scheduled("MathPrompt".to_string());

            // Simulate LLM requesting a tool call
            let tool_call = ToolCall {
                id: "call_1".to_string(),
                name: "calculator".to_string(),
                arguments: serde_json::json!({
                    "expression": expression
                }),
            };

            yield PromptExecutionEvent::ToolCallsRequested(vec![tool_call]);
        })
    }
}

struct MathNode {
    executor: MockExecutor,
    registry: ToolRegistry,
}

impl Node for MathNode {
    type Prompt = MathPrompt;
    type Executor = MockExecutor;
    type Error = String;
    type Context = MathContext;

    fn prompt(&self, ctx: &Self::Context) -> Self::Prompt {
        MathPrompt {
            expression: ctx.expression.clone(),
        }
    }

    fn prompt_executor(&self) -> Self::Executor {
        self.executor.clone()
    }

    fn run(&self, context: Self::Context) -> LocalBoxStream<'static, NodeEvent<Self::Context>> {
        let prompt = self.prompt(&context);
        let executor = self.prompt_executor();
        let mut prompt_stream = prompt.execute(executor);

        Box::pin(async_stream::stream! {
            let mut updated_context = context;
            let execution_id = uuid::Uuid::new_v4();

            while let Some(event) = prompt_stream.next().await {
                match &event {
                    PromptExecutionEvent::ToolCallsRequested(calls) => {
                        updated_context.tool_calls = Some(calls.clone());
                    }
                    _ => {}
                }
                yield NodeEvent::Prompt(execution_id, event);
            }

            yield NodeEvent::Completed(updated_context);
        })
    }

    fn select_next_node(
        &self,
        context: &Self::Context,
    ) -> Option<Box<dyn NodeRunner<Self::Context>>> {
        if context.tool_calls.is_some() && context.result.is_none() {
            Some(Box::new(NodeWrapper::new(ToolExecutorNode {
                registry: self.registry.clone(),
            })))
        } else {
            None
        }
    }
}

impl Clone for MathNode {
    fn clone(&self) -> Self {
        Self {
            executor: self.executor.clone(),
            registry: self.registry.clone(),
        }
    }
}

// ===== Tool Executor Node =====

struct ToolExecutorPrompt;

impl Prompt for ToolExecutorPrompt {
    type Output = String;
    type Context = MathContext;

    fn name(&self) -> String {
        "ToolExecutorPrompt".to_string()
    }

    fn serialize(&self) -> String {
        "Executing tools...".to_string()
    }

    fn update_context(&self, ctx: Self::Context, _data: Self::Output) -> Self::Context {
        ctx
    }

    fn execute<E: PromptExecutor>(
        &self,
        _executor: E,
    ) -> LocalBoxStream<'static, PromptExecutionEvent> {
        Box::pin(async_stream::stream! {
            yield PromptExecutionEvent::Completed("Tools executed".to_string());
        })
    }
}

struct ToolExecutorNode {
    registry: ToolRegistry,
}

impl Node for ToolExecutorNode {
    type Prompt = ToolExecutorPrompt;
    type Executor = MockExecutor;
    type Error = String;
    type Context = MathContext;

    fn prompt(&self, _ctx: &Self::Context) -> Self::Prompt {
        ToolExecutorPrompt
    }

    fn prompt_executor(&self) -> Self::Executor {
        MockExecutor
    }

    fn run(&self, context: Self::Context) -> LocalBoxStream<'static, NodeEvent<Self::Context>> {
        let registry = self.registry.clone();
        let tool_calls = context.tool_calls.clone();

        Box::pin(async_stream::stream! {
            let execution_id = uuid::Uuid::new_v4();
            let mut updated_context = context;

            if let Some(calls) = tool_calls {
                for call in calls {
                    yield NodeEvent::Prompt(
                        execution_id,
                        PromptExecutionEvent::ToolExecuting {
                            call_id: call.id.clone(),
                            tool_name: call.name.clone(),
                        }
                    );

                    match registry.execute(&call).await {
                        Ok(result) => {
                            yield NodeEvent::Prompt(
                                execution_id,
                                PromptExecutionEvent::ToolCompleted {
                                    call_id: result.call_id.clone(),
                                    result: result.clone(),
                                }
                            );

                            // Extract result and update context
                            if let Some(value) = result.output.get("result") {
                                if let Some(num) = value.as_f64() {
                                    updated_context.result = Some(num);
                                }
                            }
                        }
                        Err(e) => {
                            yield NodeEvent::Prompt(
                                execution_id,
                                PromptExecutionEvent::Error(e)
                            );
                        }
                    }
                }
            }

            yield NodeEvent::Completed(updated_context);
        })
    }

    fn select_next_node(
        &self,
        _context: &Self::Context,
    ) -> Option<Box<dyn NodeRunner<Self::Context>>> {
        None
    }
}

impl Clone for ToolExecutorNode {
    fn clone(&self) -> Self {
        Self {
            registry: self.registry.clone(),
        }
    }
}

// ===== Test =====

#[tokio::test]
async fn test_tools_pipeline() {
    // Create tool registry with calculator
    let registry = ToolRegistry::new().register(CalculatorTool);

    // Verify tool descriptor is available
    let descriptors = registry.descriptors();
    assert_eq!(descriptors.len(), 1);
    assert_eq!(descriptors[0].name, "calculator");

    // Create initial context
    let context = MathContext {
        expression: "123 * 456".to_string(),
        tool_calls: None,
        result: None,
    };

    // Create pipeline
    let first_node = MathNode {
        executor: MockExecutor,
        registry: registry.clone(),
    };
    let pipeline = Pipeline::new(Box::new(NodeWrapper::new(first_node)));

    // Run pipeline
    let local_set = tokio::task::LocalSet::new();
    let final_ctx = local_set
        .run_until(async {
            let mut stream = run_pipeline(pipeline, context);
            let mut final_context: Option<MathContext> = None;

            println!("\n=== Tools Pipeline Execution ===\n");

            while let Some(event) = stream.next().await {
                match event {
                    NodeEvent::Prompt(_id, prompt_event) => match prompt_event {
                        PromptExecutionEvent::Scheduled(name) => {
                            println!("📋 Prompt scheduled: {}", name);
                        }
                        PromptExecutionEvent::ToolCallsRequested(calls) => {
                            println!("🔧 Tool calls requested: {} tools", calls.len());
                            for call in &calls {
                                println!("   - {} (id: {})", call.name, call.id);
                            }
                        }
                        PromptExecutionEvent::ToolExecuting {
                            call_id,
                            tool_name,
                        } => {
                            println!("⚙️  Executing tool: {} ({})", tool_name, call_id);
                        }
                        PromptExecutionEvent::ToolCompleted { call_id, result } => {
                            println!("✅ Tool completed: {}", call_id);
                            println!("   Result: {:?}", result.output);
                        }
                        PromptExecutionEvent::Error(e) => {
                            eprintln!("❌ Error: {:?}", e);
                        }
                        _ => {}
                    },
                    NodeEvent::Completed(ctx) => {
                        println!("🎯 Node completed");
                        final_context = Some(ctx);
                    }
                }
            }

            final_context.expect("Pipeline should complete")
        })
        .await;

    // Verify calculator was called and result is correct
    assert!(final_ctx.result.is_some(), "Result should be computed");
    assert_eq!(
        final_ctx.result.unwrap(),
        56088.0,
        "123 * 456 should equal 56088"
    );

    println!("\n=== Final Context ===");
    println!("📊 Expression: {}", final_ctx.expression);
    println!("🔢 Result: {}", final_ctx.result.unwrap());
}
