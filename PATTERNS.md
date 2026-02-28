# Pipeline Patterns Guide

Common patterns and best practices for building pipelines with arti-pipes.

## Table of Contents

- [Pipeline Structures](#pipeline-structures)
- [Context Design](#context-design)
- [Node Patterns](#node-patterns)
- [Prompt Patterns](#prompt-patterns)
- [Error Handling](#error-handling)
- [Testing Strategies](#testing-strategies)
- [Performance](#performance)

## Pipeline Structures

### Linear Pipeline (A → B → C)

**Use when:** Each step builds on the previous one sequentially.

```rust
// Node A: Summarize
impl Node for SummaryNode {
    fn select_next_node(&self, _ctx: &Context) -> Option<Box<dyn NodeRunner<Context>>> {
        Some(Box::new(NodeWrapper::new(ExpandNode { ... })))
    }
}

// Node B: Expand
impl Node for ExpandNode {
    fn select_next_node(&self, _ctx: &Context) -> Option<Box<dyn NodeRunner<Context>>> {
        Some(Box::new(NodeWrapper::new(ConcludeNode { ... })))
    }
}

// Node C: Conclude
impl Node for ConcludeNode {
    fn select_next_node(&self, _ctx: &Context) -> Option<Box<dyn NodeRunner<Context>>> {
        None // End
    }
}
```

**Example use cases:**
- Document processing: Extract → Summarize → Format
- Analysis: Gather → Analyze → Report
- Content creation: Outline → Draft → Polish

### Branching Pipeline

**Use when:** Flow depends on results or conditions.

```rust
impl Node for RouterNode {
    fn select_next_node(&self, ctx: &Context) -> Option<Box<dyn NodeRunner<Context>>> {
        match &ctx.classification {
            Classification::Question => {
                Some(Box::new(NodeWrapper::new(AnswerNode { ... })))
            }
            Classification::Command => {
                Some(Box::new(NodeWrapper::new(CommandNode { ... })))
            }
            Classification::Unclear => {
                Some(Box::new(NodeWrapper::new(ClarifyNode { ... })))
            }
        }
    }
}
```

**Example use cases:**
- Intent classification → specialized handlers
- Quality checks → accept/reject/revise
- Error handling → retry/fallback/abort

### Loop with Exit Condition

**Use when:** Iterative refinement until goal met.

```rust
impl Node for RefineNode {
    fn select_next_node(&self, ctx: &Context) -> Option<Box<dyn NodeRunner<Context>>> {
        if ctx.quality_score >= 0.9 || ctx.iterations >= 5 {
            None // Exit: Good enough or max iterations
        } else {
            Some(Box::new(NodeWrapper::new(RefineNode {
                max_iterations: ctx.iterations + 1,
                ..self.clone()
            })))
        }
    }
}
```

**Example use cases:**
- Code generation with validation
- Content refinement until quality threshold
- Problem-solving with iterative improvement

**⚠️ Warning:** Always include max iteration limit to prevent infinite loops!

### Parallel Branches → Merge

**Use when:** Multiple independent analyses needed.

```rust
impl Node for SplitNode {
    fn select_next_node(&self, ctx: &Context) -> Option<Box<dyn NodeRunner<Context>>> {
        // Clone context for parallel processing
        let ctx1 = ctx.clone();
        let ctx2 = ctx.clone();

        // Spawn parallel tasks (outside pipeline)
        tokio::spawn(async move { /* Process ctx1 */ });
        tokio::spawn(async move { /* Process ctx2 */ });

        // Route to merge node that waits for results
        Some(Box::new(NodeWrapper::new(MergeNode { ... })))
    }
}
```

**Note:** arti-pipes pipelines are sequential. For true parallelism, spawn tasks outside the pipeline and coordinate via shared state.

### Hierarchical (Pipeline in Tool)

**Use when:** Tool needs complex multi-step logic.

```rust
struct ComplexTool {
    // Tool can internally run a pipeline
}

#[async_trait]
impl Tool for ComplexTool {
    async fn call(&self, input: Self::Input) -> Result<Self::Output, ExecutionError> {
        // 1. Pre-process
        let data = preprocess(&input)?;

        // 2. Run internal sub-pipeline
        let analysis = self.run_analysis_pipeline(data).await?;

        // 3. Post-process
        let result = postprocess(&analysis)?;

        Ok(result)
    }
}
```

**Example use cases:**
- File save + tag extraction
- Data fetch + transform + store
- Multi-step validation

## Context Design

### Minimal Context

**Use when:** Simple workflows with few intermediate results.

```rust
#[derive(Clone, Debug)]
struct SimpleContext {
    input: String,
    output: Option<String>,
}
```

**Pros:** Easy to understand, minimal overhead
**Cons:** Limited flexibility for complex workflows

### Accumulating Context

**Use when:** Each step adds information.

```rust
#[derive(Clone, Debug)]
struct AccumulatingContext {
    // Original input
    user_query: String,

    // Accumulated results
    classification: Option<Classification>,
    entities: Vec<Entity>,
    summary: Option<String>,
    analysis: Option<Analysis>,
    final_answer: Option<String>,
}
```

**Pros:** Full history available
**Cons:** Can grow large, all nodes see everything

### Staged Context

**Use when:** Different pipeline stages need different data.

```rust
#[derive(Clone, Debug)]
struct StagedContext {
    // Input stage
    input: InputData,

    // Processing stage
    intermediate: Option<IntermediateData>,

    // Output stage
    result: Option<FinalResult>,

    // Metadata (available to all stages)
    metadata: ContextMetadata,
}
```

**Pros:** Clear stages, controlled visibility
**Cons:** More complex type

### Context with State Machine

**Use when:** Workflow has clear states.

```rust
#[derive(Clone, Debug)]
enum PipelineState {
    Initial,
    Analyzing,
    WaitingForClarification,
    Processing,
    Completed,
    Failed(String),
}

#[derive(Clone, Debug)]
struct StatefulContext {
    state: PipelineState,
    data: ContextData,
    error: Option<String>,
}

impl Node for StatefulNode {
    fn select_next_node(&self, ctx: &Context) -> Option<Box<dyn NodeRunner<Context>>> {
        match ctx.state {
            PipelineState::Initial => Some(/* AnalysisNode */),
            PipelineState::Analyzing => Some(/* ProcessingNode */),
            PipelineState::Completed | PipelineState::Failed(_) => None,
            _ => Some(/* ErrorNode */),
        }
    }
}
```

**Pros:** Explicit state management, easier debugging
**Cons:** More boilerplate

### Context with Metadata

**Use when:** Need tracking, debugging, or metrics.

```rust
#[derive(Clone, Debug)]
struct TrackedContext {
    // Business data
    data: BusinessData,

    // Tracking
    metadata: ContextMetadata,
}

#[derive(Clone, Debug)]
struct ContextMetadata {
    pipeline_id: String,
    started_at: DateTime<Utc>,
    nodes_visited: Vec<String>,
    execution_times: HashMap<String, Duration>,
    total_tokens: usize,
}
```

**Pros:** Built-in observability
**Cons:** Extra overhead

## Node Patterns

### Simple Transform Node

```rust
struct TransformNode;

impl Node for TransformNode {
    type Prompt = TransformPrompt;
    type Executor = MyExecutor;
    type Context = MyContext;

    fn prompt(&self, ctx: &Self::Context) -> Self::Prompt {
        TransformPrompt { input: ctx.input.clone() }
    }

    fn prompt_executor(&self) -> Self::Executor {
        MyExecutor::default()
    }

    fn run(&self, context: Self::Context) -> LocalBoxStream<'static, NodeEvent<Self::Context>> {
        let prompt = self.prompt(&context);
        let executor = self.prompt_executor();
        let mut stream = prompt.execute(executor);

        Box::pin(stream! {
            let mut result = String::new();

            while let Some(event) = stream.next().await {
                if let PromptExecutionEvent::Completed(output) = &event {
                    result = output.clone();
                }
                yield NodeEvent::Prompt(uuid::Uuid::new_v4(), event);
            }

            let updated_ctx = prompt.update_context(context, result);
            yield NodeEvent::Completed(updated_ctx);
        })
    }

    fn select_next_node(&self, _ctx: &Self::Context) -> Option<Box<dyn NodeRunner<Self::Context>>> {
        None
    }
}
```

### Conditional Node

```rust
struct ConditionalNode {
    condition: fn(&Context) -> bool,
    true_node: Box<dyn NodeRunner<Context>>,
    false_node: Box<dyn NodeRunner<Context>>,
}

impl Node for ConditionalNode {
    fn select_next_node(&self, ctx: &Context) -> Option<Box<dyn NodeRunner<Context>>> {
        if (self.condition)(ctx) {
            Some(self.true_node.clone())
        } else {
            Some(self.false_node.clone())
        }
    }
}
```

### Retry Node

```rust
struct RetryNode {
    max_retries: usize,
    current_retry: usize,
}

impl Node for RetryNode {
    fn select_next_node(&self, ctx: &Context) -> Option<Box<dyn NodeRunner<Context>>> {
        if ctx.succeeded {
            Some(/* next step */)
        } else if self.current_retry < self.max_retries {
            Some(Box::new(NodeWrapper::new(RetryNode {
                max_retries: self.max_retries,
                current_retry: self.current_retry + 1,
            })))
        } else {
            None // Failed after max retries
        }
    }
}
```

### Node with Tool Integration

```rust
struct ToolIntegratedNode {
    registry: ToolRegistry,
}

impl Node for ToolIntegratedNode {
    fn run(&self, context: Context) -> LocalBoxStream<'static, NodeEvent<Context>> {
        let registry = self.registry.clone();

        Box::pin(stream! {
            let mut ctx = context;
            let mut prompt_stream = create_prompt_stream();

            while let Some(event) = prompt_stream.next().await {
                match event {
                    PromptExecutionEvent::ToolCallsRequested(calls) => {
                        yield NodeEvent::Prompt(id, event);

                        // Execute tools
                        for call in calls {
                            let result = registry.execute(&call).await;
                            // Apply results to context
                        }
                    }
                    other => yield NodeEvent::Prompt(id, other),
                }
            }

            yield NodeEvent::Completed(ctx);
        })
    }
}
```

## Prompt Patterns

### Simple Prompt

```rust
struct SimplePrompt {
    instruction: String,
}

impl Prompt for SimplePrompt {
    fn serialize(&self) -> String {
        self.instruction.clone()
    }
}
```

### Template Prompt

```rust
struct TemplatePrompt {
    template: String,
    variables: HashMap<String, String>,
}

impl Prompt for TemplatePrompt {
    fn serialize(&self) -> String {
        let mut result = self.template.clone();
        for (key, value) in &self.variables {
            result = result.replace(&format!("{{{}}}", key), value);
        }
        result
    }
}
```

### System + User Prompt

```rust
struct ConversationPrompt {
    system: String,
    user: String,
}

impl Prompt for ConversationPrompt {
    fn serialize(&self) -> String {
        format!("System: {}\n\nUser: {}", self.system, self.user)
    }
}
```

### Few-Shot Prompt

```rust
struct FewShotPrompt {
    examples: Vec<(String, String)>,
    query: String,
}

impl Prompt for FewShotPrompt {
    fn serialize(&self) -> String {
        let mut prompt = String::from("Here are some examples:\n\n");

        for (input, output) in &self.examples {
            prompt.push_str(&format!("Input: {}\nOutput: {}\n\n", input, output));
        }

        prompt.push_str(&format!("Now for the actual query:\nInput: {}\nOutput:", self.query));
        prompt
    }
}
```

### Chain-of-Thought Prompt

```rust
struct ChainOfThoughtPrompt {
    problem: String,
}

impl Prompt for ChainOfThoughtPrompt {
    fn serialize(&self) -> String {
        format!(
            "Let's solve this step by step:\n\n\
             Problem: {}\n\n\
             Step 1: Identify what we know\n\
             Step 2: Determine what we need to find\n\
             Step 3: Apply the appropriate method\n\
             Step 4: Verify the answer\n\n\
             Your reasoning:",
            self.problem
        )
    }
}
```

## Error Handling

### Pattern 1: Error Context Field

```rust
#[derive(Clone)]
struct ResilientContext {
    data: MyData,
    error: Option<ErrorInfo>,
    retry_count: usize,
}

#[derive(Clone, Debug)]
struct ErrorInfo {
    message: String,
    occurred_at: String, // node name
    recoverable: bool,
}

impl Node for ResilientNode {
    fn select_next_node(&self, ctx: &Context) -> Option<Box<dyn NodeRunner<Context>>> {
        if let Some(error) = &ctx.error {
            if error.recoverable && ctx.retry_count < 3 {
                Some(/* retry node */)
            } else {
                Some(Box::new(NodeWrapper::new(ErrorHandlerNode { ... })))
            }
        } else {
            Some(/* normal flow */)
        }
    }
}
```

### Pattern 2: Error Event Handling

```rust
while let Some(event) = stream.next().await {
    match event {
        NodeEvent::Prompt(_, PromptExecutionEvent::Error(e)) => {
            match e {
                ExecutionError::ModelExecution(msg) => {
                    eprintln!("LLM error: {}", msg);
                    // Could retry with different parameters
                }
                ExecutionError::ToolNotFound(name) => {
                    eprintln!("Tool '{}' not found", name);
                    // Could fallback to manual processing
                }
                _ => {
                    eprintln!("Other error: {:?}", e);
                }
            }
        }
        _ => { /* normal events */ }
    }
}
```

### Pattern 3: Validation Node

```rust
struct ValidationNode;

impl Node for ValidationNode {
    fn run(&self, context: Context) -> LocalBoxStream<'static, NodeEvent<Context>> {
        Box::pin(stream! {
            let mut ctx = context;

            // Validate context
            if let Err(e) = validate_context(&ctx) {
                ctx.error = Some(ErrorInfo {
                    message: e.to_string(),
                    occurred_at: "ValidationNode".into(),
                    recoverable: false,
                });
            }

            yield NodeEvent::Completed(ctx);
        })
    }

    fn select_next_node(&self, ctx: &Context) -> Option<Box<dyn NodeRunner<Context>>> {
        if ctx.error.is_some() {
            None // Stop on validation error
        } else {
            Some(/* continue */)
        }
    }
}
```

## Testing Strategies

### Unit Testing Prompts

```rust
#[test]
fn test_prompt_serialization() {
    let prompt = MyPrompt {
        variable: "test".to_string(),
    };

    let serialized = prompt.serialize();
    assert!(serialized.contains("test"));
}

#[test]
fn test_prompt_context_update() {
    let prompt = MyPrompt { /* ... */ };
    let ctx = MyContext { /* ... */ };
    let output = "result".to_string();

    let updated = prompt.update_context(ctx, output);
    assert_eq!(updated.result, Some("result".to_string()));
}
```

### Unit Testing Nodes

```rust
#[tokio::test]
async fn test_node_selection() {
    let node = MyNode;
    let ctx = MyContext { should_continue: true };

    let next = node.select_next_node(&ctx);
    assert!(next.is_some());

    let ctx_done = MyContext { should_continue: false };
    let next_done = node.select_next_node(&ctx_done);
    assert!(next_done.is_none());
}
```

### Integration Testing with Mock Executor

```rust
struct MockExecutor {
    canned_response: String,
}

impl PromptExecutor for MockExecutor {
    async fn execute_raw(&self, _prompt: String) -> Result<ExecutionResult<String>> {
        Ok(ExecutionResult {
            thinking_stream: Box::pin(stream! {}),
            content_stream: Box::pin(stream! {
                yield Ok(Token { content: self.canned_response.clone() });
            }),
            output: Box::pin(async move {
                Ok(Output { result: self.canned_response.clone(), metadata: None })
            }),
        })
    }
}

#[tokio::test]
async fn test_pipeline_with_mock() {
    let executor = MockExecutor {
        canned_response: "mocked response".into(),
    };

    let node = MyNode { executor };
    let pipeline = Pipeline::new(Box::new(NodeWrapper::new(node)));
    let context = MyContext { /* ... */ };

    let final_ctx = run_pipeline_to_completion(pipeline, context).await;
    assert!(final_ctx.result.is_some());
}
```

### Testing Tool Integration

```rust
#[tokio::test]
async fn test_tool_execution() {
    let tool = MyTool;
    let input = MyInput { /* ... */ };

    let result = tool.call(input).await.unwrap();
    assert_eq!(result.value, expected_value);
}

#[tokio::test]
async fn test_tool_in_registry() {
    let registry = ToolRegistry::new().register(MyTool);

    let call = ToolCall {
        id: "test".into(),
        name: "my_tool".into(),
        arguments: json!({ /* ... */ }),
    };

    let result = registry.execute(&call).await.unwrap();
    assert!(result.output.get("value").is_some());
}
```

## Performance

### Context Cloning

Context is cloned when passing to next node. Keep it lightweight:

✅ **Good:**
```rust
#[derive(Clone)]
struct LightContext {
    id: String,
    result: Option<String>,
    metadata: Arc<Metadata>, // Shared via Arc
}
```

❌ **Bad:**
```rust
#[derive(Clone)]
struct HeavyContext {
    id: String,
    result: Option<String>,
    large_data: Vec<LargeStruct>, // Cloned every time!
}
```

### Stream Processing

Process tokens as they arrive, don't buffer:

✅ **Good:**
```rust
while let Some(token) = content_stream.next().await {
    print!("{}", token.content); // Process immediately
    stdout().flush()?;
}
```

❌ **Bad:**
```rust
let mut all_tokens = Vec::new();
while let Some(token) = content_stream.next().await {
    all_tokens.push(token); // Buffers everything
}
// Process after all tokens received
```

### Parallel Tool Execution

Use `execute_all()` for independent tools:

```rust
let calls = vec![
    ToolCall { name: "weather", /* ... */ },
    ToolCall { name: "news", /* ... */ },
    ToolCall { name: "stock", /* ... */ },
];

// Executes in parallel
let results = registry.execute_all(calls).await;
```

### Lazy Initialization

Create expensive resources only when needed:

```rust
struct LazyNode {
    executor: OnceCell<ExpensiveExecutor>,
}

impl Node for LazyNode {
    fn prompt_executor(&self) -> Self::Executor {
        self.executor.get_or_init(|| {
            ExpensiveExecutor::new() // Only created once
        }).clone()
    }
}
```

## Best Practices Summary

1. **Context Design**
   - Keep context cloneable and lightweight
   - Use `Arc<T>` for shared immutable data
   - Include metadata for debugging

2. **Node Design**
   - Single responsibility per node
   - Clear naming (SummarizeNode, not ProcessNode)
   - Explicit state transitions

3. **Prompt Design**
   - Clear instructions
   - Include examples when helpful
   - Validate outputs

4. **Error Handling**
   - Always handle `PromptExecutionEvent::Error`
   - Include retry logic for transient errors
   - Fail fast for unrecoverable errors

5. **Testing**
   - Unit test prompts and nodes separately
   - Integration test with mock executors
   - Test error paths

6. **Performance**
   - Stream, don't buffer
   - Clone minimally
   - Parallelize when possible

For more examples, see the `tests/` directory in the repository.
