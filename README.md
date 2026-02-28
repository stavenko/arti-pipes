# arti-pipes

**Executor-agnostic prompt pipelines for Rust**

Build multi-step LLM workflows without coupling to specific execution implementations. Chain prompts, branch based on results, and integrate tools—all while keeping your pipeline logic separate from your LLM provider.

## Quick Start

```rust
use arti_pipes::*;

// 1. Define your context (flows through the pipeline)
#[derive(Clone)]
struct MyContext {
    input: String,
    result: Option<String>,
}

// 2. Implement a prompt
struct MyPrompt { question: String }

impl Prompt for MyPrompt {
    type Output = String;
    type Context = MyContext;

    fn name(&self) -> String { "MyPrompt".to_string() }
    fn serialize(&self) -> String { self.question.clone() }

    fn update_context(&self, mut ctx: Self::Context, result: Self::Output) -> Self::Context {
        ctx.result = Some(result);
        ctx
    }

    fn execute<E: PromptExecutor>(&self, executor: E) -> LocalBoxStream<'static, PromptExecutionEvent> {
        // Execute and stream events
    }
}

// 3. Implement a node
struct MyNode { executor: MyExecutor }

impl Node for MyNode {
    type Prompt = MyPrompt;
    type Executor = MyExecutor;
    type Context = MyContext;

    fn prompt(&self, ctx: &Self::Context) -> Self::Prompt {
        MyPrompt { question: ctx.input.clone() }
    }

    fn prompt_executor(&self) -> Self::Executor { self.executor.clone() }

    fn run(&self, context: Self::Context) -> LocalBoxStream<'static, NodeEvent<Self::Context>> {
        // Execute prompt, stream events, update context
    }

    fn select_next_node(&self, _ctx: &Self::Context) -> Option<Box<dyn NodeRunner<Self::Context>>> {
        None // End of pipeline
    }
}

// 4. Run the pipeline
let pipeline = Pipeline::new(Box::new(NodeWrapper::new(MyNode { executor })));
let mut stream = run_pipeline(pipeline, context);

while let Some(event) = stream.next().await {
    match event {
        NodeEvent::Prompt(id, prompt_event) => { /* Handle prompt events */ }
        NodeEvent::Completed(ctx) => { /* Pipeline step completed */ }
    }
}
```

## Core Concepts

### Three-Layer Architecture

```
┌─────────────────────────────────────────────┐
│  Pipeline Layer (pipeline.rs)               │
│  • Orchestrates node execution              │
│  • Streams NodeEvent<Context>               │
│  • Manages context flow                     │
└─────────────────────────────────────────────┘
                    ↓
┌─────────────────────────────────────────────┐
│  Node Layer (node.rs)                       │
│  • Creates prompts from context             │
│  • Provides executor instances              │
│  • Selects next node (branching)            │
│  • Streams events, updates context          │
└─────────────────────────────────────────────┘
                    ↓
┌─────────────────────────────────────────────┐
│  Prompt Layer (prompt.rs)                   │
│  • Serializes prompt text                   │
│  • Executes via PromptExecutor              │
│  • Parses/validates output                  │
│  • Updates context with results             │
└─────────────────────────────────────────────┘
                    ↓
┌─────────────────────────────────────────────┐
│  Executor Layer (executor.rs)               │
│  • Abstract execution interface             │
│  • LLM provider implementation              │
│  • Streaming tokens (thinking + content)    │
│  • Returns structured output                │
└─────────────────────────────────────────────┘
```

### Mental Model

1. **Context** is your data that flows through the pipeline
2. **Nodes** are steps in your workflow (summarize, expand, etc.)
3. **Prompts** are LLM interactions within a node
4. **Executors** handle the actual LLM API calls
5. **Events** stream in real-time for UI updates

### Key Design Principles

✅ **Executor Agnostic** - Swap OpenAI for Anthropic without changing pipeline logic
✅ **Type-Safe Context** - Compile-time checks ensure context compatibility
✅ **Streaming First** - Real-time token streaming for responsive UIs
✅ **Dynamic Branching** - Nodes select next steps based on results
✅ **Tool Integration** - LLMs can call external functions

## Installation

```toml
[dependencies]
arti-pipes = "0.1"
tokio = { version = "1", features = ["full"] }
futures = "0.3"
serde = { version = "1", features = ["derive"] }
```

## Examples

### 1. Linear Pipeline (A → B → C)

Three nodes that process sequentially:

```rust
// Node 1: Summarize
impl Node for SummaryNode {
    fn select_next_node(&self, _ctx: &Context) -> Option<Box<dyn NodeRunner<Context>>> {
        Some(Box::new(NodeWrapper::new(ExpandNode { ... })))
    }
}

// Node 2: Expand
impl Node for ExpandNode {
    fn select_next_node(&self, _ctx: &Context) -> Option<Box<dyn NodeRunner<Context>>> {
        Some(Box::new(NodeWrapper::new(ConclusionNode { ... })))
    }
}

// Node 3: Conclude
impl Node for ConclusionNode {
    fn select_next_node(&self, _ctx: &Context) -> Option<Box<dyn NodeRunner<Context>>> {
        None // End
    }
}
```

See: `tests/three_node_pipeline.rs`

### 2. Branching Pipeline

Route based on context:

```rust
impl Node for AnalyzerNode {
    fn select_next_node(&self, ctx: &Context) -> Option<Box<dyn NodeRunner<Context>>> {
        if ctx.needs_clarification {
            Some(Box::new(NodeWrapper::new(ClarificationNode { ... })))
        } else if ctx.is_complete {
            None // Done
        } else {
            Some(Box::new(NodeWrapper::new(ProcessingNode { ... })))
        }
    }
}
```

### 3. Tool Integration

LLM requests tool calls, node executes them:

```rust
// Define a tool
struct CalculatorTool;

#[async_trait]
impl Tool for CalculatorTool {
    type Input = CalculatorInput;
    type Output = CalculatorOutput;

    fn name(&self) -> &str { "calculator" }
    fn description(&self) -> &str { "Evaluates mathematical expressions" }

    async fn call(&self, input: Self::Input) -> Result<Self::Output, ExecutionError> {
        let result = evaluate(&input.expression)?;
        Ok(CalculatorOutput { result })
    }
}

// Register and use
let registry = ToolRegistry::new().register(CalculatorTool);

// In your node
impl Node for MathNode {
    fn run(&self, context: Context) -> LocalBoxStream<'static, NodeEvent<Context>> {
        let registry = self.registry.clone();

        Box::pin(stream! {
            // Execute prompt
            while let Some(event) = prompt_stream.next().await {
                match event {
                    PromptExecutionEvent::ToolCallsRequested(calls) => {
                        // Execute tools immediately
                        for call in calls {
                            let result = registry.execute(&call).await?;
                            // Update context with results
                        }
                    }
                    _ => { /* other events */ }
                }
            }
        })
    }
}
```

See: `tests/tools_pipeline.rs`, `tests/tool_with_subpipeline.rs`

## Tool System Patterns

### Pattern 1: Tool with Internal Logic

Tool encapsulates complex operations (recommended):

```rust
struct SaveFileAndTagTool {
    existing_tags: Vec<String>,
}

impl Tool for SaveFileAndTagTool {
    async fn call(&self, input: SaveFileInput) -> Result<SaveFileOutput> {
        // 1. Save file
        save_file(&input.filename, &input.content)?;

        // 2. Extract tags (internal logic or sub-pipeline)
        let tags = extract_tags(&input.content, &self.existing_tags);

        // 3. Return combined results
        Ok(SaveFileOutput {
            saved_path: input.filename,
            extracted_tags: tags,
        })
    }
}
```

**Advantages:**
- Tool encapsulates its complexity
- Single node handles everything
- Easier to reuse
- Natural LLM → Tool → Result flow

### Pattern 2: Multi-Node Routing

Pipeline routes between specialized nodes:

```rust
SaveFileNode → ToolExecutorNode → TagExtractorNode
```

**Advantages:**
- Clear separation of concerns
- Each node does one thing
- Easier to test individually
- More flexible routing

**Choose based on:** Does the logic belong to the tool or the pipeline orchestration?

## Event Streaming

All operations emit events for real-time UI updates:

```rust
while let Some(event) = stream.next().await {
    match event {
        NodeEvent::Prompt(id, PromptExecutionEvent::Scheduled(name)) => {
            println!("Starting: {}", name);
        }
        NodeEvent::Prompt(id, PromptExecutionEvent::ThinkingToken(token)) => {
            print!("🧠 {}", token.content); // Reasoning
        }
        NodeEvent::Prompt(id, PromptExecutionEvent::ContentToken(token)) => {
            print!("{}", token.content); // Response
        }
        NodeEvent::Prompt(id, PromptExecutionEvent::ToolCallsRequested(calls)) => {
            println!("Tools requested: {:?}", calls);
        }
        NodeEvent::Prompt(id, PromptExecutionEvent::ToolCompleted { result, .. }) => {
            println!("Tool result: {:?}", result.output);
        }
        NodeEvent::Completed(context) => {
            println!("Node completed, context updated");
        }
    }
}
```

## Implementing a Custom Executor

```rust
use arti_pipes::executor::{PromptExecutor, ExecutionResult, Token, Output};

struct MyExecutor {
    api_key: String,
    model: String,
}

impl PromptExecutor for MyExecutor {
    async fn execute_raw(&self, prompt: String) -> Result<ExecutionResult<String>, ExecutionError> {
        // 1. Call your LLM API
        let response = self.call_api(&prompt).await?;

        // 2. Create token streams
        let thinking_stream = /* stream of thinking tokens */;
        let content_stream = /* stream of content tokens */;

        // 3. Create output future
        let output = async move {
            let result = collect_response(content_stream).await?;
            Ok(Output {
                result,
                metadata: /* usage, model, etc */
            })
        };

        Ok(ExecutionResult {
            thinking_stream: Box::pin(thinking_stream),
            content_stream: Box::pin(content_stream),
            output: Box::pin(output),
        })
    }

    fn execute<T: JsonSchema>(&self, prompt: String) -> impl Future<Output = Result<ExecutionResult<String>>> + Send {
        // Similar but with JSON schema for structured output
        async move { self.execute_raw(prompt).await }
    }
}
```

## Built-in Executors

### GptOss (OpenAI-compatible)

```rust
use arti_pipes::llm_executors::GptOss;

let executor = GptOss::builder()
    .api_base("http://localhost:8080/v1")
    .model("gpt-4")
    .reasoning_effort("medium") // for o-series models
    .build();
```

## Testing

Create mock executors for testing:

```rust
struct MockExecutor;

impl PromptExecutor for MockExecutor {
    async fn execute_raw(&self, _prompt: String) -> Result<ExecutionResult<String>> {
        // Return mock data
        Ok(ExecutionResult {
            thinking_stream: Box::pin(stream! { /* mock tokens */ }),
            content_stream: Box::pin(stream! { /* mock tokens */ }),
            output: Box::pin(async { Ok(Output { result: "mocked".into(), metadata: None }) }),
        })
    }
}
```

## Common Patterns

### Context Design

```rust
#[derive(Clone, Debug)]
struct MyContext {
    // Input data
    user_input: String,
    chat_history: Vec<Message>,

    // Accumulated results
    summary: Option<String>,
    analysis: Option<AnalysisResult>,

    // Flow control
    needs_clarification: bool,
    step_count: usize,

    // Tool integration
    tool_calls: Option<Vec<ToolCall>>,
    tool_results: Vec<ToolResult>,
}
```

### Error Handling

```rust
NodeEvent::Prompt(id, PromptExecutionEvent::Error(e)) => {
    match e {
        ExecutionError::ModelExecution(msg) => {
            // LLM API failed
            eprintln!("Model error: {}", msg);
        }
        ExecutionError::ToolNotFound(name) => {
            // Tool doesn't exist
            eprintln!("Unknown tool: {}", name);
        }
        _ => {
            // Other errors
            eprintln!("Error: {:?}", e);
        }
    }
}
```

### Stopping Early

```rust
impl Node for MyNode {
    fn select_next_node(&self, ctx: &Context) -> Option<Box<dyn NodeRunner<Context>>> {
        if ctx.error_occurred || ctx.max_steps_reached {
            None // Stop pipeline
        } else {
            Some(Box::new(NodeWrapper::new(NextNode { ... })))
        }
    }
}
```

## Performance Tips

1. **Use `tokio::task::LocalSet`** for running pipelines with `!Send` futures:
```rust
let local_set = tokio::task::LocalSet::new();
local_set.run_until(async {
    let mut stream = run_pipeline(pipeline, context);
    while let Some(event) = stream.next().await {
        // Handle events
    }
}).await;
```

2. **Clone minimally** - Context is cloned between nodes, keep it lightweight

3. **Stream processing** - Don't buffer all tokens, process them as they arrive

4. **Tool parallelism** - Use `ToolRegistry::execute_all()` for parallel tool execution

## Documentation

- **Architecture**: See module documentation in `src/lib.rs`
- **Tool System**: See `TOOLS.md` (coming soon)
- **Patterns Guide**: See `PATTERNS.md` (coming soon)
- **API Reference**: Run `cargo doc --open`

## Examples

Run the integration tests to see complete examples:

```bash
# Basic 3-node pipeline
cargo test test_three_node_pipeline -- --nocapture

# Tool system basics
cargo test test_tools_pipeline -- --nocapture

# File saver with chat metadata
cargo test test_file_saver_with_chat_context -- --nocapture

# Tag extraction after file save
cargo test test_file_saver_with_tag_extraction -- --nocapture

# Tool with internal sub-logic
cargo test test_tool_with_subpipeline -- --nocapture
```

## Contributing

This library is designed to be executor-agnostic. When contributing:

- Keep pipeline logic separate from execution details
- Maintain the three-layer architecture
- Add tests for new patterns
- Document with LLM consumption in mind

## License

[Add your license here]

## FAQ

**Q: Why not just use async functions?**
A: Nodes provide structure for branching, context management, and event streaming that plain functions don't.

**Q: Can I use multiple executors in one pipeline?**
A: Yes! Each node provides its own executor via `prompt_executor()`.

**Q: How do I handle errors mid-pipeline?**
A: Errors emit `PromptExecutionEvent::Error`. Nodes can check context and route to error-handling nodes.

**Q: Can tools call other tools?**
A: Yes, tools can internally use `ToolRegistry` to orchestrate complex operations.

**Q: Is this production-ready?**
A: The architecture is stable. Add production features (retries, timeouts, metrics) in your executor implementations.
