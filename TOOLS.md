# Tool System Guide

Complete guide to using the arti-pipes tool system for LLM function calling.

## Table of Contents

- [Overview](#overview)
- [Quick Start](#quick-start)
- [Core Concepts](#core-concepts)
- [Implementing Tools](#implementing-tools)
- [Tool Registry](#tool-registry)
- [Integration Patterns](#integration-patterns)
- [Advanced Topics](#advanced-topics)
- [Best Practices](#best-practices)

## Overview

The tool system enables LLMs to invoke external functions during prompt execution. This allows:

- **Database queries** during conversation
- **API calls** for real-time data
- **Calculations** that LLMs struggle with
- **File operations** (read, write, search)
- **Multi-step reasoning** with tool augmentation

### Architecture

```
┌──────────────┐
│   Node       │  Creates prompt with tools available
└──────┬───────┘
       │ execute()
       ▼
┌──────────────┐
│   Prompt     │  Executes via PromptExecutor
└──────┬───────┘  Emits ToolCallsRequested event
       │
       ▼
┌──────────────┐
│ ToolRegistry │  Type-safe tool lookup
└──────┬───────┘  JSON serialization boundary
       │
       ▼
┌──────────────┐
│     Tool     │  Your implementation
└──────────────┘  Async execution with Input/Output types
```

## Quick Start

### 1. Define a Tool

```rust
use arti_pipes::tool::Tool;
use async_trait::async_trait;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

// Define input/output types
#[derive(Deserialize, JsonSchema)]
struct CalculatorInput {
    expression: String,
}

#[derive(Serialize, JsonSchema)]
struct CalculatorOutput {
    result: f64,
}

// Implement the tool
struct CalculatorTool;

#[async_trait]
impl Tool for CalculatorTool {
    type Input = CalculatorInput;
    type Output = CalculatorOutput;

    fn name(&self) -> &str {
        "calculator"
    }

    fn description(&self) -> &str {
        "Evaluates mathematical expressions like '2 + 2' or '10 * 5'"
    }

    async fn call(&self, input: Self::Input) -> Result<Self::Output, ExecutionError> {
        let result = eval_expression(&input.expression)?;
        Ok(CalculatorOutput { result })
    }
}
```

### 2. Register Tool

```rust
use arti_pipes::tool_registry::ToolRegistry;

let registry = ToolRegistry::new()
    .register(CalculatorTool)
    .register(WeatherTool)
    .register(DatabaseTool);
```

### 3. Use in Node

```rust
impl Node for MathNode {
    fn run(&self, context: Context) -> LocalBoxStream<'static, NodeEvent<Context>> {
        let registry = self.registry.clone();

        Box::pin(stream! {
            // Execute prompt
            let mut prompt_stream = prompt.execute(executor);

            while let Some(event) = prompt_stream.next().await {
                match event {
                    PromptExecutionEvent::ToolCallsRequested(calls) => {
                        // LLM wants to call tools
                        for call in calls {
                            let result = registry.execute(&call).await?;
                            // Update context with result
                        }
                    }
                    _ => { /* other events */ }
                }
                yield NodeEvent::Prompt(id, event);
            }

            yield NodeEvent::Completed(updated_context);
        })
    }
}
```

## Core Concepts

### Tool Trait

The `Tool` trait defines type-safe tool interfaces:

```rust
#[async_trait]
pub trait Tool: Send + Sync {
    /// Input type (must be deserializable from JSON)
    type Input: for<'de> Deserialize<'de> + Send;

    /// Output type (must be serializable to JSON)
    type Output: Serialize + Send;

    /// Unique tool name (used in LLM API calls)
    fn name(&self) -> &str;

    /// Human-readable description for the LLM
    fn description(&self) -> &str;

    /// Execute the tool with typed input
    async fn call(&self, input: Self::Input) -> Result<Self::Output, ExecutionError>;
}
```

**Key points:**
- Async execution for I/O operations
- Type-safe Input/Output
- Automatic JSON schema generation via `JsonSchema` derive
- Send + Sync for multi-threading

### ToolExecutor Trait

Type-erased trait for runtime execution (auto-implemented):

```rust
#[async_trait]
pub trait ToolExecutor: Send + Sync {
    /// Get OpenAI-compatible tool descriptor
    fn descriptor(&self) -> ToolDescriptor;

    /// Execute with JSON input/output
    async fn execute_json(&self, input: Value) -> Result<Value, ExecutionError>;
}
```

**Automatic implementation:**

```rust
impl<T: Tool> ToolExecutor for T
where
    T::Input: JsonSchema,
    T::Output: JsonSchema,
{
    // Automatically bridges type-safe Tool ↔ JSON ToolExecutor
}
```

You never implement `ToolExecutor` manually—it's auto-generated for any `Tool`.

### ToolCall and ToolResult

```rust
// From LLM response
pub struct ToolCall {
    pub id: String,        // "call_123"
    pub name: String,      // "calculator"
    pub arguments: Value,  // {"expression": "2 + 2"}
}

// After execution
pub struct ToolResult {
    pub call_id: String,   // "call_123"
    pub output: Value,     // {"result": 4.0}
}
```

## Implementing Tools

### Simple Stateless Tool

```rust
struct DateTimeTool;

#[async_trait]
impl Tool for DateTimeTool {
    type Input = DateTimeInput;
    type Output = DateTimeOutput;

    fn name(&self) -> &str { "get_datetime" }
    fn description(&self) -> &str { "Gets current date and time" }

    async fn call(&self, _input: Self::Input) -> Result<Self::Output, ExecutionError> {
        Ok(DateTimeOutput {
            datetime: chrono::Utc::now().to_rfc3339(),
        })
    }
}
```

### Tool with State

```rust
struct DatabaseTool {
    connection_pool: Arc<Pool>,
}

#[async_trait]
impl Tool for DatabaseTool {
    type Input = QueryInput;
    type Output = QueryOutput;

    fn name(&self) -> &str { "database_query" }
    fn description(&self) -> &str { "Executes SQL queries" }

    async fn call(&self, input: Self::Input) -> Result<Self::Output, ExecutionError> {
        let conn = self.connection_pool.get().await?;
        let rows = conn.query(&input.sql, &[]).await?;

        Ok(QueryOutput {
            rows: rows.into_iter().map(|r| row_to_json(r)).collect(),
        })
    }
}
```

### Tool with File I/O

```rust
struct FileSaverTool {
    base_path: PathBuf,
    chat_metadata: String,
}

#[async_trait]
impl Tool for FileSaverTool {
    type Input = SaveFileInput;
    type Output = SaveFileOutput;

    fn name(&self) -> &str { "save_file" }
    fn description(&self) -> &str { "Saves content to a file" }

    async fn call(&self, input: Self::Input) -> Result<Self::Output, ExecutionError> {
        // Add metadata header
        let header = format!("# Created from: {}\n\n", self.chat_metadata);
        let content = format!("{}{}", header, input.content);

        // Write file
        let path = self.base_path.join(&input.filename);
        tokio::fs::write(&path, content).await
            .map_err(|e| ExecutionError::Io(e.to_string()))?;

        Ok(SaveFileOutput {
            saved_path: path.to_string_lossy().to_string(),
            bytes_written: content.len(),
        })
    }
}
```

### Tool with API Call

```rust
struct WeatherTool {
    api_key: String,
    client: reqwest::Client,
}

#[async_trait]
impl Tool for WeatherTool {
    type Input = WeatherInput;
    type Output = WeatherOutput;

    fn name(&self) -> &str { "get_weather" }
    fn description(&self) -> &str { "Gets current weather for a location" }

    async fn call(&self, input: Self::Input) -> Result<Self::Output, ExecutionError> {
        let url = format!(
            "https://api.weather.com/v1/current?location={}&key={}",
            input.location, self.api_key
        );

        let response: WeatherApiResponse = self.client
            .get(&url)
            .send()
            .await
            .map_err(|e| ExecutionError::ToolExecutionFailed(e.to_string()))?
            .json()
            .await
            .map_err(|e| ExecutionError::ToolExecutionFailed(e.to_string()))?;

        Ok(WeatherOutput {
            temperature: response.temp,
            conditions: response.conditions,
            humidity: response.humidity,
        })
    }
}
```

## Tool Registry

### Creating a Registry

```rust
let registry = ToolRegistry::new()
    .register(CalculatorTool)
    .register(WeatherTool { api_key: "...".into(), client })
    .register(DatabaseTool { connection_pool });
```

### Getting Tool Descriptors

For OpenAI-compatible APIs:

```rust
let descriptors = registry.descriptors();
// Returns Vec<ToolDescriptor> with JSON schemas

// Send to LLM API
let tools_json = serde_json::to_value(&descriptors)?;
```

### Executing Tools

Single tool:

```rust
let tool_call = ToolCall {
    id: "call_1".to_string(),
    name: "calculator".to_string(),
    arguments: json!({"expression": "2 + 2"}),
};

let result = registry.execute(&tool_call).await?;
// Returns ToolResult { call_id: "call_1", output: {"result": 4.0} }
```

Multiple tools in parallel:

```rust
let calls = vec![
    ToolCall { /* calculator */ },
    ToolCall { /* weather */ },
    ToolCall { /* database */ },
];

let results: Vec<Result<ToolResult>> = registry.execute_all(calls).await;
```

## Integration Patterns

### Pattern 1: Single Node with Inline Tool Execution

**Best for:** Simple workflows, tools don't need complex orchestration

```rust
struct MyNode {
    registry: ToolRegistry,
}

impl Node for MyNode {
    fn run(&self, context: Context) -> LocalBoxStream<'static, NodeEvent<Context>> {
        let registry = self.registry.clone();

        Box::pin(stream! {
            let mut prompt_stream = create_prompt_stream();
            let mut updated_context = context;

            while let Some(event) = prompt_stream.next().await {
                match event {
                    PromptExecutionEvent::ToolCallsRequested(calls) => {
                        yield NodeEvent::Prompt(id, event);

                        // Execute tools immediately
                        for call in &calls {
                            match registry.execute(call).await {
                                Ok(result) => {
                                    // Update context with tool results
                                    apply_tool_result(&mut updated_context, &result);

                                    yield NodeEvent::Prompt(
                                        id,
                                        PromptExecutionEvent::ToolCompleted {
                                            call_id: result.call_id.clone(),
                                            result,
                                        }
                                    );
                                }
                                Err(e) => {
                                    yield NodeEvent::Prompt(id, PromptExecutionEvent::Error(e));
                                }
                            }
                        }
                    }
                    other => {
                        yield NodeEvent::Prompt(id, other);
                    }
                }
            }

            yield NodeEvent::Completed(updated_context);
        })
    }

    fn select_next_node(&self, _ctx: &Context) -> Option<Box<dyn NodeRunner<Context>>> {
        None // Single node handles everything
    }
}
```

**Advantages:**
- Simple, easy to understand
- Tools execute immediately
- No routing complexity

**Use when:**
- Workflow is straightforward
- Tools don't need special handling
- You want minimal code

### Pattern 2: Multi-Node Routing

**Best for:** Complex workflows, different tool types need different handling

```rust
// Node 1: Request tools
impl Node for RequestNode {
    fn select_next_node(&self, ctx: &Context) -> Option<Box<dyn NodeRunner<Context>>> {
        if ctx.tool_calls.is_some() {
            Some(Box::new(NodeWrapper::new(ToolExecutorNode { registry })))
        } else {
            None
        }
    }
}

// Node 2: Execute tools
impl Node for ToolExecutorNode {
    fn run(&self, context: Context) -> LocalBoxStream<'static, NodeEvent<Context>> {
        // Execute all tool calls from context
    }

    fn select_next_node(&self, ctx: &Context) -> Option<Box<dyn NodeRunner<Context>>> {
        // Route based on tool results
        if needs_more_processing(ctx) {
            Some(Box::new(NodeWrapper::new(ProcessNode { ... })))
        } else {
            None
        }
    }
}
```

**Advantages:**
- Clear separation of concerns
- Each node has single responsibility
- Easier to test
- More flexible routing

**Use when:**
- Different tools need different post-processing
- Complex routing logic
- Want to test tool execution separately

### Pattern 3: Tool with Internal Sub-Logic

**Best for:** Tools that need complex operations (e.g., running their own pipelines)

```rust
struct SaveAndTagTool {
    existing_tags: Vec<String>,
    base_path: PathBuf,
}

#[async_trait]
impl Tool for SaveAndTagTool {
    type Input = SaveFileInput;
    type Output = SaveAndTagOutput;

    async fn call(&self, input: Self::Input) -> Result<Self::Output, ExecutionError> {
        // 1. Save the file
        let path = self.save_file(&input.filename, &input.content).await?;

        // 2. Run tag extraction (internal logic)
        let tags = self.extract_tags(&input.content, &self.existing_tags).await?;

        // 3. Return combined results
        Ok(SaveAndTagOutput {
            saved_path: path,
            extracted_tags: tags,
        })
    }
}

impl SaveAndTagTool {
    async fn extract_tags(&self, content: &str, existing: &[String]) -> Result<Vec<String>> {
        // Could run a sub-pipeline here
        // Or call another LLM
        // Or use ML model
        // Tool encapsulates the complexity
    }
}
```

**Advantages:**
- Tool encapsulates complexity
- Reusable across different pipelines
- Clean interface for LLM

**Use when:**
- Tool needs multiple internal steps
- Logic belongs to the tool, not the pipeline
- Want maximum reusability

## Advanced Topics

### Tool with Validation

```rust
#[async_trait]
impl Tool for DatabaseTool {
    async fn call(&self, input: Self::Input) -> Result<Self::Output, ExecutionError> {
        // Validate input
        if !self.is_safe_query(&input.sql) {
            return Err(ExecutionError::ToolExecutionFailed(
                "Query contains forbidden operations".into()
            ));
        }

        // Execute
        let result = self.execute_query(&input.sql).await?;
        Ok(result)
    }
}
```

### Tool with Retries

```rust
#[async_trait]
impl Tool for ApiTool {
    async fn call(&self, input: Self::Input) -> Result<Self::Output, ExecutionError> {
        let mut attempts = 0;
        let max_attempts = 3;

        loop {
            match self.try_api_call(&input).await {
                Ok(result) => return Ok(result),
                Err(e) if attempts < max_attempts && is_retryable(&e) => {
                    attempts += 1;
                    tokio::time::sleep(Duration::from_secs(2_u64.pow(attempts))).await;
                }
                Err(e) => return Err(ExecutionError::ToolExecutionFailed(e.to_string())),
            }
        }
    }
}
```

### Tool with Progress Updates

Tools can't emit events directly, but can update shared state:

```rust
struct LongRunningTool {
    progress: Arc<Mutex<f64>>,
}

#[async_trait]
impl Tool for LongRunningTool {
    async fn call(&self, input: Self::Input) -> Result<Self::Output, ExecutionError> {
        for step in 0..100 {
            // Do work
            process_step(step).await?;

            // Update progress
            *self.progress.lock().unwrap() = step as f64 / 100.0;
        }

        Ok(output)
    }
}

// In node, poll progress and emit custom events
```

### Dynamic Tool Registration

```rust
fn create_registry_from_config(config: &ToolConfig) -> ToolRegistry {
    let mut registry = ToolRegistry::new();

    for tool_def in &config.tools {
        match tool_def.name.as_str() {
            "calculator" => registry = registry.register(CalculatorTool),
            "weather" => registry = registry.register(WeatherTool { /* config */ }),
            "database" => registry = registry.register(DatabaseTool { /* config */ }),
            _ => eprintln!("Unknown tool: {}", tool_def.name),
        }
    }

    registry
}
```

## Best Practices

### 1. Clear Tool Naming

✅ **Good:**
```rust
fn name(&self) -> &str { "get_current_weather" }
fn name(&self) -> &str { "search_knowledge_base" }
fn name(&self) -> &str { "calculate_expression" }
```

❌ **Bad:**
```rust
fn name(&self) -> &str { "tool1" }
fn name(&self) -> &str { "helper" }
fn name(&self) -> &str { "doStuff" }
```

### 2. Descriptive Tool Descriptions

✅ **Good:**
```rust
fn description(&self) -> &str {
    "Searches the knowledge base for relevant documents. \
     Returns up to 10 documents ranked by relevance. \
     Use when the user asks about past conversations or stored information."
}
```

❌ **Bad:**
```rust
fn description(&self) -> &str { "Searches stuff" }
```

### 3. Well-Typed Inputs/Outputs

✅ **Good:**
```rust
#[derive(Deserialize, JsonSchema)]
struct SearchInput {
    /// The search query string
    query: String,
    /// Maximum number of results (1-100)
    #[serde(default = "default_limit")]
    limit: usize,
    /// Filter by date range
    #[serde(default)]
    date_range: Option<DateRange>,
}
```

❌ **Bad:**
```rust
#[derive(Deserialize, JsonSchema)]
struct SearchInput {
    data: serde_json::Value, // Untyped!
}
```

### 4. Error Handling

✅ **Good:**
```rust
async fn call(&self, input: Self::Input) -> Result<Self::Output, ExecutionError> {
    let result = self.api_client.query(&input.query).await
        .map_err(|e| ExecutionError::ToolExecutionFailed(
            format!("API query failed: {}", e)
        ))?;

    if result.is_empty() {
        return Err(ExecutionError::ToolExecutionFailed(
            "No results found for query".into()
        ));
    }

    Ok(output)
}
```

### 5. Validation

```rust
async fn call(&self, input: Self::Input) -> Result<Self::Output, ExecutionError> {
    // Validate before execution
    if input.limit > 100 {
        return Err(ExecutionError::ToolInputParse(
            "Limit must be <= 100".into()
        ));
    }

    if input.query.len() < 3 {
        return Err(ExecutionError::ToolInputParse(
            "Query must be at least 3 characters".into()
        ));
    }

    // Execute
    // ...
}
```

### 6. Keep Tools Focused

✅ **Good:** Multiple focused tools
```rust
struct GetWeatherTool;      // Gets current weather
struct GetForecastTool;     // Gets 7-day forecast
struct GetHistoricalTool;   // Gets historical data
```

❌ **Bad:** One tool does everything
```rust
struct WeatherTool {
    // Has 20 different operation modes
    // Complex input with many optional fields
    // Hard for LLM to use correctly
}
```

### 7. Document JSON Schemas

```rust
#[derive(Deserialize, JsonSchema)]
struct ComplexInput {
    /// The primary query string (required)
    #[schemars(description = "Main search query, supports boolean operators")]
    query: String,

    /// Filter results by category
    #[schemars(description = "One of: 'docs', 'code', 'images'")]
    category: Option<String>,

    /// Maximum results to return
    #[schemars(range(min = 1, max = 100))]
    limit: usize,
}
```

## Testing Tools

### Unit Testing

```rust
#[tokio::test]
async fn test_calculator_tool() {
    let tool = CalculatorTool;

    let input = CalculatorInput {
        expression: "2 + 2".to_string(),
    };

    let result = tool.call(input).await.unwrap();
    assert_eq!(result.result, 4.0);
}
```

### Integration Testing

```rust
#[tokio::test]
async fn test_tool_in_pipeline() {
    let registry = ToolRegistry::new().register(CalculatorTool);

    let context = MyContext {
        expression: "123 * 456".to_string(),
        result: None,
    };

    let pipeline = Pipeline::new(Box::new(NodeWrapper::new(MathNode { registry })));

    // Run and verify
    let final_ctx = run_pipeline_to_completion(pipeline, context).await;
    assert_eq!(final_ctx.result, Some(56088.0));
}
```

### Mock Tools for Testing

```rust
struct MockDatabaseTool {
    mock_results: Vec<Row>,
}

#[async_trait]
impl Tool for MockDatabaseTool {
    async fn call(&self, _input: Self::Input) -> Result<Self::Output, ExecutionError> {
        Ok(QueryOutput {
            rows: self.mock_results.clone(),
        })
    }
}
```

## Common Patterns

See `tests/` directory for complete examples:
- `tools_pipeline.rs` - Basic calculator tool
- `file_saver_tool.rs` - File operations with metadata
- `file_saver_with_tag_extraction.rs` - Multi-node routing pattern
- `tool_with_subpipeline.rs` - Tool with internal logic pattern

## Troubleshooting

**Tool not found:**
```
Error: ToolNotFound("calculator")
```
→ Check tool name matches exactly (case-sensitive)
→ Verify tool is registered in registry

**JSON parsing error:**
```
Error: ToolInputParse("missing field `expression`")
```
→ LLM didn't provide required field
→ Improve tool description
→ Make field optional with `Option<T>`

**Type mismatch:**
```
Error: ToolInputParse("invalid type: string, expected f64")
```
→ LLM provided wrong type
→ Update JSON schema description
→ Add validation in tool

**Send/Sync errors:**
```
Error: future cannot be sent between threads
```
→ Ensure all tool state is Send + Sync
→ Use `Arc<Mutex<T>>` for shared mutable state
→ Avoid `Rc`, `Cell`, or `!Send` types
