# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

`arti-pipes` is a Rust library for building executor-agnostic prompt pipelines. It provides a framework for chaining multi-step LLM workflows without coupling to specific execution implementations (OpenAI, Anthropic, mock executors, caching layers, etc.).

## Common Commands

```bash
# Build the library
cargo build

# Run tests
cargo test

# Check code without building
cargo check

# Build with release optimizations
cargo build --release

# Generate and view documentation
cargo doc --open
```

## Core Architecture

The library uses a **three-layer separation of concerns**:

### 1. Pipeline Layer (`pipeline.rs`)
- Orchestrates execution of node chains
- `Pipeline<Context>` starts with a first node and follows the chain by calling `next_node` until `None`
- `run_pipeline()` returns a stream of `NodeEvent<Context>`
- `run_pipeline_with_stream()` sends events to a custom sink

### 2. Node Layer (`node.rs`)
- **Node trait**: Concrete node implementations that create prompts and select next nodes
- **NodeRunner trait**: Type-erased trait for dynamic node execution
- **NodeWrapper**: Bridges concrete `Node` implementations to the `NodeRunner` trait
- **NodeEvent enum**: Events during node execution (`Prompt`, `Completed`)

### 3. Executor Layer (`executor.rs`)
- **PromptExecutor trait**: Abstracts prompt execution (must be implemented by users)
- Returns `ExecutionResult<T>` with:
  - `thinking_stream`: Stream of reasoning tokens
  - `content_stream`: Stream of response tokens
  - `output`: Future for final `Output<T>` with metadata

### 4. Prompt Layer (`prompt.rs`)
- **Prompt trait**: Defines LLM interactions within nodes
- Key methods:
  - `serialize()`: Convert prompt to string
  - `execute<E: PromptExecutor>()`: Execute using provided executor
  - `update_context()`: Apply output to context
- **PromptExecutionEvent**: Streaming events (`Scheduled`, `ThinkingToken`, `ContentToken`, `Error`, `Completed`)

## Key Design Patterns

**Type Safety**: Context types are enforced at compile time. Each node defines its `Context` type, and all nodes in a pipeline must share the same context type.

**Dynamic Dispatch**: Nodes are stored as `Box<dyn NodeRunner<Context>>` to allow runtime branching. Use `NodeWrapper::new(node)` to convert concrete nodes to trait objects.

**Streaming First**: All execution flows through async streams (`LocalBoxStream`), enabling real-time token streaming for UI updates.

**Executor Injection**: Nodes create their own executor via `prompt_executor()`, allowing different execution strategies per node.

## Implementation Flow

To implement a pipeline:

1. **Define Context**: Create a struct that flows through the pipeline
2. **Implement PromptExecutor**: Define how prompts are executed (API calls, mocks, etc.)
3. **Implement Prompt**: Define prompt serialization, parsing, and context updates
4. **Implement Node**: Create prompts, execute them, select next nodes
5. **Build Pipeline**: Use `Pipeline::new(Box::new(NodeWrapper::new(first_node)))`
6. **Run**: Call `run_pipeline(pipeline, context)` and consume the event stream

## Error Handling

- All errors use `ExecutionError` from `error.rs`
- Errors flow through `PromptExecutionEvent::Error` in the stream
- No silent error catching - errors must propagate to the stream

## Dependencies

- **tokio**: Async runtime (full features enabled)
- **futures**: Stream combinators and async utilities
- **serde/serde_json**: Serialization (with derive features)
- **uuid**: Unique IDs for prompt executions (v4, v7, serde features)
- **schemars**: JSON schema generation for structured outputs
- **thiserror**: Error type derivation
- **async-stream**: Stream macro utilities
