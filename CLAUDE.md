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

### 5. Transport Layer (`transport/`)

Built-in providers (`llm_executors/`) are otherwise-identical; everything
platform-specific about *talking to the server* lives here.

- **`HttpRequest`**: Platform-agnostic request description (url, optional api_key, JSON body).
- **`HttpTransport` trait** — deliberately **two cfg-gated definitions**, one per platform, because they disagree on `Send`:
  - `transport::host::HttpTransport` (+ `ReqwestTransport`) — native, `Send + Sync` futures/streams for the tokio runtime, body streamed via `reqwest::bytes_stream()`.
  - `transport::wasm::HttpTransport` (+ `FetchTransport`) — browser, **no** `Send` bounds; built on the Fetch API + `ReadableStream` (`reqwest`'s wasm backend cannot stream bodies). Fetch futures are `!Send`.
  - `transport::{HttpTransport, DefaultTransport, ByteStream}` are cfg aliases resolving to the right platform variant, so providers and the engine are written once.
- **`run_chat_completion` (`engine.rs`)**: The single, generic, platform-agnostic SSE engine. Sends the request, parses Server-Sent-Events, forwards tokens, accumulates the final `Output`. Providers differ only in request body + `CompletionOptions { emit_reasoning, fallback_to_thinking }`.

### Cross-platform support (host + `wasm32`)

The crate compiles for both native hosts and `wasm32-unknown-unknown` (browser).
Platform divergences are funnelled through **`platform.rs`** (`spawn_output`,
`spawn_local`, `OutputFuture`) and the two transport traits. Other rules:

- `std::time::Instant` panics on wasm — use `web_time::Instant` (already used in the engine).
- `Cargo.toml` splits deps per target: host gets `tokio` (`full`) + `reqwest`; wasm gets `tokio` (`sync` only), `wasm-bindgen(-futures)`, `js-sys`, `web-sys`, and `uuid` with the `js` feature.
- `TokenStream` and `PromptExecutor` are cfg-gated (`Send` on host, not on wasm).
- Verify both: `cargo test` (host) and `cargo check --target wasm32-unknown-unknown` (browser).

**Providers are generic over the transport** (`OpenAI<T = DefaultTransport>`, etc.).
`build()` uses the platform default; `build_with_transport(t)` injects a custom
one — used by `tests/transport.rs` to drive the engine and providers with a mock
transport (no network). To use the browser transport explicitly:

```rust
// In a wasm32 build, FetchTransport is the DefaultTransport, so plain
// `.build()` already targets the Fetch API. To be explicit:
use arti_pipes::transport::wasm::FetchTransport;
let executor = OpenAI::builder()
    .api_base("https://api.openai.com/v1")
    .api_key(key)
    .model("gpt-4o")
    .build_with_transport(FetchTransport::new());
```

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

Cross-platform (all targets):

- **futures**: Stream combinators and async utilities
- **serde/serde_json**: Serialization (with derive features)
- **uuid**: Unique IDs for prompt executions (v4, v7, serde; `js` feature added on wasm)
- **schemars**: JSON schema generation for structured outputs
- **thiserror**: Error type derivation
- **async-stream / async-trait**: Stream macro & async-trait utilities
- **tokio-stream**: `UnboundedReceiverStream` for token channels
- **web-time**: Cross-platform `Instant` (std on host, `performance.now()` on wasm)

Host-only (`cfg(not(target_arch = "wasm32"))`):

- **tokio** (`full`): Async runtime
- **reqwest** (`json`, `stream`): HTTP client with streaming bodies
- **toml**: Config parsing

WASM-only (`cfg(target_arch = "wasm32")`):

- **tokio** (`sync` only): channels; the runtime/net/fs do not compile to wasm
- **wasm-bindgen / wasm-bindgen-futures / js-sys / web-sys**: Fetch API bindings + `spawn_local`
