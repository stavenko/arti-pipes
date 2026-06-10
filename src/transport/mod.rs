//! Universal HTTP transport for LLM requests.
//!
//! Every built-in provider ([`crate::llm_executors`]) is identical except for
//! the JSON body it builds and a couple of streaming flags. Everything
//! platform-specific about *talking to the server* — opening the connection
//! and reading the streamed response body — is isolated behind a single
//! transport contract with two implementations:
//!
//! - [`host::ReqwestTransport`] — native hosts, backed by `reqwest` + tokio.
//! - [`wasm::FetchTransport`] — the browser, backed by the Fetch API and a
//!   `ReadableStream` reader.
//!
//! The contract is deliberately **two traits**, not one, because the platforms
//! disagree on `Send`: [`host::HttpTransport`] requires `Send` futures/streams
//! so they can cross tokio worker threads, while [`wasm::HttpTransport`] cannot
//! — Fetch futures and stream readers are `!Send`. A single trait could not
//! express both. The shared SSE engine ([`run_chat_completion`]) and the
//! providers are written once against whichever trait the target selects via
//! the [`HttpTransport`]/[`DefaultTransport`]/[`ByteStream`] aliases below.

mod engine;

#[cfg(not(target_arch = "wasm32"))]
pub mod host;
#[cfg(target_arch = "wasm32")]
pub mod wasm;

pub use engine::{run_chat_completion, CompletionOptions};

#[cfg(not(target_arch = "wasm32"))]
pub use host::{ByteStream, HttpTransport, ReqwestTransport as DefaultTransport};
#[cfg(target_arch = "wasm32")]
pub use wasm::{ByteStream, FetchTransport as DefaultTransport, HttpTransport};

use serde_json::Value;

/// Platform-agnostic description of a request to an LLM chat-completions
/// endpoint.
///
/// Providers serialize their typed request struct into [`HttpRequest::body`];
/// the transport is intentionally ignorant of provider-specific shapes.
#[derive(Debug, Clone)]
pub struct HttpRequest {
    /// Full URL of the endpoint (e.g. `https://api.openai.com/v1/chat/completions`).
    pub url: String,
    /// Bearer token. `None` or an empty string means no `Authorization` header.
    pub api_key: Option<String>,
    /// JSON request body.
    pub body: Value,
}

impl HttpRequest {
    /// Construct a new request description.
    pub fn new(url: String, api_key: Option<String>, body: Value) -> Self {
        Self { url, api_key, body }
    }
}
