//! Native (host) HTTP transport backed by `reqwest` + tokio.

use std::future::Future;
use std::pin::Pin;

use futures::{Stream, StreamExt};
use reqwest::Client;

use super::HttpRequest;
use crate::error::{ExecutionError, Result};

/// Stream of response-body byte chunks.
///
/// `Send` so it can be driven on the multi-threaded tokio runtime.
pub type ByteStream = Pin<Box<dyn Stream<Item = Result<Vec<u8>>> + Send>>;

/// Host-side transport contract: send a request, get a streaming body back.
///
/// The native counterpart of [`crate::transport::wasm::HttpTransport`]. The
/// `Send` bounds on the implementor, the returned future, and the
/// [`ByteStream`] are what set the two traits apart — they let everything run
/// across tokio worker threads.
pub trait HttpTransport: Clone + Send + Sync + 'static {
    /// Send `request` and resolve to a stream of response-body byte chunks.
    ///
    /// Implementations must surface a non-2xx status as an `Err`, not a
    /// successful empty stream.
    fn send(&self, request: HttpRequest) -> impl Future<Output = Result<ByteStream>> + Send;
}

/// Default host transport using a shared, connection-pooling `reqwest::Client`.
#[derive(Clone, Default)]
pub struct ReqwestTransport {
    client: Client,
}

impl ReqwestTransport {
    /// Create a transport with a fresh `reqwest::Client`.
    pub fn new() -> Self {
        Self {
            client: Client::new(),
        }
    }

    /// Create a transport reusing an existing `reqwest::Client`.
    pub fn with_client(client: Client) -> Self {
        Self { client }
    }
}

impl HttpTransport for ReqwestTransport {
    async fn send(&self, request: HttpRequest) -> Result<ByteStream> {
        let mut builder = self.client.post(&request.url).json(&request.body);

        if let Some(key) = request.api_key.filter(|k| !k.is_empty()) {
            builder = builder.header("Authorization", format!("Bearer {key}"));
        }

        let response = builder
            .send()
            .await
            .map_err(|e| ExecutionError::ModelExecution(e.to_string()))?;

        if !response.status().is_success() {
            let status = response.status();
            let text = response.text().await.unwrap_or_default();
            return Err(ExecutionError::ModelExecution(format!(
                "HTTP {status}: {text}"
            )));
        }

        let stream = response.bytes_stream().map(|chunk| {
            chunk
                .map(|bytes| bytes.to_vec())
                .map_err(|e| ExecutionError::ModelExecution(e.to_string()))
        });

        Ok(Box::pin(stream))
    }
}
