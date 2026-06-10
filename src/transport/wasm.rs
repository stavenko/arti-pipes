//! Browser (WASM) HTTP transport backed by the Fetch API + `ReadableStream`.
//!
//! `reqwest`'s wasm backend cannot stream a response body (`bytes_stream` is
//! unavailable there), so we go straight to the Fetch API via `web-sys` and
//! read the body through a `ReadableStreamDefaultReader`. This yields real
//! token-by-token streaming in the browser.

use std::future::Future;
use std::pin::Pin;

use futures::Stream;
use wasm_bindgen::prelude::*;
use wasm_bindgen::JsCast;
use wasm_bindgen_futures::JsFuture;
use web_sys::{Headers, ReadableStream, ReadableStreamDefaultReader, RequestInit, Response};

use super::HttpRequest;
use crate::error::{ExecutionError, Result};

/// Stream of response-body byte chunks.
///
/// Not `Send`: Fetch futures and stream readers live on the single JS event
/// loop.
pub type ByteStream = Pin<Box<dyn Stream<Item = Result<Vec<u8>>>>>;

/// Browser-side transport contract: send a request, get a streaming body back.
///
/// The browser counterpart of [`crate::transport::host::HttpTransport`].
/// Crucially it has **no** `Send` bounds, because the Fetch API objects it
/// builds on are `!Send`.
pub trait HttpTransport: Clone + 'static {
    /// Send `request` and resolve to a stream of response-body byte chunks.
    fn send(&self, request: HttpRequest) -> impl Future<Output = Result<ByteStream>>;
}

/// Default browser transport using the global `fetch`.
#[derive(Clone, Default)]
pub struct FetchTransport;

impl FetchTransport {
    /// Create a new browser transport.
    pub fn new() -> Self {
        Self
    }
}

/// Wrap a JS error value into an [`ExecutionError`] with context. Accepts any
/// JS type that converts into a `JsValue` (e.g. `JsValue`, `js_sys::Object`).
fn js_err(context: &str, value: impl Into<JsValue>) -> ExecutionError {
    ExecutionError::ModelExecution(format!("{context}: {:?}", value.into()))
}

impl HttpTransport for FetchTransport {
    async fn send(&self, request: HttpRequest) -> Result<ByteStream> {
        let opts = RequestInit::new();
        opts.set_method("POST");

        let headers = Headers::new().map_err(|e| js_err("create headers", e))?;
        headers
            .set("Content-Type", "application/json")
            .map_err(|e| js_err("set content-type header", e))?;
        if let Some(key) = request.api_key.filter(|k| !k.is_empty()) {
            headers
                .set("Authorization", &format!("Bearer {key}"))
                .map_err(|e| js_err("set authorization header", e))?;
        }
        opts.set_headers(&headers);

        let body = serde_json::to_string(&request.body)?;
        opts.set_body(&JsValue::from_str(&body));

        let req = web_sys::Request::new_with_str_and_init(&request.url, &opts)
            .map_err(|e| js_err("build request", e))?;

        // Resolve `fetch` from the global scope so this works in both window
        // and worker contexts.
        let global = js_sys::global();
        let fetch = js_sys::Reflect::get(&global, &JsValue::from_str("fetch"))
            .map_err(|e| js_err("look up fetch", e))?
            .dyn_into::<js_sys::Function>()
            .map_err(|e| js_err("fetch is not callable", e))?;
        let promise = fetch
            .call1(&global, &req)
            .map_err(|e| js_err("invoke fetch", e))?
            .dyn_into::<js_sys::Promise>()
            .map_err(|e| js_err("fetch did not return a promise", e))?;

        let resp_value = JsFuture::from(promise)
            .await
            .map_err(|e| js_err("fetch failed", e))?;
        let response: Response = resp_value
            .dyn_into()
            .map_err(|e| js_err("fetch result is not a Response", e))?;

        if !response.ok() {
            let status = response.status();
            let text = match response.text() {
                Ok(promise) => JsFuture::from(promise)
                    .await
                    .ok()
                    .and_then(|v| v.as_string())
                    .unwrap_or_default(),
                Err(_) => String::new(),
            };
            return Err(ExecutionError::ModelExecution(format!(
                "HTTP {status}: {text}"
            )));
        }

        let body: ReadableStream = response
            .body()
            .ok_or_else(|| ExecutionError::ModelExecution("response has no body".to_string()))?;
        let reader: ReadableStreamDefaultReader = body
            .get_reader()
            .dyn_into()
            .map_err(|e| js_err("acquire stream reader", e))?;

        let stream = async_stream::stream! {
            loop {
                let result = match JsFuture::from(reader.read()).await {
                    Ok(value) => value,
                    Err(e) => {
                        yield Err(js_err("read response chunk", e));
                        break;
                    }
                };

                let done = js_sys::Reflect::get(&result, &JsValue::from_str("done"))
                    .ok()
                    .and_then(|v| v.as_bool())
                    .unwrap_or(true);
                if done {
                    break;
                }

                let value = match js_sys::Reflect::get(&result, &JsValue::from_str("value")) {
                    Ok(value) => value,
                    Err(e) => {
                        yield Err(js_err("extract chunk value", e));
                        break;
                    }
                };

                yield Ok(js_sys::Uint8Array::new(&value).to_vec());
            }
        };

        Ok(Box::pin(stream))
    }
}
