//! Tests for the universal transport layer and the shared SSE engine.
//!
//! These exercise the request/response machinery in isolation by injecting a
//! [`MockTransport`] that records the outgoing [`HttpRequest`] and replays a
//! canned stream of response-body byte chunks. No network or live LLM server
//! is involved.

use std::sync::{Arc, Mutex};

use arti_pipes::error::Result;
use arti_pipes::executor::{PromptExecutor, Token};
use arti_pipes::llm_executors::{DeepSeek, GptOss, OpenAI, Qwen};
use arti_pipes::transport::{
    run_chat_completion, ByteStream, CompletionOptions, HttpRequest, HttpTransport,
};
use futures::StreamExt;
use tokio::sync::mpsc;

/// Transport that records the request it was handed and replays fixed chunks.
#[derive(Clone)]
struct MockTransport {
    chunks: Arc<Vec<Vec<u8>>>,
    captured: Arc<Mutex<Option<HttpRequest>>>,
}

impl MockTransport {
    fn new(chunks: Vec<Vec<u8>>) -> Self {
        Self {
            chunks: Arc::new(chunks),
            captured: Arc::new(Mutex::new(None)),
        }
    }

    /// The request handed to the most recent `send`.
    fn captured(&self) -> HttpRequest {
        self.captured
            .lock()
            .unwrap()
            .clone()
            .expect("transport was never called")
    }
}

impl HttpTransport for MockTransport {
    async fn send(&self, request: HttpRequest) -> Result<ByteStream> {
        *self.captured.lock().unwrap() = Some(request);
        let items: Vec<Result<Vec<u8>>> = self.chunks.iter().cloned().map(Ok).collect();
        Ok(Box::pin(futures::stream::iter(items)))
    }
}

/// Build a single SSE `data:` frame (newline-terminated) from a JSON payload.
fn sse(json: &str) -> Vec<u8> {
    format!("data: {json}\n").into_bytes()
}

/// A streaming chunk carrying a content delta.
fn content_frame(text: &str) -> Vec<u8> {
    sse(&format!(
        r#"{{"choices":[{{"delta":{{"content":{}}},"finish_reason":null}}]}}"#,
        serde_json::to_string(text).unwrap()
    ))
}

/// A streaming chunk carrying a reasoning delta.
fn reasoning_frame(text: &str) -> Vec<u8> {
    sse(&format!(
        r#"{{"choices":[{{"delta":{{"reasoning":{}}},"finish_reason":null}}]}}"#,
        serde_json::to_string(text).unwrap()
    ))
}

fn done_frame() -> Vec<u8> {
    b"data: [DONE]\n".to_vec()
}

/// Drain a receiver that has already been closed (the engine has finished and
/// dropped its sender), returning the token contents in order.
fn drain(mut rx: mpsc::UnboundedReceiver<Result<Token>>) -> Vec<String> {
    let mut out = Vec::new();
    while let Ok(item) = rx.try_recv() {
        out.push(item.expect("token error").content);
    }
    out
}

fn request() -> HttpRequest {
    HttpRequest::new("http://mock/chat/completions".to_string(), None, serde_json::json!({}))
}

// === Engine tests ===

#[tokio::test]
async fn engine_streams_content_tokens_and_accumulates_output() {
    let transport = MockTransport::new(vec![
        content_frame("Hello"),
        content_frame(" world"),
        done_frame(),
    ]);
    let (ttx, trx) = mpsc::unbounded_channel();
    let (ctx, crx) = mpsc::unbounded_channel();

    let output = run_chat_completion(
        transport,
        request(),
        "model-x".to_string(),
        CompletionOptions {
            emit_reasoning: false,
            fallback_to_thinking: false,
        },
        ttx,
        ctx,
    )
    .await
    .expect("engine run failed");

    assert_eq!(drain(crx), vec!["Hello", " world"]);
    assert!(drain(trx).is_empty(), "no reasoning expected");
    assert_eq!(output.result, "Hello world");
    assert_eq!(output.metadata.total_tokens, 2);
    assert_eq!(output.metadata.model_id, "model-x");
}

#[tokio::test]
async fn engine_emits_reasoning_when_enabled() {
    let transport = MockTransport::new(vec![
        reasoning_frame("thinking"),
        content_frame("answer"),
        done_frame(),
    ]);
    let (ttx, trx) = mpsc::unbounded_channel();
    let (ctx, crx) = mpsc::unbounded_channel();

    let output = run_chat_completion(
        transport,
        request(),
        "m".to_string(),
        CompletionOptions {
            emit_reasoning: true,
            fallback_to_thinking: false,
        },
        ttx,
        ctx,
    )
    .await
    .unwrap();

    assert_eq!(drain(trx), vec!["thinking"]);
    assert_eq!(drain(crx), vec!["answer"]);
    assert_eq!(output.result, "answer");
}

#[tokio::test]
async fn engine_drops_reasoning_when_disabled() {
    let transport = MockTransport::new(vec![
        reasoning_frame("thinking"),
        content_frame("answer"),
        done_frame(),
    ]);
    let (ttx, trx) = mpsc::unbounded_channel();
    let (ctx, crx) = mpsc::unbounded_channel();

    let output = run_chat_completion(
        transport,
        request(),
        "m".to_string(),
        CompletionOptions {
            emit_reasoning: false,
            fallback_to_thinking: false,
        },
        ttx,
        ctx,
    )
    .await
    .unwrap();

    assert!(drain(trx).is_empty());
    assert_eq!(drain(crx), vec!["answer"]);
    assert_eq!(output.result, "answer");
}

#[tokio::test]
async fn engine_falls_back_to_thinking_when_content_empty() {
    let transport = MockTransport::new(vec![
        reasoning_frame("step one "),
        reasoning_frame("step two"),
        done_frame(),
    ]);
    let (ttx, trx) = mpsc::unbounded_channel();
    let (ctx, crx) = mpsc::unbounded_channel();

    let output = run_chat_completion(
        transport,
        request(),
        "m".to_string(),
        CompletionOptions {
            emit_reasoning: true,
            fallback_to_thinking: true,
        },
        ttx,
        ctx,
    )
    .await
    .unwrap();

    assert_eq!(drain(trx), vec!["step one ", "step two"]);
    assert!(drain(crx).is_empty(), "no content frames were sent");
    assert_eq!(output.result, "step one step two");
}

#[tokio::test]
async fn engine_reassembles_frames_split_across_transport_chunks() {
    // A single SSE frame delivered in two byte chunks that split mid-token.
    let frame = content_frame("Hello");
    let split = frame.len() / 2;
    let transport = MockTransport::new(vec![
        frame[..split].to_vec(),
        frame[split..].to_vec(),
        done_frame(),
    ]);
    let (ttx, trx) = mpsc::unbounded_channel();
    let (ctx, crx) = mpsc::unbounded_channel();

    let output = run_chat_completion(
        transport,
        request(),
        "m".to_string(),
        CompletionOptions::default(),
        ttx,
        ctx,
    )
    .await
    .unwrap();

    assert_eq!(drain(crx), vec!["Hello"]);
    assert!(drain(trx).is_empty());
    assert_eq!(output.result, "Hello");
}

// === Provider tests (request construction + streaming via mock transport) ===

/// Collect a `TokenStream` to a single concatenated string.
async fn collect(mut stream: arti_pipes::executor::TokenStream) -> String {
    let mut acc = String::new();
    while let Some(token) = stream.next().await {
        acc.push_str(&token.expect("token error").content);
    }
    acc
}

#[tokio::test]
async fn openai_builds_expected_request_and_streams() {
    let mock = MockTransport::new(vec![content_frame("Hi"), done_frame()]);
    let executor = OpenAI::builder()
        .api_base("http://api")
        .model("gpt-x")
        .reasoning_effort("high")
        .build_with_transport(mock.clone());

    let result = executor.execute_raw("hello".to_string()).await.unwrap();
    let content = collect(result.content_stream).await;
    let output = result.output.await.unwrap();

    assert_eq!(content, "Hi");
    assert_eq!(output.result, "Hi");

    let req = mock.captured();
    assert_eq!(req.body["model"], "gpt-x");
    assert_eq!(req.body["stream"], true);
    assert_eq!(req.body["messages"][0]["role"], "user");
    assert_eq!(req.body["messages"][0]["content"], "hello");
    assert_eq!(req.body["reasoning_effort"], "high");
}

#[tokio::test]
async fn qwen_gates_reasoning_on_think_flag() {
    let frames = || vec![reasoning_frame("hmm"), content_frame("ok"), done_frame()];

    // think = false: reasoning must be suppressed.
    let mock_off = MockTransport::new(frames());
    let off = Qwen::builder()
        .api_base("http://api")
        .model("qwen")
        .think(false)
        .build_with_transport(mock_off.clone());
    let result = off.execute_raw("q".to_string()).await.unwrap();
    let thinking = collect(result.thinking_stream).await;
    let content = collect(result.content_stream).await;
    result.output.await.unwrap();
    assert_eq!(thinking, "");
    assert_eq!(content, "ok");
    assert_eq!(mock_off.captured().body["think"], false);

    // think = true: reasoning must flow.
    let mock_on = MockTransport::new(frames());
    let on = Qwen::builder()
        .api_base("http://api")
        .model("qwen")
        .think(true)
        .build_with_transport(mock_on.clone());
    let result = on.execute_raw("q".to_string()).await.unwrap();
    let thinking = collect(result.thinking_stream).await;
    let content = collect(result.content_stream).await;
    result.output.await.unwrap();
    assert_eq!(thinking, "hmm");
    assert_eq!(content, "ok");
    assert_eq!(mock_on.captured().body["think"], true);
}

#[tokio::test]
async fn gpt_oss_falls_back_to_thinking_when_no_content() {
    let mock = MockTransport::new(vec![reasoning_frame("only thoughts"), done_frame()]);
    let executor = GptOss::builder()
        .api_base("http://api")
        .model("gpt-oss")
        .reasoning_effort("low")
        .build_with_transport(mock.clone());

    let result = executor.execute_raw("q".to_string()).await.unwrap();
    let content = collect(result.content_stream).await;
    let output = result.output.await.unwrap();

    assert_eq!(content, "", "no content frames were sent");
    assert_eq!(output.result, "only thoughts");
    assert_eq!(mock.captured().body["think"], "low");
}

#[tokio::test]
async fn deepseek_gates_reasoning_and_omits_provider_specific_fields() {
    let mock = MockTransport::new(vec![
        reasoning_frame("reasoned"),
        content_frame("done"),
        done_frame(),
    ]);
    let executor = DeepSeek::builder()
        .api_base("http://api")
        .model("deepseek")
        .reasoning(true)
        .build_with_transport(mock.clone());

    let result = executor.execute_raw("q".to_string()).await.unwrap();
    let thinking = collect(result.thinking_stream).await;
    let content = collect(result.content_stream).await;
    let output = result.output.await.unwrap();

    assert_eq!(thinking, "reasoned");
    assert_eq!(content, "done");
    assert_eq!(output.result, "done");

    let req = mock.captured();
    assert_eq!(req.body["model"], "deepseek");
    assert_eq!(req.body["stream"], true);
    // DeepSeek has no `think`/`reasoning_effort` knobs in its request body.
    assert!(req.body.get("think").is_none());
    assert!(req.body.get("reasoning_effort").is_none());
}
