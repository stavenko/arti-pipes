//! Shared SSE streaming engine used by every LLM provider.
//!
//! This is the platform-agnostic heart of request handling: it drives a
//! streaming chat-completion through a [`HttpTransport`], parses the
//! Server-Sent-Events body, forwards reasoning/content tokens to their
//! channels, and accumulates the final [`Output`]. The providers differ only
//! in the request body they pass in and the [`CompletionOptions`] flags.

use futures::StreamExt;
use tokio::sync::mpsc::UnboundedSender;
use web_time::Instant;

use super::{HttpRequest, HttpTransport};
use crate::error::Result;
use crate::executor::{Output, OutputMetadata, Token};
use crate::llm_executors::types::parse_sse_line;

/// Behavioural knobs that distinguish the providers' otherwise-identical loops.
#[derive(Debug, Clone, Copy, Default)]
pub struct CompletionOptions {
    /// Forward reasoning/thinking deltas to the thinking stream. Providers that
    /// gate reasoning behind a config flag pass that flag here.
    pub emit_reasoning: bool,
    /// If the content stream ends up empty, use the accumulated reasoning text
    /// as the final result instead (GPT-OSS behaviour).
    pub fallback_to_thinking: bool,
}

/// Drive a streaming chat-completion request to completion.
///
/// Sends `request` through `transport`, parses the streamed SSE body, forwards
/// tokens to the two channels, and returns the final accumulated [`Output`].
/// Send errors on the channels are ignored — a dropped receiver simply means
/// the consumer stopped listening.
pub async fn run_chat_completion<T: HttpTransport>(
    transport: T,
    request: HttpRequest,
    model: String,
    options: CompletionOptions,
    thinking_tx: UnboundedSender<Result<Token>>,
    content_tx: UnboundedSender<Result<Token>>,
) -> Result<Output<String>> {
    let start_time = Instant::now();
    let mut body = transport.send(request).await?;

    let mut thinking_token_index = 0usize;
    let mut content_token_index = 0usize;
    let mut thinking_content = String::new();
    let mut response_content = String::new();
    let mut buffer = String::new();

    while let Some(chunk) = body.next().await {
        let chunk = chunk?;
        buffer.push_str(&String::from_utf8_lossy(&chunk));

        while let Some(newline_pos) = buffer.find('\n') {
            let line = buffer[..newline_pos].to_string();
            buffer = buffer[newline_pos + 1..].to_string();

            let Some(parsed) = parse_sse_line(&line) else {
                continue;
            };

            for choice in parsed.choices {
                if options.emit_reasoning {
                    if let Some(reasoning) = choice.delta.reasoning {
                        if !reasoning.is_empty() {
                            thinking_content.push_str(&reasoning);
                            let token = Token {
                                content: reasoning,
                                index: thinking_token_index,
                            };
                            thinking_token_index += 1;
                            let _ = thinking_tx.send(Ok(token));
                        }
                    }
                }

                if let Some(content) = choice.delta.content {
                    if !content.is_empty() {
                        response_content.push_str(&content);
                        let token = Token {
                            content,
                            index: content_token_index,
                        };
                        content_token_index += 1;
                        let _ = content_tx.send(Ok(token));
                    }
                }
            }
        }
    }

    let final_response = if options.fallback_to_thinking && response_content.is_empty() {
        thinking_content
    } else {
        response_content
    };

    let metadata = OutputMetadata {
        total_tokens: content_token_index,
        generation_time_ms: start_time.elapsed().as_millis() as u64,
        model_id: model,
    };

    Ok(Output::new(final_response, metadata))
}
