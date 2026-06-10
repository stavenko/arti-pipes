//! Platform abstractions for native hosts and the browser (`wasm32`).
//!
//! The crate runs both on native hosts (multi-threaded tokio runtime) and in
//! the browser (`wasm32-unknown-unknown`, single JS event loop). The two
//! environments disagree on two fundamentals:
//!
//! - **`Send`**: native futures must be `Send` to cross tokio worker threads,
//!   while browser futures built on the Fetch API are `!Send`.
//! - **spawning**: native code uses `tokio::spawn`/`spawn_local`, the browser
//!   uses `wasm_bindgen_futures::spawn_local`.
//!
//! Everything that differs is funnelled through this module so the rest of the
//! crate is written once.

use std::future::Future;
use std::pin::Pin;

use crate::error::{ExecutionError, Result};
use crate::executor::Output;

/// Future resolving to the final output of an execution.
///
/// `Send` on native hosts (it is driven on the tokio runtime), not `Send` in
/// the browser.
#[cfg(not(target_arch = "wasm32"))]
pub type OutputFuture<T> = Pin<Box<dyn Future<Output = Result<Output<T>>> + Send>>;

/// Future resolving to the final output of an execution.
#[cfg(target_arch = "wasm32")]
pub type OutputFuture<T> = Pin<Box<dyn Future<Output = Result<Output<T>>>>>;

/// Spawn the background work that produces an execution's final output and
/// return a future that resolves once it finishes.
///
/// The work starts running immediately, so the token streams begin flowing
/// before the returned [`OutputFuture`] is awaited.
#[cfg(not(target_arch = "wasm32"))]
pub fn spawn_output<T, F>(fut: F) -> OutputFuture<T>
where
    T: Send + 'static,
    F: Future<Output = Result<Output<T>>> + Send + 'static,
{
    let handle = tokio::spawn(fut);
    Box::pin(async move {
        handle
            .await
            .map_err(|e| ExecutionError::ModelExecution(format!("execution task failed: {e}")))?
    })
}

/// Spawn the background work that produces an execution's final output and
/// return a future that resolves once it finishes.
#[cfg(target_arch = "wasm32")]
pub fn spawn_output<T, F>(fut: F) -> OutputFuture<T>
where
    T: 'static,
    F: Future<Output = Result<Output<T>>> + 'static,
{
    let (tx, rx) = futures::channel::oneshot::channel();
    wasm_bindgen_futures::spawn_local(async move {
        let _ = tx.send(fut.await);
    });
    Box::pin(async move {
        rx.await
            .map_err(|_| ExecutionError::ModelExecution("execution task dropped".to_string()))?
    })
}

/// Spawn a detached, `()`-returning task on the current platform's executor.
///
/// On native hosts this requires a `tokio::task::LocalSet` to be active (as the
/// pipeline driver already assumes); in the browser it schedules onto the JS
/// microtask queue.
#[cfg(not(target_arch = "wasm32"))]
pub fn spawn_local<F>(fut: F)
where
    F: Future<Output = ()> + 'static,
{
    tokio::task::spawn_local(fut);
}

/// Spawn a detached, `()`-returning task on the current platform's executor.
#[cfg(target_arch = "wasm32")]
pub fn spawn_local<F>(fut: F)
where
    F: Future<Output = ()> + 'static,
{
    wasm_bindgen_futures::spawn_local(fut);
}
