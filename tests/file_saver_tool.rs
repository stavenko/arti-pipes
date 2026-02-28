//! Integration test for file saver tool with chat context metadata

use arti_pipes::{
    error::ExecutionError,
    executor::PromptExecutor,
    node::{Node, NodeEvent, NodeRunner, NodeWrapper},
    pipeline::{run_pipeline, Pipeline},
    prompt::{Prompt, PromptExecutionEvent},
    tool::{Tool, ToolCall},
    tool_registry::ToolRegistry,
};
use async_trait::async_trait;
use futures::{stream::LocalBoxStream, StreamExt};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::sync::{Arc, Mutex};

// ===== Context =====

#[derive(Clone, Debug)]
struct ChatContext {
    /// Path to the YAML file containing chat history
    chat_filename: String,
    /// Pending tool calls from LLM
    tool_calls: Option<Vec<ToolCall>>,
    /// Content of the saved file (for verification)
    saved_content: Option<String>,
}

// ===== Mock Executor =====

#[derive(Clone)]
struct MockExecutor;

impl PromptExecutor for MockExecutor {
    async fn execute_raw(
        &self,
        _prompt: String,
    ) -> Result<arti_pipes::executor::ExecutionResult<String>, ExecutionError> {
        Err(ExecutionError::ModelExecution(
            "Not implemented for this test".to_string(),
        ))
    }

    fn execute<T: schemars::JsonSchema>(
        &self,
        _prompt: String,
    ) -> impl std::future::Future<Output = Result<arti_pipes::executor::ExecutionResult<String>, ExecutionError>>
        + Send {
        async {
            Err(ExecutionError::ModelExecution(
                "Not implemented for this test".to_string(),
            ))
        }
    }
}

// ===== File Saver Tool =====

#[derive(Deserialize, JsonSchema)]
struct FileSaverInput {
    filename: String,
    content: String,
}

#[derive(Serialize, JsonSchema)]
struct FileSaverOutput {
    success: bool,
    saved_path: String,
    final_content: String,
}

/// Tool that saves files with chat metadata header
struct FileSaverTool {
    chat_filename: String,
    /// In-memory storage for testing (instead of actual file I/O)
    saved_files: Arc<Mutex<Vec<(String, String)>>>,
}

#[async_trait]
impl Tool for FileSaverTool {
    type Input = FileSaverInput;
    type Output = FileSaverOutput;

    fn name(&self) -> &str {
        "file_saver"
    }

    fn description(&self) -> &str {
        "Saves a file with the given content. Automatically adds metadata about the chat origin."
    }

    async fn call(&self, input: Self::Input) -> Result<Self::Output, ExecutionError> {
        // Prepend the chat metadata header
        let header = format!("# Created from chat: {}\n\n", self.chat_filename);
        let final_content = format!("{}{}", header, input.content);

        // Save to in-memory storage (in real implementation, this would be fs::write)
        self.saved_files
            .lock()
            .unwrap()
            .push((input.filename.clone(), final_content.clone()));

        Ok(FileSaverOutput {
            success: true,
            saved_path: input.filename,
            final_content,
        })
    }
}

// ===== Request Node (simulates assistant asking to save file) =====

struct SaveFilePrompt {
    user_request: String,
}

impl Prompt for SaveFilePrompt {
    type Output = String;
    type Context = ChatContext;

    fn name(&self) -> String {
        "SaveFilePrompt".to_string()
    }

    fn serialize(&self) -> String {
        format!("User wants to save: {}", self.user_request)
    }

    fn update_context(&self, ctx: Self::Context, _data: Self::Output) -> Self::Context {
        ctx
    }

    fn execute<E: PromptExecutor>(
        &self,
        _executor: E,
    ) -> LocalBoxStream<'static, PromptExecutionEvent> {
        // Simulate LLM deciding to save a Python script
        let tool_call = ToolCall {
            id: "call_save_1".to_string(),
            name: "file_saver".to_string(),
            arguments: serde_json::json!({
                "filename": "hello.py",
                "content": "def hello():\n    print('Hello, World!')\n\nhello()"
            }),
        };

        Box::pin(async_stream::stream! {
            yield PromptExecutionEvent::Scheduled("SaveFilePrompt".to_string());
            yield PromptExecutionEvent::ToolCallsRequested(vec![tool_call]);
        })
    }
}

struct SaveFileNode {
    executor: MockExecutor,
    registry: ToolRegistry,
}

impl Node for SaveFileNode {
    type Prompt = SaveFilePrompt;
    type Executor = MockExecutor;
    type Error = String;
    type Context = ChatContext;

    fn prompt(&self, _ctx: &Self::Context) -> Self::Prompt {
        SaveFilePrompt {
            user_request: "Create a Python hello world script".to_string(),
        }
    }

    fn prompt_executor(&self) -> Self::Executor {
        self.executor.clone()
    }

    fn run(&self, context: Self::Context) -> LocalBoxStream<'static, NodeEvent<Self::Context>> {
        let prompt = self.prompt(&context);
        let executor = self.prompt_executor();
        let mut prompt_stream = prompt.execute(executor);

        Box::pin(async_stream::stream! {
            let mut updated_context = context;
            let execution_id = uuid::Uuid::new_v4();

            while let Some(event) = prompt_stream.next().await {
                match &event {
                    PromptExecutionEvent::ToolCallsRequested(calls) => {
                        updated_context.tool_calls = Some(calls.clone());
                    }
                    _ => {}
                }
                yield NodeEvent::Prompt(execution_id, event);
            }

            yield NodeEvent::Completed(updated_context);
        })
    }

    fn select_next_node(
        &self,
        context: &Self::Context,
    ) -> Option<Box<dyn NodeRunner<Self::Context>>> {
        if context.tool_calls.is_some() && context.saved_content.is_none() {
            Some(Box::new(NodeWrapper::new(ToolExecutorNode {
                registry: self.registry.clone(),
            })))
        } else {
            None
        }
    }
}

impl Clone for SaveFileNode {
    fn clone(&self) -> Self {
        Self {
            executor: self.executor.clone(),
            registry: self.registry.clone(),
        }
    }
}

// ===== Tool Executor Node =====

struct ToolExecutorPrompt;

impl Prompt for ToolExecutorPrompt {
    type Output = String;
    type Context = ChatContext;

    fn name(&self) -> String {
        "ToolExecutorPrompt".to_string()
    }

    fn serialize(&self) -> String {
        "Executing tools...".to_string()
    }

    fn update_context(&self, ctx: Self::Context, _data: Self::Output) -> Self::Context {
        ctx
    }

    fn execute<E: PromptExecutor>(
        &self,
        _executor: E,
    ) -> LocalBoxStream<'static, PromptExecutionEvent> {
        Box::pin(async_stream::stream! {
            yield PromptExecutionEvent::Completed("Tools executed".to_string());
        })
    }
}

struct ToolExecutorNode {
    registry: ToolRegistry,
}

impl Node for ToolExecutorNode {
    type Prompt = ToolExecutorPrompt;
    type Executor = MockExecutor;
    type Error = String;
    type Context = ChatContext;

    fn prompt(&self, _ctx: &Self::Context) -> Self::Prompt {
        ToolExecutorPrompt
    }

    fn prompt_executor(&self) -> Self::Executor {
        MockExecutor
    }

    fn run(&self, context: Self::Context) -> LocalBoxStream<'static, NodeEvent<Self::Context>> {
        let registry = self.registry.clone();
        let tool_calls = context.tool_calls.clone();

        Box::pin(async_stream::stream! {
            let execution_id = uuid::Uuid::new_v4();
            let mut updated_context = context;

            if let Some(calls) = tool_calls {
                for call in calls {
                    yield NodeEvent::Prompt(
                        execution_id,
                        PromptExecutionEvent::ToolExecuting {
                            call_id: call.id.clone(),
                            tool_name: call.name.clone(),
                        }
                    );

                    match registry.execute(&call).await {
                        Ok(result) => {
                            yield NodeEvent::Prompt(
                                execution_id,
                                PromptExecutionEvent::ToolCompleted {
                                    call_id: result.call_id.clone(),
                                    result: result.clone(),
                                }
                            );

                            // Extract saved content from result
                            if let Some(final_content) = result.output.get("final_content") {
                                if let Some(content_str) = final_content.as_str() {
                                    updated_context.saved_content = Some(content_str.to_string());
                                }
                            }
                        }
                        Err(e) => {
                            yield NodeEvent::Prompt(
                                execution_id,
                                PromptExecutionEvent::Error(e)
                            );
                        }
                    }
                }
            }

            yield NodeEvent::Completed(updated_context);
        })
    }

    fn select_next_node(
        &self,
        _context: &Self::Context,
    ) -> Option<Box<dyn NodeRunner<Self::Context>>> {
        None
    }
}

impl Clone for ToolExecutorNode {
    fn clone(&self) -> Self {
        Self {
            registry: self.registry.clone(),
        }
    }
}

// ===== Test =====

#[tokio::test]
async fn test_file_saver_with_chat_context() {
    // Shared storage to verify file was saved
    let saved_files = Arc::new(Mutex::new(Vec::new()));

    // Create tool registry with file saver that includes chat context
    let chat_filename = "session_2024_02_28_conversation.yaml";
    let file_saver = FileSaverTool {
        chat_filename: chat_filename.to_string(),
        saved_files: saved_files.clone(),
    };

    let registry = ToolRegistry::new().register(file_saver);

    // Verify tool descriptor
    let descriptors = registry.descriptors();
    assert_eq!(descriptors.len(), 1);
    assert_eq!(descriptors[0].name, "file_saver");

    // Create initial context with chat filename
    let context = ChatContext {
        chat_filename: chat_filename.to_string(),
        tool_calls: None,
        saved_content: None,
    };

    // Create pipeline
    let first_node = SaveFileNode {
        executor: MockExecutor,
        registry: registry.clone(),
    };
    let pipeline = Pipeline::new(Box::new(NodeWrapper::new(first_node)));

    // Run pipeline
    let local_set = tokio::task::LocalSet::new();
    let final_ctx = local_set
        .run_until(async {
            let mut stream = run_pipeline(pipeline, context);
            let mut final_context: Option<ChatContext> = None;

            println!("\n=== File Saver Tool Test ===\n");
            println!("📁 Chat context: {}\n", chat_filename);

            while let Some(event) = stream.next().await {
                match event {
                    NodeEvent::Prompt(_id, prompt_event) => match prompt_event {
                        PromptExecutionEvent::Scheduled(name) => {
                            println!("📋 Prompt scheduled: {}", name);
                        }
                        PromptExecutionEvent::ToolCallsRequested(calls) => {
                            println!("🔧 Tool calls requested: {} tools", calls.len());
                            for call in &calls {
                                println!("   - {} (id: {})", call.name, call.id);
                            }
                        }
                        PromptExecutionEvent::ToolExecuting {
                            call_id,
                            tool_name,
                        } => {
                            println!("⚙️  Executing tool: {} ({})", tool_name, call_id);
                        }
                        PromptExecutionEvent::ToolCompleted { call_id, result } => {
                            println!("✅ Tool completed: {}", call_id);
                            if let Some(path) = result.output.get("saved_path") {
                                println!("   Saved to: {}", path);
                            }
                        }
                        PromptExecutionEvent::Error(e) => {
                            eprintln!("❌ Error: {:?}", e);
                        }
                        _ => {}
                    },
                    NodeEvent::Completed(ctx) => {
                        println!("🎯 Node completed");
                        final_context = Some(ctx);
                    }
                }
            }

            final_context.expect("Pipeline should complete")
        })
        .await;

    // Verify file was saved with chat metadata
    assert!(
        final_ctx.saved_content.is_some(),
        "File content should be saved in context"
    );

    let saved_content = final_ctx.saved_content.unwrap();

    // Verify the metadata header was added
    assert!(
        saved_content.contains(&format!("Created from chat: {}", chat_filename)),
        "Saved content should include chat metadata header"
    );

    // Verify the original content is present
    assert!(
        saved_content.contains("def hello():"),
        "Saved content should include the Python code"
    );
    assert!(
        saved_content.contains("print('Hello, World!')"),
        "Saved content should include the print statement"
    );

    // Verify in-memory storage
    let files = saved_files.lock().unwrap();
    assert_eq!(files.len(), 1, "Should have saved exactly one file");
    assert_eq!(files[0].0, "hello.py", "Filename should be hello.py");
    assert_eq!(
        files[0].1, saved_content,
        "Stored content should match context content"
    );

    println!("\n=== Saved File Content ===");
    println!("{}", saved_content);
    println!("\n=== Verification ===");
    println!("✅ Chat metadata header added");
    println!("✅ Original content preserved");
    println!("✅ File saved to in-memory storage");
}
