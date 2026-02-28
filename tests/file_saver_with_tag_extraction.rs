//! Integration test for file saver with automatic tag extraction pipeline

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
struct TaggingContext {
    /// Path to chat YAML file
    chat_filename: String,
    /// User's existing tags for navigation
    existing_tags: Vec<String>,
    /// Content to be saved
    content_to_save: Option<String>,
    /// Filename to save to
    filename_to_save: Option<String>,
    /// Pending tool calls
    tool_calls: Option<Vec<ToolCall>>,
    /// Saved file content (with metadata)
    saved_content: Option<String>,
    /// Extracted tags for the saved file
    extracted_tags: Option<Vec<String>>,
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

struct FileSaverTool {
    chat_filename: String,
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
        let header = format!("# Created from chat: {}\n\n", self.chat_filename);
        let final_content = format!("{}{}", header, input.content);

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

// ===== Node 1: Save File Request Node =====

struct SaveFilePrompt {
    filename: String,
    content: String,
}

impl Prompt for SaveFilePrompt {
    type Output = String;
    type Context = TaggingContext;

    fn name(&self) -> String {
        "SaveFilePrompt".to_string()
    }

    fn serialize(&self) -> String {
        format!("Saving file: {}", self.filename)
    }

    fn update_context(&self, ctx: Self::Context, _data: Self::Output) -> Self::Context {
        ctx
    }

    fn execute<E: PromptExecutor>(
        &self,
        _executor: E,
    ) -> LocalBoxStream<'static, PromptExecutionEvent> {
        let filename = self.filename.clone();
        let content = self.content.clone();

        Box::pin(async_stream::stream! {
            yield PromptExecutionEvent::Scheduled("SaveFilePrompt".to_string());

            let tool_call = ToolCall {
                id: "call_save_file".to_string(),
                name: "file_saver".to_string(),
                arguments: serde_json::json!({
                    "filename": filename,
                    "content": content
                }),
            };

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
    type Context = TaggingContext;

    fn prompt(&self, ctx: &Self::Context) -> Self::Prompt {
        SaveFilePrompt {
            filename: ctx.filename_to_save.clone().unwrap_or_default(),
            content: ctx.content_to_save.clone().unwrap_or_default(),
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

// ===== Node 2: Tool Executor Node =====

struct ToolExecutorPrompt;

impl Prompt for ToolExecutorPrompt {
    type Output = String;
    type Context = TaggingContext;

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
    type Context = TaggingContext;

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

                            if let Some(final_content) = result.output.get("final_content") {
                                if let Some(content_str) = final_content.as_str() {
                                    updated_context.saved_content = Some(content_str.to_string());
                                }
                            }
                        }
                        Err(e) => {
                            yield NodeEvent::Prompt(execution_id, PromptExecutionEvent::Error(e));
                        }
                    }
                }
            }

            yield NodeEvent::Completed(updated_context);
        })
    }

    fn select_next_node(
        &self,
        context: &Self::Context,
    ) -> Option<Box<dyn NodeRunner<Self::Context>>> {
        // After saving file, proceed to tag extraction
        if context.saved_content.is_some() && context.extracted_tags.is_none() {
            Some(Box::new(NodeWrapper::new(TagExtractorNode {
                executor: MockExecutor,
            })))
        } else {
            None
        }
    }
}

impl Clone for ToolExecutorNode {
    fn clone(&self) -> Self {
        Self {
            registry: self.registry.clone(),
        }
    }
}

// ===== Node 3: Tag Extractor Node =====

struct TagExtractorPrompt {
    content: String,
    existing_tags: Vec<String>,
}

impl Prompt for TagExtractorPrompt {
    type Output = Vec<String>;
    type Context = TaggingContext;

    fn name(&self) -> String {
        "TagExtractorPrompt".to_string()
    }

    fn serialize(&self) -> String {
        format!(
            "This is content of the file user wants to save. \
             Here are existing user tags: [{}]. \
             Your job is to find most appropriate 5 tags for this content. \
             Tags are supposed to be used for navigation, so select tags which reflect meaning of this text. \
             If there are no sufficient tags to determine this document, suggest 3 new tags for this document and use them in output.\n\n\
             Content:\n{}",
            self.existing_tags.join(", "),
            self.content
        )
    }

    fn update_context(&self, mut ctx: Self::Context, tags: Self::Output) -> Self::Context {
        ctx.extracted_tags = Some(tags);
        ctx
    }

    fn execute<E: PromptExecutor>(
        &self,
        _executor: E,
    ) -> LocalBoxStream<'static, PromptExecutionEvent> {
        let content = self.content.clone();
        let existing_tags = self.existing_tags.clone();

        Box::pin(async_stream::stream! {
            yield PromptExecutionEvent::Scheduled("TagExtractorPrompt".to_string());

            // Simulate LLM analyzing content and selecting tags
            // For this test, we'll do simple keyword matching
            let suggested_tags = extract_tags_from_content(&content, &existing_tags);

            let tags_json = serde_json::to_string(&suggested_tags).unwrap();
            yield PromptExecutionEvent::Completed(tags_json);
        })
    }
}

// Simple mock tag extraction based on keyword matching
fn extract_tags_from_content(content: &str, existing_tags: &[String]) -> Vec<String> {
    let content_lower = content.to_lowercase();
    let mut matched_tags = Vec::new();

    // Try to match existing tags
    for tag in existing_tags {
        if content_lower.contains(&tag.to_lowercase()) {
            matched_tags.push(tag.clone());
            if matched_tags.len() >= 5 {
                break;
            }
        }
    }

    // If not enough tags found, suggest new ones based on content
    if matched_tags.len() < 3 {
        let mut new_tags = Vec::new();

        if content_lower.contains("machine learning") || content_lower.contains("ml") {
            new_tags.push("machine-learning".to_string());
        }
        if content_lower.contains("neural") || content_lower.contains("model") {
            new_tags.push("ai-models".to_string());
        }
        if content_lower.contains("train") || content_lower.contains("dataset") {
            new_tags.push("data-science".to_string());
        }

        // Add new tags up to 3 total
        for tag in new_tags {
            if !matched_tags.contains(&tag) {
                matched_tags.push(tag);
                if matched_tags.len() >= 5 {
                    break;
                }
            }
        }
    }

    matched_tags
}

struct TagExtractorNode {
    executor: MockExecutor,
}

impl Node for TagExtractorNode {
    type Prompt = TagExtractorPrompt;
    type Executor = MockExecutor;
    type Error = String;
    type Context = TaggingContext;

    fn prompt(&self, ctx: &Self::Context) -> Self::Prompt {
        TagExtractorPrompt {
            content: ctx.saved_content.clone().unwrap_or_default(),
            existing_tags: ctx.existing_tags.clone(),
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
            let mut final_output = String::new();
            let execution_id = uuid::Uuid::new_v4();

            while let Some(event) = prompt_stream.next().await {
                match &event {
                    PromptExecutionEvent::Completed(output) => {
                        final_output = output.clone();
                    }
                    _ => {}
                }
                yield NodeEvent::Prompt(execution_id, event);
            }

            // Parse tags from JSON output
            let tags: Vec<String> = serde_json::from_str(&final_output).unwrap_or_default();
            let updated_context = prompt.update_context(context, tags);
            yield NodeEvent::Completed(updated_context);
        })
    }

    fn select_next_node(
        &self,
        _context: &Self::Context,
    ) -> Option<Box<dyn NodeRunner<Self::Context>>> {
        None // End of pipeline
    }
}

impl Clone for TagExtractorNode {
    fn clone(&self) -> Self {
        Self {
            executor: self.executor.clone(),
        }
    }
}

// ===== Test =====

#[tokio::test]
async fn test_file_saver_with_tag_extraction() {
    let saved_files = Arc::new(Mutex::new(Vec::new()));
    let chat_filename = "ml_discussion_2024_02_28.yaml";

    // Create tool registry
    let file_saver = FileSaverTool {
        chat_filename: chat_filename.to_string(),
        saved_files: saved_files.clone(),
    };
    let registry = ToolRegistry::new().register(file_saver);

    // Machine learning Python script content
    let ml_script = r#"import numpy as np
from sklearn.neural_network import MLPClassifier

# Load training dataset
X_train = np.array([[0, 0], [0, 1], [1, 0], [1, 1]])
y_train = np.array([0, 1, 1, 0])

# Create and train neural network model
model = MLPClassifier(hidden_layer_sizes=(10,), max_iter=1000)
model.fit(X_train, y_train)

print("Model trained successfully!")
"#;

    // Create context with existing user tags
    let context = TaggingContext {
        chat_filename: chat_filename.to_string(),
        existing_tags: vec![
            "python".to_string(),
            "tutorial".to_string(),
            "web".to_string(),
            "database".to_string(),
            "api".to_string(),
            "testing".to_string(),
            "deployment".to_string(),
        ],
        content_to_save: Some(ml_script.to_string()),
        filename_to_save: Some("xor_classifier.py".to_string()),
        tool_calls: None,
        saved_content: None,
        extracted_tags: None,
    };

    // Create pipeline starting with SaveFileNode
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
            let mut final_context: Option<TaggingContext> = None;

            println!("\n=== File Saver + Tag Extraction Pipeline ===\n");
            println!("📁 Chat: {}", chat_filename);
            println!("🏷️  Existing tags: python, tutorial, web, database, api, testing, deployment\n");

            while let Some(event) = stream.next().await {
                match event {
                    NodeEvent::Prompt(_id, prompt_event) => match prompt_event {
                        PromptExecutionEvent::Scheduled(name) => {
                            println!("📋 Prompt scheduled: {}", name);
                        }
                        PromptExecutionEvent::ToolCallsRequested(calls) => {
                            println!("🔧 Tool calls requested:");
                            for call in &calls {
                                println!("   - {} (id: {})", call.name, call.id);
                            }
                        }
                        PromptExecutionEvent::ToolExecuting { tool_name, .. } => {
                            println!("⚙️  Executing: {}", tool_name);
                        }
                        PromptExecutionEvent::ToolCompleted { result, .. } => {
                            if let Some(path) = result.output.get("saved_path") {
                                println!("✅ File saved: {}", path);
                            }
                        }
                        PromptExecutionEvent::Completed(output) => {
                            if output.starts_with('[') {
                                // Tag extraction output
                                println!("✅ Tags extracted: {}", output);
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

    // Verify file was saved
    assert!(
        final_ctx.saved_content.is_some(),
        "File should be saved"
    );

    let saved_content = final_ctx.saved_content.unwrap();
    assert!(
        saved_content.contains(&format!("Created from chat: {}", chat_filename)),
        "Should have chat metadata"
    );
    assert!(
        saved_content.contains("MLPClassifier"),
        "Should contain original ML code"
    );

    // Verify tags were extracted
    assert!(
        final_ctx.extracted_tags.is_some(),
        "Tags should be extracted"
    );

    let tags = final_ctx.extracted_tags.unwrap();
    assert!(!tags.is_empty(), "Should have at least one tag");

    // Verify appropriate tags (either existing or new)
    println!("\n=== Verification ===");
    println!("✅ File saved with chat metadata");
    println!("✅ Tags extracted: {:?}", tags);

    // Check that we got relevant tags
    let has_python = tags.iter().any(|t| t == "python");
    let has_ml_related = tags.iter().any(|t| {
        t.contains("machine") || t.contains("ai") || t.contains("data")
    });

    assert!(
        has_python || has_ml_related,
        "Tags should be relevant to the content (Python or ML-related)"
    );

    println!("✅ Tags are relevant to content");
    println!("\n📊 Final extracted tags: {}", tags.join(", "));
}
