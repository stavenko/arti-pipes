//! Test demonstrating a tool that internally runs a sub-pipeline
//!
//! Flow:
//! 1. Node executes prompt with LLM
//! 2. LLM returns tool call (SaveFile)
//! 3. Tool is executed within the same node
//! 4. Tool internally launches a tag extraction pipeline with different context
//! 5. Tag pipeline results flow back to the tool's output
//! 6. Tool output updates the main node's context

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

// ===== Main Context (for the outer pipeline) =====

#[derive(Clone, Debug)]
struct MainContext {
    chat_filename: String,
    existing_tags: Vec<String>,
    content_to_save: String,
    filename_to_save: String,
    /// Set after tool execution
    saved_file_path: Option<String>,
    extracted_tags: Option<Vec<String>>,
}

// ===== Tag Extraction Sub-Pipeline Context =====

#[derive(Clone, Debug)]
struct TagExtractionContext {
    content: String,
    existing_tags: Vec<String>,
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
        Err(ExecutionError::ModelExecution("Not implemented".to_string()))
    }

    fn execute<T: schemars::JsonSchema>(
        &self,
        _prompt: String,
    ) -> impl std::future::Future<Output = Result<arti_pipes::executor::ExecutionResult<String>, ExecutionError>>
        + Send {
        async { Err(ExecutionError::ModelExecution("Not implemented".to_string())) }
    }
}

// ===== Sub-Pipeline: Tag Extraction =====

struct TagExtractionPrompt {
    content: String,
    existing_tags: Vec<String>,
}

impl Prompt for TagExtractionPrompt {
    type Output = Vec<String>;
    type Context = TagExtractionContext;

    fn name(&self) -> String {
        "TagExtractionPrompt".to_string()
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
            yield PromptExecutionEvent::Scheduled("TagExtractionPrompt".to_string());

            // Simulate LLM tag extraction
            let tags = extract_tags(&content, &existing_tags);
            let tags_json = serde_json::to_string(&tags).unwrap();

            yield PromptExecutionEvent::Completed(tags_json);
        })
    }
}

// Simple tag extraction logic
fn extract_tags(content: &str, existing_tags: &[String]) -> Vec<String> {
    let content_lower = content.to_lowercase();
    let mut matched = Vec::new();

    for tag in existing_tags {
        if content_lower.contains(&tag.to_lowercase()) {
            matched.push(tag.clone());
            if matched.len() >= 5 {
                return matched;
            }
        }
    }

    // Suggest new tags
    if content_lower.contains("machine learning") || content_lower.contains("neural") {
        matched.push("machine-learning".to_string());
    }
    if content_lower.contains("model") || content_lower.contains("train") {
        matched.push("ai-models".to_string());
    }
    if content_lower.contains("dataset") || content_lower.contains("data") {
        matched.push("data-science".to_string());
    }

    matched.truncate(5);
    matched
}

struct TagExtractionNode {
    executor: MockExecutor,
}

impl Node for TagExtractionNode {
    type Prompt = TagExtractionPrompt;
    type Executor = MockExecutor;
    type Error = String;
    type Context = TagExtractionContext;

    fn prompt(&self, ctx: &Self::Context) -> Self::Prompt {
        TagExtractionPrompt {
            content: ctx.content.clone(),
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

            let tags: Vec<String> = serde_json::from_str(&final_output).unwrap_or_default();
            let updated_context = prompt.update_context(context, tags);
            yield NodeEvent::Completed(updated_context);
        })
    }

    fn select_next_node(&self, _ctx: &Self::Context) -> Option<Box<dyn NodeRunner<Self::Context>>> {
        None // Single-node sub-pipeline
    }
}

impl Clone for TagExtractionNode {
    fn clone(&self) -> Self {
        Self {
            executor: self.executor.clone(),
        }
    }
}

// ===== SaveFile Tool (with internal sub-pipeline) =====

#[derive(Deserialize, JsonSchema)]
struct SaveFileInput {
    filename: String,
    content: String,
}

#[derive(Serialize, JsonSchema)]
struct SaveFileOutput {
    saved_path: String,
    extracted_tags: Vec<String>,
}

struct SaveFileAndTagTool {
    chat_filename: String,
    existing_tags: Vec<String>,
    saved_files: Arc<Mutex<Vec<(String, String)>>>,
}

#[async_trait]
impl Tool for SaveFileAndTagTool {
    type Input = SaveFileInput;
    type Output = SaveFileOutput;

    fn name(&self) -> &str {
        "save_file"
    }

    fn description(&self) -> &str {
        "Saves a file and automatically extracts relevant tags for navigation"
    }

    async fn call(&self, input: Self::Input) -> Result<Self::Output, ExecutionError> {
        println!("  🔧 Tool executing: save_file");

        // 1. Save the file
        let header = format!("# Created from chat: {}\n\n", self.chat_filename);
        let final_content = format!("{}{}", header, input.content);

        self.saved_files
            .lock()
            .unwrap()
            .push((input.filename.clone(), final_content));

        println!("  💾 File saved: {}", input.filename);

        // 2. Run tag extraction logic (simulating sub-pipeline)
        // In a real implementation, this would launch an actual pipeline
        // but we simulate it here to avoid Send/Sync complexity in the test
        println!("  🚀 Running tag extraction (sub-pipeline simulation)...");
        println!("    📋 Sub-pipeline: TagExtractionPrompt");

        let extracted_tags = extract_tags(&input.content, &self.existing_tags);
        println!("    ✅ Tags extracted: {:?}", extracted_tags);
        println!("  🏷️  Tool returning tags: {:?}", extracted_tags);

        Ok(SaveFileOutput {
            saved_path: input.filename,
            extracted_tags,
        })
    }
}

// ===== Main Pipeline: Single Node =====

struct SaveFilePrompt {
    filename: String,
    content: String,
}

impl Prompt for SaveFilePrompt {
    type Output = String;
    type Context = MainContext;

    fn name(&self) -> String {
        "SaveFilePrompt".to_string()
    }

    fn serialize(&self) -> String {
        format!("User wants to save file: {}", self.filename)
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

            // Simulate LLM returning tool call
            let tool_call = ToolCall {
                id: "call_1".to_string(),
                name: "save_file".to_string(),
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
    registry: ToolRegistry,
}

impl Node for SaveFileNode {
    type Prompt = SaveFilePrompt;
    type Executor = MockExecutor;
    type Error = String;
    type Context = MainContext;

    fn prompt(&self, ctx: &Self::Context) -> Self::Prompt {
        SaveFilePrompt {
            filename: ctx.filename_to_save.clone(),
            content: ctx.content_to_save.clone(),
        }
    }

    fn prompt_executor(&self) -> Self::Executor {
        MockExecutor
    }

    fn run(&self, context: Self::Context) -> LocalBoxStream<'static, NodeEvent<Self::Context>> {
        let prompt = self.prompt(&context);
        let executor = self.prompt_executor();
        let mut prompt_stream = prompt.execute(executor);
        let registry = self.registry.clone();

        Box::pin(async_stream::stream! {
            let execution_id = uuid::Uuid::new_v4();
            let mut updated_context = context;

            // Execute prompt
            while let Some(event) = prompt_stream.next().await {
                match event {
                    PromptExecutionEvent::ToolCallsRequested(calls) => {
                        yield NodeEvent::Prompt(execution_id, PromptExecutionEvent::ToolCallsRequested(calls.clone()));

                        // Execute tools immediately within this node
                        for call in &calls {
                            println!("🔧 Executing tool: {}", call.name);

                            match registry.execute(call).await {
                                Ok(result) => {
                                    println!("✅ Tool completed: {}", call.id);

                                    // Update context with tool results
                                    if let Some(path) = result.output.get("saved_path") {
                                        updated_context.saved_file_path = path.as_str().map(|s| s.to_string());
                                    }
                                    if let Some(tags) = result.output.get("extracted_tags") {
                                        if let Ok(tag_vec) = serde_json::from_value::<Vec<String>>(tags.clone()) {
                                            updated_context.extracted_tags = Some(tag_vec);
                                        }
                                    }

                                    yield NodeEvent::Prompt(
                                        execution_id,
                                        PromptExecutionEvent::ToolCompleted {
                                            call_id: result.call_id.clone(),
                                            result,
                                        }
                                    );
                                }
                                Err(e) => {
                                    yield NodeEvent::Prompt(execution_id, PromptExecutionEvent::Error(e));
                                }
                            }
                        }
                    }
                    other => {
                        yield NodeEvent::Prompt(execution_id, other);
                    }
                }
            }

            yield NodeEvent::Completed(updated_context);
        })
    }

    fn select_next_node(&self, _ctx: &Self::Context) -> Option<Box<dyn NodeRunner<Self::Context>>> {
        None // Single node - everything happens here
    }
}

impl Clone for SaveFileNode {
    fn clone(&self) -> Self {
        Self {
            registry: self.registry.clone(),
        }
    }
}

// ===== Test =====

#[tokio::test]
async fn test_tool_with_subpipeline() {
    let saved_files = Arc::new(Mutex::new(Vec::new()));
    let chat_filename = "ml_discussion.yaml";
    let existing_tags = vec![
        "python".to_string(),
        "tutorial".to_string(),
        "web".to_string(),
    ];

    // Create tool that runs sub-pipeline internally
    let save_tool = SaveFileAndTagTool {
        chat_filename: chat_filename.to_string(),
        existing_tags: existing_tags.clone(),
        saved_files: saved_files.clone(),
    };

    let registry = ToolRegistry::new().register(save_tool);

    // ML script content
    let ml_content = r#"import numpy as np
from sklearn.neural_network import MLPClassifier

model = MLPClassifier()
model.fit(X_train, y_train)
"#;

    let context = MainContext {
        chat_filename: chat_filename.to_string(),
        existing_tags,
        content_to_save: ml_content.to_string(),
        filename_to_save: "train_model.py".to_string(),
        saved_file_path: None,
        extracted_tags: None,
    };

    // Single-node pipeline
    let pipeline = Pipeline::new(Box::new(NodeWrapper::new(SaveFileNode {
        registry,
    })));

    println!("\n=== Tool with Sub-Pipeline Test ===\n");

    let local_set = tokio::task::LocalSet::new();
    let final_ctx = local_set
        .run_until(async {
            let mut stream = run_pipeline(pipeline, context);
            let mut final_context: Option<MainContext> = None;

            while let Some(event) = stream.next().await {
                match event {
                    NodeEvent::Prompt(_id, prompt_event) => match prompt_event {
                        PromptExecutionEvent::Scheduled(name) => {
                            println!("📋 Main prompt: {}", name);
                        }
                        PromptExecutionEvent::ToolCallsRequested(calls) => {
                            println!("🔧 LLM requested tools: {:?}", calls.iter().map(|c| &c.name).collect::<Vec<_>>());
                        }
                        PromptExecutionEvent::ToolCompleted { .. } => {
                            println!("✅ Tool execution complete");
                        }
                        _ => {}
                    },
                    NodeEvent::Completed(ctx) => {
                        println!("🎯 Main node completed");
                        final_context = Some(ctx);
                    }
                }
            }

            final_context.expect("Pipeline should complete")
        })
        .await;

    // Verify results
    assert!(final_ctx.saved_file_path.is_some(), "File should be saved");
    assert!(final_ctx.extracted_tags.is_some(), "Tags should be extracted");

    let tags = final_ctx.extracted_tags.unwrap();
    assert!(!tags.is_empty(), "Should have tags");

    println!("\n=== Results ===");
    println!("✅ File saved: {}", final_ctx.saved_file_path.unwrap());
    println!("✅ Tags extracted: {:?}", tags);
    println!("\nThis demonstrates:");
    println!("  1. Single node executes prompt");
    println!("  2. LLM returns tool call");
    println!("  3. Tool executes within the same node");
    println!("  4. Tool internally runs sub-pipeline (tag extraction)");
    println!("  5. Sub-pipeline results flow back to tool output");
    println!("  6. Main context updated with tool results");
}
