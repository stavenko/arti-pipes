//! Integration test for a 3-node linear pipeline

use arti_pipes::{
    executor::PromptExecutor,
    llm_executors::GptOss,
    node::{Node, NodeEvent, NodeRunner, NodeWrapper},
    pipeline::{run_pipeline, Pipeline},
    prompt::{Prompt, PromptExecutionEvent},
};
use futures::{stream::LocalBoxStream, StreamExt};

// ===== Context =====

#[derive(Clone, Debug)]
struct PipelineContext {
    input: String,
    summary: Option<String>,
    expanded: Option<String>,
    conclusion: Option<String>,
}

// ===== Node 1: Summary Node =====

struct SummaryPrompt {
    text: String,
}

impl Prompt for SummaryPrompt {
    type Output = String;
    type Context = PipelineContext;

    fn name(&self) -> String {
        "SummaryPrompt".to_string()
    }

    fn serialize(&self) -> String {
        format!(
            "Summarize the following text in 1-2 sentences:\n\n{}",
            self.text
        )
    }

    fn update_context(&self, mut ctx: Self::Context, data: Self::Output) -> Self::Context {
        ctx.summary = Some(data);
        ctx
    }

    fn execute<E: PromptExecutor>(
        &self,
        executor: E,
    ) -> LocalBoxStream<'static, PromptExecutionEvent> {
        let prompt_text = self.serialize();
        let prompt_name = self.name();

        Box::pin(async_stream::stream! {
            yield PromptExecutionEvent::Scheduled(prompt_name);

            match executor.execute_raw(prompt_text).await {
                Ok(result) => {
                    let mut thinking = result.thinking_stream;
                    let mut content = result.content_stream;

                    // Stream thinking tokens
                    while let Some(token_result) = thinking.next().await {
                        match token_result {
                            Ok(token) => yield PromptExecutionEvent::ThinkingToken(token),
                            Err(e) => yield PromptExecutionEvent::Error(e),
                        }
                    }

                    // Stream content tokens
                    while let Some(token_result) = content.next().await {
                        match token_result {
                            Ok(token) => yield PromptExecutionEvent::ContentToken(token),
                            Err(e) => yield PromptExecutionEvent::Error(e),
                        }
                    }

                    // Wait for final output
                    match result.output.await {
                        Ok(Ok(output)) => {
                            yield PromptExecutionEvent::Completed(output.result);
                        }
                        Ok(Err(e)) => yield PromptExecutionEvent::Error(e),
                        Err(e) => yield PromptExecutionEvent::Error(
                            arti_pipes::error::ExecutionError::ModelExecution(e.to_string())
                        ),
                    }
                }
                Err(e) => yield PromptExecutionEvent::Error(e),
            }
        })
    }
}

struct SummaryNode {
    executor: GptOss,
}

impl Node for SummaryNode {
    type Prompt = SummaryPrompt;
    type Executor = GptOss;
    type Error = String;
    type Context = PipelineContext;

    fn prompt(&self, ctx: &Self::Context) -> Self::Prompt {
        SummaryPrompt {
            text: ctx.input.clone(),
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

            let updated_context = prompt.update_context(context, final_output);
            yield NodeEvent::Completed(updated_context);
        })
    }

    fn select_next_node(
        &self,
        _context: &Self::Context,
    ) -> Option<Box<dyn NodeRunner<Self::Context>>> {
        Some(Box::new(NodeWrapper::new(ExpandNode {
            executor: self.executor.clone(),
        })))
    }
}

impl Clone for SummaryNode {
    fn clone(&self) -> Self {
        Self {
            executor: self.executor.clone(),
        }
    }
}

// ===== Node 2: Expand Node =====

struct ExpandPrompt {
    summary: String,
}

impl Prompt for ExpandPrompt {
    type Output = String;
    type Context = PipelineContext;

    fn name(&self) -> String {
        "ExpandPrompt".to_string()
    }

    fn serialize(&self) -> String {
        format!(
            "Expand the following summary into a detailed explanation (3-4 sentences):\n\n{}",
            self.summary
        )
    }

    fn update_context(&self, mut ctx: Self::Context, data: Self::Output) -> Self::Context {
        ctx.expanded = Some(data);
        ctx
    }

    fn execute<E: PromptExecutor>(
        &self,
        executor: E,
    ) -> LocalBoxStream<'static, PromptExecutionEvent> {
        let prompt_text = self.serialize();
        let prompt_name = self.name();

        Box::pin(async_stream::stream! {
            yield PromptExecutionEvent::Scheduled(prompt_name);

            match executor.execute_raw(prompt_text).await {
                Ok(result) => {
                    let mut thinking = result.thinking_stream;
                    let mut content = result.content_stream;

                    while let Some(token_result) = thinking.next().await {
                        match token_result {
                            Ok(token) => yield PromptExecutionEvent::ThinkingToken(token),
                            Err(e) => yield PromptExecutionEvent::Error(e),
                        }
                    }

                    while let Some(token_result) = content.next().await {
                        match token_result {
                            Ok(token) => yield PromptExecutionEvent::ContentToken(token),
                            Err(e) => yield PromptExecutionEvent::Error(e),
                        }
                    }

                    match result.output.await {
                        Ok(Ok(output)) => {
                            yield PromptExecutionEvent::Completed(output.result);
                        }
                        Ok(Err(e)) => yield PromptExecutionEvent::Error(e),
                        Err(e) => yield PromptExecutionEvent::Error(
                            arti_pipes::error::ExecutionError::ModelExecution(e.to_string())
                        ),
                    }
                }
                Err(e) => yield PromptExecutionEvent::Error(e),
            }
        })
    }
}

struct ExpandNode {
    executor: GptOss,
}

impl Node for ExpandNode {
    type Prompt = ExpandPrompt;
    type Executor = GptOss;
    type Error = String;
    type Context = PipelineContext;

    fn prompt(&self, ctx: &Self::Context) -> Self::Prompt {
        ExpandPrompt {
            summary: ctx.summary.clone().unwrap_or_default(),
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

            let updated_context = prompt.update_context(context, final_output);
            yield NodeEvent::Completed(updated_context);
        })
    }

    fn select_next_node(
        &self,
        _context: &Self::Context,
    ) -> Option<Box<dyn NodeRunner<Self::Context>>> {
        Some(Box::new(NodeWrapper::new(ConclusionNode {
            executor: self.executor.clone(),
        })))
    }
}

impl Clone for ExpandNode {
    fn clone(&self) -> Self {
        Self {
            executor: self.executor.clone(),
        }
    }
}

// ===== Node 3: Conclusion Node =====

struct ConclusionPrompt {
    expanded: String,
}

impl Prompt for ConclusionPrompt {
    type Output = String;
    type Context = PipelineContext;

    fn name(&self) -> String {
        "ConclusionPrompt".to_string()
    }

    fn serialize(&self) -> String {
        format!(
            "Based on the following text, provide a brief conclusion (1 sentence):\n\n{}",
            self.expanded
        )
    }

    fn update_context(&self, mut ctx: Self::Context, data: Self::Output) -> Self::Context {
        ctx.conclusion = Some(data);
        ctx
    }

    fn execute<E: PromptExecutor>(
        &self,
        executor: E,
    ) -> LocalBoxStream<'static, PromptExecutionEvent> {
        let prompt_text = self.serialize();
        let prompt_name = self.name();

        Box::pin(async_stream::stream! {
            yield PromptExecutionEvent::Scheduled(prompt_name);

            match executor.execute_raw(prompt_text).await {
                Ok(result) => {
                    let mut thinking = result.thinking_stream;
                    let mut content = result.content_stream;

                    while let Some(token_result) = thinking.next().await {
                        match token_result {
                            Ok(token) => yield PromptExecutionEvent::ThinkingToken(token),
                            Err(e) => yield PromptExecutionEvent::Error(e),
                        }
                    }

                    while let Some(token_result) = content.next().await {
                        match token_result {
                            Ok(token) => yield PromptExecutionEvent::ContentToken(token),
                            Err(e) => yield PromptExecutionEvent::Error(e),
                        }
                    }

                    match result.output.await {
                        Ok(Ok(output)) => {
                            yield PromptExecutionEvent::Completed(output.result);
                        }
                        Ok(Err(e)) => yield PromptExecutionEvent::Error(e),
                        Err(e) => yield PromptExecutionEvent::Error(
                            arti_pipes::error::ExecutionError::ModelExecution(e.to_string())
                        ),
                    }
                }
                Err(e) => yield PromptExecutionEvent::Error(e),
            }
        })
    }
}

struct ConclusionNode {
    executor: GptOss,
}

impl Node for ConclusionNode {
    type Prompt = ConclusionPrompt;
    type Executor = GptOss;
    type Error = String;
    type Context = PipelineContext;

    fn prompt(&self, ctx: &Self::Context) -> Self::Prompt {
        ConclusionPrompt {
            expanded: ctx.expanded.clone().unwrap_or_default(),
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

            let updated_context = prompt.update_context(context, final_output);
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

impl Clone for ConclusionNode {
    fn clone(&self) -> Self {
        Self {
            executor: self.executor.clone(),
        }
    }
}

// ===== Test =====

#[tokio::test]
async fn test_three_node_pipeline() {
    // Create executor from config
    let executor = GptOss::builder()
        .api_base("http://192.168.1.17:8080/v1")
        .model("gpt-oss-120b")
        .reasoning_effort("medium")
        .build();

    // Create initial context
    let context = PipelineContext {
        input: "Rust is a systems programming language that focuses on safety, speed, and concurrency. It achieves memory safety without using garbage collection.".to_string(),
        summary: None,
        expanded: None,
        conclusion: None,
    };

    // Create pipeline
    let first_node = SummaryNode {
        executor: executor.clone(),
    };
    let pipeline = Pipeline::new(Box::new(NodeWrapper::new(first_node)));

    // Run pipeline
    let local_set = tokio::task::LocalSet::new();
    let ctx = local_set
        .run_until(async {
            let mut stream = run_pipeline(pipeline, context);
            let mut final_context: Option<PipelineContext> = None;

            println!("\n=== Pipeline Execution ===\n");

            while let Some(event) = stream.next().await {
                match event {
                    NodeEvent::Prompt(_id, prompt_event) => match prompt_event {
                        PromptExecutionEvent::Scheduled(name) => {
                            println!("📋 Prompt scheduled: {}", name);
                        }
                        PromptExecutionEvent::ThinkingToken(token) => {
                            print!("🧠 {}", token.content);
                        }
                        PromptExecutionEvent::ContentToken(token) => {
                            print!("{}", token.content);
                        }
                        PromptExecutionEvent::Completed(_output) => {
                            println!("\n✅ Prompt completed");
                        }
                        PromptExecutionEvent::Error(e) => {
                            eprintln!("❌ Error: {:?}", e);
                        }
                    },
                    NodeEvent::Completed(ctx) => {
                        println!("\n🎯 Node completed\n");
                        final_context = Some(ctx);
                    }
                }
            }

            // Verify all nodes executed
            final_context.expect("Pipeline should complete")
        })
        .await;
    assert!(ctx.summary.is_some(), "Summary should be generated");
    assert!(ctx.expanded.is_some(), "Expansion should be generated");
    assert!(ctx.conclusion.is_some(), "Conclusion should be generated");

    println!("\n=== Final Context ===");
    println!("📝 Summary: {}", ctx.summary.unwrap());
    println!("📖 Expanded: {}", ctx.expanded.unwrap());
    println!("🎓 Conclusion: {}", ctx.conclusion.unwrap());
}
