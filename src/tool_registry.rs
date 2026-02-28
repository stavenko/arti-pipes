//! Tool registry for managing collections of tools

use crate::error::ExecutionError;
use crate::tool::{ToolCall, ToolDescriptor, ToolExecutor, ToolResult};
use std::collections::HashMap;
use std::sync::Arc;

/// Registry of available tools for a prompt
pub struct ToolRegistry {
    tools: HashMap<String, Arc<dyn ToolExecutor>>,
}

impl ToolRegistry {
    /// Create empty registry
    pub fn new() -> Self {
        Self {
            tools: HashMap::new(),
        }
    }

    /// Add a tool to the registry
    pub fn register<T: ToolExecutor + 'static>(mut self, tool: T) -> Self {
        let name = tool.descriptor().name.clone();
        self.tools.insert(name, Arc::new(tool));
        self
    }

    /// Get all tool descriptors for API calls
    pub fn descriptors(&self) -> Vec<ToolDescriptor> {
        self.tools.values().map(|t| t.descriptor()).collect()
    }

    /// Execute a single tool call
    pub async fn execute(&self, call: &ToolCall) -> Result<ToolResult, ExecutionError> {
        let tool = self
            .tools
            .get(&call.name)
            .ok_or_else(|| ExecutionError::ToolNotFound(call.name.clone()))?;

        let output = tool.execute_json(call.arguments.clone()).await?;

        Ok(ToolResult {
            call_id: call.id.clone(),
            output,
        })
    }

    /// Execute multiple tool calls in parallel
    pub async fn execute_all(
        &self,
        calls: Vec<ToolCall>,
    ) -> Vec<Result<ToolResult, ExecutionError>> {
        use futures::future::join_all;

        let futures = calls.iter().map(|call| self.execute(call));
        join_all(futures).await
    }
}

impl Default for ToolRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl Clone for ToolRegistry {
    fn clone(&self) -> Self {
        Self {
            tools: self.tools.clone(),
        }
    }
}
