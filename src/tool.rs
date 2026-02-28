//! Tool system for LLM function calling
//!
//! Provides traits and types for defining and executing tools that LLMs can invoke
//! during prompt execution.

use async_trait::async_trait;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Tool trait - defines a callable tool with type-safe inputs/outputs
#[async_trait]
pub trait Tool: Send + Sync {
    /// Input type for this tool (must be deserializable from JSON)
    type Input: for<'de> Deserialize<'de> + Send;

    /// Output type for this tool (must be serializable to JSON)
    type Output: Serialize + Send;

    /// Unique tool name (used in LLM API and tool calls)
    fn name(&self) -> &str;

    /// Human-readable description for LLM
    fn description(&self) -> &str;

    /// Execute the tool with typed input, return typed output
    async fn call(&self, input: Self::Input) -> Result<Self::Output, crate::error::ExecutionError>;
}

/// Type-erased tool executor - handles JSON serialization boundary
#[async_trait]
pub trait ToolExecutor: Send + Sync {
    /// Get tool descriptor for API integration
    fn descriptor(&self) -> ToolDescriptor;

    /// Execute tool with JSON input, return JSON output
    async fn execute_json(&self, input: Value) -> Result<Value, crate::error::ExecutionError>;
}

/// Auto-implement ToolExecutor for any Tool
#[async_trait]
impl<T> ToolExecutor for T
where
    T: Tool,
    T::Input: JsonSchema,
    T::Output: JsonSchema,
{
    fn descriptor(&self) -> ToolDescriptor {
        ToolDescriptor {
            name: self.name().to_string(),
            description: self.description().to_string(),
            input_schema: schemars::schema_for!(T::Input),
            output_schema: schemars::schema_for!(T::Output),
        }
    }

    async fn execute_json(&self, input: Value) -> Result<Value, crate::error::ExecutionError> {
        let typed_input: T::Input = serde_json::from_value(input)
            .map_err(|e| crate::error::ExecutionError::ToolInputParse(e.to_string()))?;

        let output = self.call(typed_input).await?;

        serde_json::to_value(output)
            .map_err(|e| crate::error::ExecutionError::ToolOutputSerialize(e.to_string()))
    }
}

/// Tool descriptor for API integration (OpenAI format)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolDescriptor {
    pub name: String,
    pub description: String,
    pub input_schema: schemars::Schema,
    pub output_schema: schemars::Schema,
}

/// Tool call from LLM response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolCall {
    pub id: String,
    pub name: String,
    pub arguments: Value,
}

/// Tool execution result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolResult {
    pub call_id: String,
    pub output: Value,
}
