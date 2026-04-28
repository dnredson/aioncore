use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ToolDefinition {
    pub name: String,
    pub description: String,
    pub input_schema: Value,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ToolRequest {
    #[serde(default)]
    pub arguments: Value,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ToolResponse {
    pub tool_name: String,
    pub result: Option<ToolResult>,
    pub error: Option<ToolError>,
}

impl ToolResponse {
    pub fn success(tool_name: impl Into<String>, content: Value) -> Self {
        Self {
            tool_name: tool_name.into(),
            result: Some(ToolResult { content }),
            error: None,
        }
    }

    pub fn error(
        tool_name: impl Into<String>,
        code: impl Into<String>,
        message: impl Into<String>,
    ) -> Self {
        Self {
            tool_name: tool_name.into(),
            result: None,
            error: Some(ToolError {
                code: code.into(),
                message: message.into(),
            }),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ToolResult {
    pub content: Value,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolError {
    pub code: String,
    pub message: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn creates_success_response() {
        let response = ToolResponse::success("list_entities", json!({"items": []}));

        assert_eq!(response.tool_name, "list_entities");
        assert!(response.result.is_some());
        assert!(response.error.is_none());
    }

    #[test]
    fn creates_error_response() {
        let response =
            ToolResponse::error("get_entity", "missing_argument", "entity_id is required");

        assert_eq!(response.tool_name, "get_entity");
        assert!(response.result.is_none());
        assert_eq!(response.error.unwrap().code, "missing_argument");
    }
}
