use crate::{
    SmartSentinelSkippedItem, SmartSentinelValidationIssue, SmartSentinelValidationReport,
};
use aion_storage::StorageError;
use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde::Serialize;

#[derive(Debug)]
pub(crate) struct ApiError {
    pub(crate) status: StatusCode,
    pub(crate) message: String,
    pub(crate) validation_errors: Vec<SmartSentinelValidationIssue>,
    pub(crate) validation_warnings: Vec<SmartSentinelValidationIssue>,
    pub(crate) skipped_items: Vec<SmartSentinelSkippedItem>,
}

impl ApiError {
    pub(crate) fn bad_request(message: impl Into<String>) -> Self {
        Self::new(StatusCode::BAD_REQUEST, message)
    }

    pub(crate) fn unauthorized(message: impl Into<String>) -> Self {
        Self::new(StatusCode::UNAUTHORIZED, message)
    }

    pub(crate) fn forbidden(message: impl Into<String>) -> Self {
        Self::new(StatusCode::FORBIDDEN, message)
    }

    pub(crate) fn not_found() -> Self {
        Self::new(StatusCode::NOT_FOUND, "record was not found")
    }

    pub(crate) fn smartsentinel_validation(
        message: impl Into<String>,
        report: SmartSentinelValidationReport,
    ) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            message: message.into(),
            validation_errors: report.errors,
            validation_warnings: report.warnings,
            skipped_items: report.skipped_items,
        }
    }

    fn new(status: StatusCode, message: impl Into<String>) -> Self {
        Self {
            status,
            message: message.into(),
            validation_errors: Vec::new(),
            validation_warnings: Vec::new(),
            skipped_items: Vec::new(),
        }
    }
}

impl From<StorageError> for ApiError {
    fn from(value: StorageError) -> Self {
        match value {
            StorageError::NotFound => Self::not_found(),
            StorageError::Conflict => Self::new(StatusCode::CONFLICT, value.to_string()),
            StorageError::ConflictWithMessage(message) => Self::new(StatusCode::CONFLICT, message),
            StorageError::InvalidInput(message) => Self::bad_request(message),
            StorageError::Backend(message) => Self::new(StatusCode::INTERNAL_SERVER_ERROR, message),
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (
            self.status,
            Json(ErrorResponse {
                error: self.message,
                validation_errors: self.validation_errors,
                validation_warnings: self.validation_warnings,
                skipped_items: self.skipped_items,
            }),
        )
            .into_response()
    }
}

#[derive(Debug, Serialize)]
struct ErrorResponse {
    error: String,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    validation_errors: Vec<SmartSentinelValidationIssue>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    validation_warnings: Vec<SmartSentinelValidationIssue>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    skipped_items: Vec<SmartSentinelSkippedItem>,
}
