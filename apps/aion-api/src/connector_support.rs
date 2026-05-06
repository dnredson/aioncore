use crate::{error::ApiError, record_event, AppState, EventDraft};
use aion_event::{Event, EventSeverity};
use aion_storage::IngestionConnector;
use chrono::Utc;
use serde_json::{json, Value};
use uuid::Uuid;

pub(crate) fn ensure_connector_secret_exists(
    state: &AppState,
    secret_id: Option<Uuid>,
) -> Result<(), ApiError> {
    let Some(secret_id) = secret_id else {
        return Ok(());
    };
    state
        .storage
        .get_connector_secret(state.tenant_id, secret_id)?
        .map(|_| ())
        .ok_or_else(ApiError::not_found)
}

pub(crate) fn get_connector(
    state: &AppState,
    connector_id: Uuid,
) -> Result<IngestionConnector, ApiError> {
    state
        .storage
        .get_ingestion_connector(state.tenant_id, connector_id)?
        .ok_or_else(ApiError::not_found)
}

pub(crate) fn connector_event_metadata(connector: &IngestionConnector) -> Value {
    json!({
        "connector_id": connector.id,
        "connector_key": connector.connector_key,
        "connector_type": connector.connector_type,
        "connector_profile": connector.connector_profile,
        "enabled": connector.enabled,
        "secret_ref_id": connector.secret_ref_id
    })
}

pub(crate) fn record_connector_event(
    state: &AppState,
    event_type: impl Into<String>,
    connector: &IngestionConnector,
    message: Option<String>,
) -> Result<Event, ApiError> {
    record_event(
        state,
        EventDraft {
            event_type: event_type.into(),
            severity: EventSeverity::Info,
            source_entity_id: None,
            target_entity_id: None,
            message,
            occurred_at: Utc::now(),
            observed_at: None,
            correlation_id: None,
            raw_message_id: None,
            observation_id: None,
            command_id: None,
            action_id: None,
            action_result_id: None,
            metadata: Some(connector_event_metadata(connector)),
        },
    )
}
