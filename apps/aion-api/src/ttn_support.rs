use crate::{error::ApiError, record_event, AppState, EventDraft};
use aion_event::{Event, EventSeverity};
use aion_storage::TtnDeviceMapping;
use chrono::Utc;
use serde_json::{json, Value};

pub(crate) fn is_plausible_ttn_topic_filter(topic_filter: &str) -> bool {
    let normalized = topic_filter.trim().to_ascii_lowercase();
    normalized.contains("v3/")
        && normalized.contains("/devices/")
        && (normalized.ends_with("/up") || normalized.contains("/up/"))
}

fn ttn_device_mapping_event_metadata(mapping: &TtnDeviceMapping) -> Value {
    json!({
        "mapping_id": mapping.id,
        "connector_id": mapping.connector_id,
        "ttn_application_id": mapping.ttn_application_id,
        "ttn_device_id": mapping.ttn_device_id,
        "producer_entity_id": mapping.producer_entity_id,
        "feature_of_interest_id": mapping.feature_of_interest_id,
        "enabled": mapping.enabled
    })
}

pub(crate) fn record_ttn_device_mapping_event(
    state: &AppState,
    event_type: impl Into<String>,
    mapping: &TtnDeviceMapping,
    message: Option<String>,
) -> Result<Event, ApiError> {
    record_event(
        state,
        EventDraft {
            event_type: event_type.into(),
            severity: EventSeverity::Info,
            source_entity_id: Some(mapping.producer_entity_id),
            target_entity_id: mapping.feature_of_interest_id,
            message,
            occurred_at: Utc::now(),
            observed_at: None,
            correlation_id: None,
            raw_message_id: None,
            observation_id: None,
            command_id: None,
            action_id: None,
            action_result_id: None,
            metadata: Some(ttn_device_mapping_event_metadata(mapping)),
        },
    )
}
