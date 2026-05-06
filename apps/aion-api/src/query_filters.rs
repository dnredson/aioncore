use aion_raw_message::RawMessage;
use serde_json::Value;

pub(crate) fn optional_metadata_string_matches(
    metadata: Option<&Value>,
    key: &str,
    expected: Option<&str>,
) -> bool {
    expected
        .map(|expected| {
            metadata
                .map(|metadata| metadata_string_matches(metadata, key, expected))
                .unwrap_or(false)
        })
        .unwrap_or(true)
}

fn metadata_string_matches(metadata: &Value, key: &str, expected: &str) -> bool {
    value_string_matches(metadata.get(key), expected)
        || metadata
            .get("provenance")
            .map(|provenance| value_string_matches(provenance.get(key), expected))
            .unwrap_or(false)
}

pub(crate) fn optional_metadata_evidence_matches(
    metadata: Option<&Value>,
    evidence_id: Option<&str>,
    external_id: Option<&str>,
) -> bool {
    evidence_id
        .map(|expected| {
            metadata
                .map(|metadata| metadata_evidence_id_matches(metadata, expected))
                .unwrap_or(false)
        })
        .unwrap_or(true)
        && external_id
            .map(|expected| {
                metadata
                    .map(|metadata| metadata_external_id_matches(metadata, expected))
                    .unwrap_or(false)
            })
            .unwrap_or(true)
}

fn metadata_evidence_id_matches(metadata: &Value, expected: &str) -> bool {
    metadata
        .get("evidence_refs")
        .and_then(Value::as_array)
        .map(|refs| refs.iter().any(|value| value.as_str() == Some(expected)))
        .unwrap_or(false)
        || metadata
            .get("evidence")
            .and_then(Value::as_array)
            .map(|evidence| {
                evidence
                    .iter()
                    .any(|item| value_string_matches(item.get("evidence_id"), expected))
            })
            .unwrap_or(false)
}

fn metadata_external_id_matches(metadata: &Value, expected: &str) -> bool {
    metadata
        .get("external_id")
        .map(|value| value.as_str() == Some(expected))
        .unwrap_or(false)
        || metadata
            .get("evidence")
            .and_then(Value::as_array)
            .map(|evidence| {
                evidence
                    .iter()
                    .any(|item| value_string_matches(item.get("external_id"), expected))
            })
            .unwrap_or(false)
        || metadata
            .get("provenance")
            .and_then(|provenance| provenance.get("external_refs"))
            .and_then(Value::as_array)
            .map(|refs| {
                refs.iter()
                    .any(|item| value_string_matches(item.get("external_id"), expected))
            })
            .unwrap_or(false)
}

pub(crate) fn optional_raw_header_string_matches(
    raw_message: &RawMessage,
    key: &str,
    expected: Option<&str>,
) -> bool {
    expected
        .map(|expected| {
            raw_message
                .headers
                .get(key)
                .and_then(Value::as_str)
                .map(|value| value == expected)
                .unwrap_or(false)
        })
        .unwrap_or(true)
}

pub(crate) fn optional_raw_smartsentinel_string_matches(
    raw_message: &RawMessage,
    key: &str,
    expected: Option<&str>,
) -> bool {
    expected
        .map(|expected| raw_smartsentinel_string_matches(raw_message, key, expected))
        .unwrap_or(true)
}

fn raw_smartsentinel_string_matches(raw_message: &RawMessage, key: &str, expected: &str) -> bool {
    raw_message
        .headers
        .get("smartsentinel")
        .map(|metadata| metadata_string_matches(metadata, key, expected))
        .unwrap_or(false)
}

pub(crate) fn optional_raw_smartsentinel_evidence_id_matches(
    raw_message: &RawMessage,
    expected: Option<&str>,
) -> bool {
    expected
        .map(|expected| {
            raw_message
                .headers
                .get("smartsentinel")
                .map(|metadata| metadata_evidence_id_matches(metadata, expected))
                .unwrap_or(false)
        })
        .unwrap_or(true)
}

pub(crate) fn optional_raw_smartsentinel_external_id_matches(
    raw_message: &RawMessage,
    expected: Option<&str>,
) -> bool {
    expected
        .map(|expected| {
            raw_message
                .headers
                .get("smartsentinel")
                .map(|metadata| metadata_external_id_matches(metadata, expected))
                .unwrap_or(false)
        })
        .unwrap_or(true)
}

fn value_string_matches(value: Option<&Value>, expected: &str) -> bool {
    value.and_then(Value::as_str) == Some(expected)
}

pub(crate) mod event_filtering_helpers {
    pub(crate) use super::{
        optional_metadata_evidence_matches, optional_metadata_string_matches,
        optional_raw_header_string_matches, optional_raw_smartsentinel_evidence_id_matches,
        optional_raw_smartsentinel_external_id_matches, optional_raw_smartsentinel_string_matches,
    };
}
