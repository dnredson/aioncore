use super::*;
use rumqttc::{AsyncClient, Event, Incoming, MqttOptions, QoS};
use std::time::Duration as StdDuration;
use tokio::time::timeout;

const DEFAULT_MQTT_BROKER_URL: &str = "mqtt://127.0.0.1:1883";
const DEFAULT_MQTT_CLIENT_ID: &str = "aioncore-ingest";
const DEFAULT_MQTT_TOPIC_FILTER: &str = "aioncore/+/+/data";
const DEFAULT_MQTT_POLL_TIMEOUT_SECS: u64 = 10;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MqttIngestConfig {
    pub enabled: bool,
    pub broker_url: String,
    pub client_id: String,
    pub topic_filter: String,
    pub payload_format: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MqttPublishContext {
    pub topic: String,
    pub source_ref: String,
    pub producer_entity_id: Option<Uuid>,
    pub feature_of_interest_id: Option<Uuid>,
    pub payload_format: String,
    pub content_type: Option<String>,
    pub headers: Value,
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MqttTopicParts {
    pub producer_entity_id: Uuid,
    pub feature_of_interest_id: Uuid,
}

impl MqttIngestConfig {
    pub fn from_env() -> Result<Self, StartupError> {
        let enabled = parse_bool_env(
            std::env::var("AIONCORE_MQTT_ENABLED").ok().as_deref(),
            false,
        )?;

        Ok(Self {
            enabled,
            broker_url: std::env::var("AIONCORE_MQTT_BROKER_URL")
                .ok()
                .filter(|value| !value.trim().is_empty())
                .unwrap_or_else(|| DEFAULT_MQTT_BROKER_URL.to_string()),
            client_id: std::env::var("AIONCORE_MQTT_CLIENT_ID")
                .ok()
                .filter(|value| !value.trim().is_empty())
                .unwrap_or_else(|| DEFAULT_MQTT_CLIENT_ID.to_string()),
            topic_filter: std::env::var("AIONCORE_MQTT_TOPIC_FILTER")
                .ok()
                .filter(|value| !value.trim().is_empty())
                .unwrap_or_else(|| DEFAULT_MQTT_TOPIC_FILTER.to_string()),
            payload_format: std::env::var("AIONCORE_MQTT_PAYLOAD_FORMAT")
                .ok()
                .and_then(|value| {
                    let trimmed = value.trim();
                    if trimmed.is_empty() {
                        None
                    } else {
                        Some(trimmed.to_string())
                    }
                }),
        })
    }
}

fn parse_bool_env(value: Option<&str>, default: bool) -> Result<bool, StartupError> {
    match value.map(str::trim).filter(|value| !value.is_empty()) {
        None => Ok(default),
        Some(value) if value.eq_ignore_ascii_case("true") || value == "1" => Ok(true),
        Some(value) if value.eq_ignore_ascii_case("false") || value == "0" => Ok(false),
        Some(other) => Err(StartupError::backend_initialization(format!(
            "invalid boolean value '{other}' for MQTT configuration"
        ))),
    }
}

pub async fn start_if_enabled(state: AppState) -> Result<(), StartupError> {
    let config = MqttIngestConfig::from_env()?;
    start(state, config).await
}

pub async fn start(state: AppState, config: MqttIngestConfig) -> Result<(), StartupError> {
    if !config.enabled {
        return Ok(());
    }

    eprintln!(
        "mqtt startup enabled=true broker_url={} client_id={} topic_filter={} payload_format={}",
        config.broker_url,
        config.client_id,
        config.topic_filter,
        config.payload_format.as_deref().unwrap_or("canonical-json")
    );

    let (host, port) = parse_broker_url(&config.broker_url)?;
    let mut options = MqttOptions::new(config.client_id.clone(), host, port);
    options.set_keep_alive(StdDuration::from_secs(30));

    let (client, mut eventloop) = AsyncClient::new(options, 16);
    wait_for_connection(&mut eventloop, &config).await?;
    client
        .subscribe(config.topic_filter.clone(), QoS::AtLeastOnce)
        .await
        .map_err(|err| {
            StartupError::backend_initialization(format!(
                "failed to subscribe to MQTT topic filter: {err}"
            ))
        })?;

    eprintln!(
        "mqtt startup subscribed broker_url={} topic_filter={}",
        config.broker_url, config.topic_filter
    );

    tokio::spawn(async move {
        let _client = client;
        loop {
            match eventloop.poll().await {
                Ok(Event::Incoming(Incoming::Publish(publish))) => {
                    if let Err(err) = handle_publish(&state, &config, publish).await {
                        eprintln!("mqtt ingest failed: {err:?}");
                    }
                }
                Ok(_) => {}
                Err(err) => {
                    eprintln!("mqtt event loop stopped: {err}");
                    break;
                }
            }
        }
    });

    Ok(())
}

pub fn parse_mqtt_topic(topic: &str) -> Result<MqttTopicParts, String> {
    let segments = topic.split('/').collect::<Vec<_>>();
    if segments.len() != 4 {
        return Err(
            "topic must have four segments: aioncore/{producer_entity_id}/{feature_of_interest_id}/data"
                .to_string(),
        );
    }
    if segments[0] != "aioncore" {
        return Err("topic must start with aioncore".to_string());
    }
    if segments[3] != "data" {
        return Err("topic must end with data".to_string());
    }

    let producer_entity_id = decode_topic_uuid(segments[1])?;
    let feature_of_interest_id = decode_topic_uuid(segments[2])?;

    Ok(MqttTopicParts {
        producer_entity_id,
        feature_of_interest_id,
    })
}

pub fn mqtt_publish_to_context(
    topic: &str,
    payload: &[u8],
    configured_payload_format: Option<&str>,
    content_type: Option<&str>,
) -> Result<MqttPublishContext, String> {
    let parsed_topic = parse_mqtt_topic(topic).ok();
    let payload_format = resolve_mqtt_payload_format(configured_payload_format)?;
    let content_type = content_type.map(ToOwned::to_owned).or_else(|| {
        default_content_type_for_payload_format(&payload_format).map(ToOwned::to_owned)
    });
    let headers = json!({
        "topic": topic,
        "payload_format_source": if configured_payload_format.is_some() { "config" } else { "default" },
        "payload_format": payload_format,
        "ingest_source": "mqtt"
    });

    Ok(MqttPublishContext {
        topic: topic.to_string(),
        source_ref: topic.to_string(),
        producer_entity_id: parsed_topic.as_ref().map(|parts| parts.producer_entity_id),
        feature_of_interest_id: parsed_topic
            .as_ref()
            .map(|parts| parts.feature_of_interest_id),
        payload_format,
        content_type,
        headers,
        payload: payload.to_vec(),
    })
}

async fn handle_publish(
    state: &AppState,
    config: &MqttIngestConfig,
    publish: rumqttc::Publish,
) -> Result<(), ApiError> {
    let topic = publish.topic.clone();
    let payload = publish.payload.to_vec();
    eprintln!(
        "mqtt ingest received topic={} payload_bytes={}",
        topic,
        payload.len()
    );
    let context =
        match mqtt_publish_to_context(&topic, &payload, config.payload_format.as_deref(), None) {
            Ok(context) => context,
            Err(err) => {
                let raw_message = store_mqtt_raw_message(
                    state,
                    &topic,
                    None,
                    None,
                    &payload,
                    config.payload_format.as_deref(),
                    &err,
                )?;
                record_mqtt_failure(
                    state,
                    &topic,
                    None,
                    None,
                    raw_message.id,
                    "invalid MQTT topic",
                    json!({
                        "topic": topic,
                        "reason": "invalid_topic",
                        "parse_error": err
                    }),
                )?;
                return Ok(());
            }
        };

    let topic_parts = parse_mqtt_topic(&topic).ok();
    let producer_entity_id = context.producer_entity_id;
    let feature_of_interest_id = context.feature_of_interest_id;
    let raw_message = store_mqtt_raw_message(
        state,
        &topic,
        producer_entity_id,
        feature_of_interest_id,
        &context.payload,
        Some(&context.payload_format),
        "received",
    )?;

    record_ingest_event_optional(
        state,
        "aion:MqttMessageReceived",
        EventSeverity::Info,
        producer_entity_id,
        feature_of_interest_id,
        Some(raw_message.id),
        Some("MQTT message received".to_string()),
        json!({
            "topic": topic,
            "payload_format": context.payload_format,
            "ingest_source": "mqtt"
        }),
    )?;

    let Some(topic_parts) = topic_parts else {
        record_mqtt_failure(
            state,
            &topic,
            producer_entity_id,
            feature_of_interest_id,
            raw_message.id,
            "invalid MQTT topic",
            json!({
                "topic": topic,
                "reason": "invalid_topic"
            }),
        )?;
        return Ok(());
    };

    ensure_entity_exists(state, topic_parts.producer_entity_id)?;
    ensure_entity_exists(state, topic_parts.feature_of_interest_id)?;

    let profile = state
        .storage
        .get_payload_profile(state.tenant_id, topic_parts.producer_entity_id)?;

    let decoder_config = if payload_format_requires_mapping(&context.payload_format) {
        let mapping = profile
            .as_ref()
            .and_then(|profile| profile.attribute_mapping.clone());
        if mapping.is_none() {
            record_mqtt_failure(
                state,
                &topic,
                producer_entity_id,
                feature_of_interest_id,
                raw_message.id,
                "missing payload profile mapping",
                json!({
                    "topic": topic,
                    "reason": "missing_mapping",
                    "payload_format": context.payload_format
                }),
            )?;
            return Ok(());
        }
        mapping
    } else {
        None
    };

    let decoder = decoder_for_format(&context.payload_format).map_err(|_| {
        ApiError::bad_request(format!(
            "unsupported MQTT payload format '{}'",
            context.payload_format
        ))
    })?;

    let decode_result = decoder.decode(DecodeInput {
        tenant_id: state.tenant_id,
        device_key: Some(topic_parts.producer_entity_id.to_string()),
        format: PayloadFormat::from_str(&context.payload_format)
            .map_err(|err| ApiError::bad_request(err.to_string()))?,
        content_type: context.content_type.clone(),
        payload: context.payload.clone(),
        received_at: Utc::now(),
        config: decoder_config,
    });

    let decoded = match decode_result {
        Ok(decoded) => decoded,
        Err(err) => {
            record_mqtt_failure(
                state,
                &topic,
                producer_entity_id,
                feature_of_interest_id,
                raw_message.id,
                err.to_string(),
                json!({
                    "topic": topic,
                    "reason": "decoder_error",
                    "payload_format": context.payload_format
                }),
            )?;
            return Ok(());
        }
    };

    let received_at = Utc::now();
    let mut observations = Vec::with_capacity(decoded.len());
    for measurement in decoded {
        let observation = Observation::new(
            state.tenant_id,
            topic_parts.producer_entity_id,
            topic_parts.feature_of_interest_id,
            measurement.observed_property,
            measurement.value,
            measurement.unit,
            measurement.time,
            received_at,
            "mqtt".to_string(),
            context.payload_format.clone(),
            Some(raw_message.id),
            json!({
                "topic": topic,
                "ingest_source": "mqtt"
            }),
            measurement.metadata,
        )
        .map_err(|err| ApiError::bad_request(err.to_string()))?;
        let observation = state.storage.store_observation(observation)?;
        evaluate_rules_for_observation(state, &observation, true)?;
        observations.push(observation);
    }

    state
        .storage
        .mark_raw_message_normalized(state.tenant_id, raw_message.id)?;
    record_ingest_event_optional(
        state,
        "aion:PayloadIngested",
        EventSeverity::Info,
        producer_entity_id,
        feature_of_interest_id,
        Some(raw_message.id),
        Some("Payload ingested and normalized".to_string()),
        json!({
            "topic": topic,
            "payload_format": context.payload_format,
            "observation_count": observations.len(),
            "ingest_source": "mqtt"
        }),
    )?;

    Ok(())
}

async fn wait_for_connection(
    eventloop: &mut rumqttc::EventLoop,
    config: &MqttIngestConfig,
) -> Result<(), StartupError> {
    timeout(
        StdDuration::from_secs(DEFAULT_MQTT_POLL_TIMEOUT_SECS),
        async {
            loop {
                match eventloop.poll().await {
                    Ok(Event::Incoming(Incoming::ConnAck(_))) => return Ok(()),
                    Ok(_) => continue,
                    Err(err) => {
                        return Err(StartupError::backend_initialization(format!(
                            "failed to connect to MQTT broker at {}: {err}",
                            config.broker_url
                        )))
                    }
                }
            }
        },
    )
    .await
    .map_err(|_| {
        StartupError::backend_initialization(format!(
            "timed out connecting to MQTT broker at {}",
            config.broker_url
        ))
    })?
}

fn resolve_mqtt_payload_format(configured: Option<&str>) -> Result<String, String> {
    let payload_format = configured
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .unwrap_or_else(|| "canonical-json".to_string());
    let normalized = payload_format.to_ascii_lowercase().replace('-', "_");
    match normalized.as_str() {
        "senml_json" | "ultralight" | "canonical_json" => Ok(payload_format),
        _ => Err(format!("unsupported MQTT payload format: {payload_format}")),
    }
}

fn default_content_type_for_payload_format(payload_format: &str) -> Option<&'static str> {
    match payload_format
        .to_ascii_lowercase()
        .replace('-', "_")
        .as_str()
    {
        "senml_json" => Some("application/senml+json"),
        "ultralight" => Some("text/plain"),
        "canonical_json" => Some("application/json"),
        _ => None,
    }
}

fn parse_broker_url(value: &str) -> Result<(String, u16), StartupError> {
    let trimmed = value.trim();
    let without_scheme = trimmed.strip_prefix("mqtt://").ok_or_else(|| {
        StartupError::backend_initialization(format!(
            "unsupported MQTT broker URL '{trimmed}'; expected mqtt://host:port"
        ))
    })?;
    let host_port = without_scheme.split('/').next().unwrap_or(without_scheme);
    let host_port = host_port.split('@').next_back().unwrap_or(host_port);
    let (host, port) = match host_port.rsplit_once(':') {
        Some((host, port)) => {
            let port = port.parse::<u16>().map_err(|err| {
                StartupError::backend_initialization(format!(
                    "invalid MQTT broker port in '{trimmed}': {err}"
                ))
            })?;
            (host.to_string(), port)
        }
        None => (host_port.to_string(), 1883),
    };

    if host.is_empty() {
        return Err(StartupError::backend_initialization(format!(
            "invalid MQTT broker URL '{trimmed}'"
        )));
    }

    Ok((host, port))
}

fn decode_topic_uuid(segment: &str) -> Result<Uuid, String> {
    let decoded = percent_decode(segment)?;
    Uuid::parse_str(&decoded)
        .map_err(|err| format!("invalid UUID in MQTT topic segment '{segment}': {err}"))
}

fn percent_decode(value: &str) -> Result<String, String> {
    let bytes = value.as_bytes();
    let mut output = Vec::with_capacity(bytes.len());
    let mut index = 0;

    while index < bytes.len() {
        match bytes[index] {
            b'%' => {
                if index + 2 >= bytes.len() {
                    return Err(format!("invalid percent-encoding in '{value}'"));
                }
                let high = hex_value(bytes[index + 1])
                    .ok_or_else(|| format!("invalid percent-encoding in '{value}'"))?;
                let low = hex_value(bytes[index + 2])
                    .ok_or_else(|| format!("invalid percent-encoding in '{value}'"))?;
                output.push((high << 4) | low);
                index += 3;
            }
            byte => {
                output.push(byte);
                index += 1;
            }
        }
    }

    String::from_utf8(output).map_err(|_| format!("invalid UTF-8 in '{value}'"))
}

fn hex_value(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

fn store_mqtt_raw_message(
    state: &AppState,
    topic: &str,
    producer_entity_id: Option<Uuid>,
    feature_of_interest_id: Option<Uuid>,
    payload: &[u8],
    configured_payload_format: Option<&str>,
    ingest_reason: &str,
) -> Result<RawMessage, ApiError> {
    let payload_format = configured_payload_format
        .map(ToOwned::to_owned)
        .unwrap_or_else(|| "canonical-json".to_string());
    let raw_message = RawMessage::new(
        state.tenant_id,
        RawMessageSource::Mqtt,
        Some(topic.to_string()),
        producer_entity_id.map(|id| id.to_string()),
        Some(payload_format.clone()),
        default_content_type_for_payload_format(&payload_format).map(ToOwned::to_owned),
        producer_entity_id,
        feature_of_interest_id,
        Some(payload_format.clone()),
        json!({
            "topic": topic,
            "protocol": "mqtt",
            "producer_entity_id": producer_entity_id,
            "feature_of_interest_id": feature_of_interest_id,
            "ingest_source": "mqtt",
            "reason": ingest_reason,
            "payload_format": payload_format,
        }),
        payload.to_vec(),
        Utc::now(),
    )
    .map_err(|raw_err| ApiError::bad_request(raw_err.to_string()))?;
    Ok(state.storage.store_raw_message(raw_message)?)
}

fn record_mqtt_failure(
    state: &AppState,
    topic: &str,
    producer_entity_id: Option<Uuid>,
    feature_of_interest_id: Option<Uuid>,
    raw_message_id: Uuid,
    message: impl Into<String>,
    metadata: Value,
) -> Result<(), ApiError> {
    let message = message.into();
    state
        .storage
        .mark_raw_message_failed(state.tenant_id, raw_message_id, &message)?;
    record_ingest_event_optional(
        state,
        "aion:PayloadIngestionFailed",
        EventSeverity::Error,
        producer_entity_id,
        feature_of_interest_id,
        Some(raw_message_id),
        Some(message.to_string()),
        json!({
            "topic": topic,
            "message": message,
            "ingest_source": "mqtt",
            "details": metadata
        }),
    )?;
    Ok(())
}

#[allow(dead_code)]
pub fn mqtt_topic_to_request_context(topic: &str) -> Result<(Uuid, Uuid), String> {
    let topic = parse_mqtt_topic(topic)?;
    Ok((topic.producer_entity_id, topic.feature_of_interest_id))
}

#[allow(dead_code)]
pub fn mqtt_request_context(
    topic: &str,
    payload: &[u8],
    payload_format: Option<&str>,
    content_type: Option<&str>,
) -> Result<MqttPublishContext, String> {
    mqtt_publish_to_context(topic, payload, payload_format, content_type)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_simple_mqtt_topic() {
        let producer = Uuid::new_v4();
        let feature = Uuid::new_v4();
        let topic = format!("aioncore/{producer}/{feature}/data");
        let parsed = parse_mqtt_topic(&topic).expect("topic should parse");
        assert_eq!(parsed.producer_entity_id, producer);
        assert_eq!(parsed.feature_of_interest_id, feature);
    }

    #[test]
    fn parses_percent_encoded_mqtt_topic() {
        let producer = Uuid::new_v4();
        let feature = Uuid::new_v4();
        let encoded_producer = producer.to_string().replace('-', "%2D");
        let encoded_feature = feature.to_string().replace('-', "%2D");
        let topic = format!("aioncore/{encoded_producer}/{encoded_feature}/data");
        let parsed = parse_mqtt_topic(&topic).expect("topic should parse");
        assert_eq!(parsed.producer_entity_id, producer);
        assert_eq!(parsed.feature_of_interest_id, feature);
    }

    #[test]
    fn converts_mqtt_message_to_ingest_context() {
        let producer = Uuid::new_v4();
        let feature = Uuid::new_v4();
        let topic = format!("aioncore/{producer}/{feature}/data");
        let context = mqtt_publish_to_context(&topic, br#"{"v":12}"#, Some("canonical-json"), None)
            .expect("context should build");
        assert_eq!(context.producer_entity_id, Some(producer));
        assert_eq!(context.feature_of_interest_id, Some(feature));
        assert_eq!(context.payload_format, "canonical-json");
    }

    #[test]
    fn defaults_to_canonical_json_when_format_absent() {
        let context = mqtt_publish_to_context(
            "aioncore/11111111-1111-1111-1111-111111111111/22222222-2222-2222-2222-222222222222/data",
            br#"{"v":12}"#,
            None,
            None,
        )
        .expect("context should build");
        assert_eq!(context.payload_format, "canonical-json");
    }

    #[test]
    fn rejects_unsupported_payload_format() {
        assert!(resolve_mqtt_payload_format(Some("xml")).is_err());
    }
}
