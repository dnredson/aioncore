use super::*;
use rumqttc::{AsyncClient, Event as MqttEvent, Incoming, MqttOptions, QoS};
use std::fmt;
use std::time::Duration as StdDuration;

const DEFAULT_MQTT_BROKER_URL: &str = "mqtt://127.0.0.1:1883";
const DEFAULT_MQTT_CLIENT_ID: &str = "aioncore-ingest";
const DEFAULT_MQTT_TOPIC_FILTER: &str = "aioncore/+/+/data";
const CONNECTOR_RECONNECT_INITIAL_DELAY_SECS: u64 = 1;
const CONNECTOR_RECONNECT_MAX_DELAY_SECS: u64 = 60;

#[derive(Clone, PartialEq, Eq)]
pub struct MqttIngestConfig {
    pub enabled: bool,
    pub broker_url: String,
    pub client_id: String,
    pub topic_filter: String,
    pub payload_format: Option<String>,
    pub content_type: Option<String>,
    pub username: Option<String>,
    password: Option<String>,
    pub connector: Option<MqttConnectorMetadata>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MqttConnectorMetadata {
    pub connector_id: Uuid,
    pub connector_key: String,
    pub connector_profile: ConnectorProfile,
}

impl fmt::Debug for MqttIngestConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MqttIngestConfig")
            .field("enabled", &self.enabled)
            .field("broker_url", &self.broker_url)
            .field("client_id", &self.client_id)
            .field("topic_filter", &self.topic_filter)
            .field("payload_format", &self.payload_format)
            .field("content_type", &self.content_type)
            .field("username", &self.username)
            .field("password", &self.password.as_ref().map(|_| "<redacted>"))
            .field("connector", &self.connector)
            .finish()
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct MqttReadiness {
    pub ready: bool,
    pub enabled: bool,
    pub connected: bool,
    pub subscribed: bool,
    pub broker_url: Option<String>,
    pub topic_filter: Option<String>,
    pub last_error: Option<String>,
    pub last_message_at: Option<DateTime<Utc>>,
    pub last_successful_ingest_at: Option<DateTime<Utc>>,
    pub last_failed_ingest_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Default)]
pub struct MqttWorkerState {
    pub enabled: bool,
    pub connected: bool,
    pub subscribed: bool,
    pub broker_url: Option<String>,
    pub topic_filter: Option<String>,
    pub last_error: Option<String>,
    pub last_message_at: Option<DateTime<Utc>>,
    pub last_successful_ingest_at: Option<DateTime<Utc>>,
    pub last_failed_ingest_at: Option<DateTime<Utc>>,
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
        Self::from_env_values(MqttEnvValues {
            enabled: std::env::var("AIONCORE_MQTT_ENABLED").ok(),
            broker_url: std::env::var("AIONCORE_MQTT_BROKER_URL").ok(),
            client_id: std::env::var("AIONCORE_MQTT_CLIENT_ID").ok(),
            topic_filter: std::env::var("AIONCORE_MQTT_TOPIC_FILTER").ok(),
            payload_format: std::env::var("AIONCORE_MQTT_PAYLOAD_FORMAT").ok(),
            username: std::env::var("AIONCORE_MQTT_USERNAME").ok(),
            password: std::env::var("AIONCORE_MQTT_PASSWORD").ok(),
        })
    }

    pub fn from_env_values(values: MqttEnvValues) -> Result<Self, StartupError> {
        let enabled = parse_bool_env(values.enabled.as_deref(), false)?;

        Ok(Self {
            enabled,
            broker_url: values
                .broker_url
                .filter(|value| !value.trim().is_empty())
                .unwrap_or_else(|| DEFAULT_MQTT_BROKER_URL.to_string()),
            client_id: values
                .client_id
                .filter(|value| !value.trim().is_empty())
                .unwrap_or_else(|| DEFAULT_MQTT_CLIENT_ID.to_string()),
            topic_filter: values
                .topic_filter
                .filter(|value| !value.trim().is_empty())
                .unwrap_or_else(|| DEFAULT_MQTT_TOPIC_FILTER.to_string()),
            payload_format: optional_nonempty(values.payload_format),
            content_type: None,
            username: optional_nonempty(values.username),
            password: optional_nonempty(values.password),
            connector: None,
        })
    }

    pub fn for_connector(
        broker_url: String,
        client_id: String,
        topic_filter: String,
        payload_format: Option<String>,
        content_type: Option<String>,
        connector: MqttConnectorMetadata,
    ) -> Self {
        Self {
            enabled: true,
            broker_url,
            client_id,
            topic_filter,
            payload_format,
            content_type,
            username: None,
            password: None,
            connector: Some(connector),
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MqttEnvValues {
    pub enabled: Option<String>,
    pub broker_url: Option<String>,
    pub client_id: Option<String>,
    pub topic_filter: Option<String>,
    pub payload_format: Option<String>,
    pub username: Option<String>,
    pub password: Option<String>,
}

fn optional_nonempty(value: Option<String>) -> Option<String> {
    value.and_then(|value| {
        let trimmed = value.trim();
        if trimmed.is_empty() {
            None
        } else {
            Some(trimmed.to_string())
        }
    })
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
    start_runtime(state, config, false).await.map(|_| ())
}

pub async fn start_connector_worker(
    state: AppState,
    config: MqttIngestConfig,
) -> Result<tokio::task::JoinHandle<()>, StartupError> {
    if let Some(connector) = config.connector.as_ref() {
        mark_connector_worker_starting(&state, connector.connector_id);
    }
    start_runtime(state, config, true)
        .await?
        .ok_or_else(|| StartupError::backend_initialization("connector MQTT worker was disabled"))
}

async fn start_runtime(
    state: AppState,
    config: MqttIngestConfig,
    connector_worker: bool,
) -> Result<Option<tokio::task::JoinHandle<()>>, StartupError> {
    update_worker_state(
        &state,
        |worker| {
            worker.enabled = config.enabled;
            worker.connected = false;
            worker.subscribed = false;
            worker.broker_url = Some(config.broker_url.clone());
            worker.topic_filter = Some(config.topic_filter.clone());
            worker.last_error = None;
        },
        connector_worker,
    );

    if !config.enabled {
        return Ok(None);
    }

    eprintln!(
        "mqtt startup enabled=true broker_url={} client_id={} topic_filter={} payload_format={}",
        config.broker_url,
        config.client_id,
        config.topic_filter,
        config.payload_format.as_deref().unwrap_or("canonical-json")
    );

    if connector_worker {
        return start_connector_runtime(state, config).await;
    }

    let (host, port) = parse_broker_url(&config.broker_url)?;
    let mut options = MqttOptions::new(config.client_id.clone(), host, port);
    options.set_keep_alive(StdDuration::from_secs(30));
    if let Some(username) = config.username.as_deref() {
        options.set_credentials(username, config.password.as_deref().unwrap_or_default());
    }

    let (client, mut eventloop) = AsyncClient::new(options, 16);

    let handle = tokio::spawn(async move {
        let _client = client;
        let _ = record_mqtt_worker_event(
            &state,
            "aion:MqttWorkerStarted",
            EventSeverity::Info,
            Some("MQTT worker started".to_string()),
            metadata_with_connector(
                json!({
                    "broker_url": config.broker_url,
                    "topic_filter": config.topic_filter,
                    "payload_format": config.payload_format.as_deref().unwrap_or("canonical-json"),
                    "credentials_configured": config.username.is_some()
                }),
                config.connector.as_ref(),
            ),
        );
        loop {
            match eventloop.poll().await {
                Ok(MqttEvent::Incoming(Incoming::ConnAck(_))) => {
                    update_worker_state(
                        &state,
                        |worker| {
                            worker.connected = true;
                            worker.last_error = None;
                        },
                        connector_worker,
                    );
                    let _ = record_mqtt_worker_event(
                        &state,
                        "aion:MqttWorkerConnected",
                        EventSeverity::Info,
                        Some("MQTT worker connected".to_string()),
                        metadata_with_connector(
                            json!({
                                "broker_url": config.broker_url,
                                "topic_filter": config.topic_filter
                            }),
                            config.connector.as_ref(),
                        ),
                    );
                    if let Some(connector) = config.connector.as_ref() {
                        mark_connector_worker_connected(&state, connector.connector_id);
                    }
                    match _client
                        .subscribe(config.topic_filter.clone(), QoS::AtLeastOnce)
                        .await
                    {
                        Ok(()) => {}
                        Err(err) => {
                            let message =
                                format!("failed to subscribe to MQTT topic filter: {err}");
                            mark_worker_failure(&state, message.clone(), connector_worker);
                            if let Some(connector) = config.connector.as_ref() {
                                mark_connector_worker_failure(
                                    &state,
                                    connector.connector_id,
                                    message.clone(),
                                );
                            }
                            let _ = record_mqtt_worker_event(
                                &state,
                                "aion:MqttWorkerConnectionFailed",
                                EventSeverity::Error,
                                Some(message.clone()),
                                metadata_with_connector(
                                    json!({
                                        "broker_url": config.broker_url,
                                        "topic_filter": config.topic_filter,
                                        "reason": "subscribe_failed",
                                        "error": message
                                    }),
                                    config.connector.as_ref(),
                                ),
                            );
                        }
                    }
                }
                Ok(MqttEvent::Incoming(Incoming::SubAck(_))) => {
                    update_worker_state(
                        &state,
                        |worker| {
                            worker.subscribed = true;
                            worker.last_error = None;
                        },
                        connector_worker,
                    );
                    if let Some(connector) = config.connector.as_ref() {
                        mark_connector_worker_subscribed(&state, connector.connector_id);
                    }
                    eprintln!(
                        "mqtt startup subscribed broker_url={} topic_filter={}",
                        config.broker_url, config.topic_filter
                    );
                    let _ = record_mqtt_worker_event(
                        &state,
                        "aion:MqttWorkerSubscribed",
                        EventSeverity::Info,
                        Some("MQTT worker subscribed".to_string()),
                        metadata_with_connector(
                            json!({
                                "broker_url": config.broker_url,
                                "topic_filter": config.topic_filter
                            }),
                            config.connector.as_ref(),
                        ),
                    );
                }
                Ok(MqttEvent::Incoming(Incoming::Publish(publish))) => {
                    update_worker_state(
                        &state,
                        |worker| {
                            worker.last_message_at = Some(Utc::now());
                        },
                        connector_worker,
                    );
                    if let Some(connector) = config.connector.as_ref() {
                        mark_connector_worker_message(&state, connector.connector_id);
                    }
                    if let Err(err) = handle_publish(&state, &config, publish).await {
                        eprintln!("mqtt ingest failed: {err:?}");
                        mark_worker_ingest_failed(&state, err.message, connector_worker);
                    }
                }
                Ok(_) => {}
                Err(err) => {
                    let message = format!(
                        "failed to connect to MQTT broker at {}: {err}",
                        config.broker_url
                    );
                    eprintln!("mqtt event loop stopped: {message}");
                    mark_worker_failure(&state, message.clone(), connector_worker);
                    if let Some(connector) = config.connector.as_ref() {
                        mark_connector_worker_failure(
                            &state,
                            connector.connector_id,
                            message.clone(),
                        );
                    }
                    let _ = record_mqtt_worker_event(
                        &state,
                        "aion:MqttWorkerConnectionFailed",
                        EventSeverity::Error,
                        Some(message.clone()),
                        metadata_with_connector(
                            json!({
                                "broker_url": config.broker_url,
                                "topic_filter": config.topic_filter,
                                "reason": "event_loop_error",
                                "error": message
                            }),
                            config.connector.as_ref(),
                        ),
                    );
                    break;
                }
            }
        }
    });

    Ok(Some(handle))
}

async fn start_connector_runtime(
    state: AppState,
    config: MqttIngestConfig,
) -> Result<Option<tokio::task::JoinHandle<()>>, StartupError> {
    let (host, port) = parse_broker_url(&config.broker_url)?;
    let handle = tokio::spawn(async move {
        let _ = record_mqtt_worker_event(
            &state,
            "aion:MqttWorkerStarted",
            EventSeverity::Info,
            Some("MQTT connector worker started".to_string()),
            metadata_with_connector(
                json!({
                    "broker_url": config.broker_url,
                    "topic_filter": config.topic_filter,
                    "payload_format": config.payload_format.as_deref().unwrap_or("canonical-json")
                }),
                config.connector.as_ref(),
            ),
        );

        let mut reconnects_scheduled = 0_u32;
        loop {
            let mut options = MqttOptions::new(config.client_id.clone(), host.clone(), port);
            options.set_keep_alive(StdDuration::from_secs(30));
            let (client, mut eventloop) = AsyncClient::new(options, 16);
            let mut reconnected_event_emitted = false;

            let failure_message = loop {
                match eventloop.poll().await {
                    Ok(MqttEvent::Incoming(Incoming::ConnAck(_))) => {
                        if let Some(connector) = config.connector.as_ref() {
                            mark_connector_worker_connected(&state, connector.connector_id);
                        }
                        if let Err(err) = client
                            .subscribe(config.topic_filter.clone(), QoS::AtLeastOnce)
                            .await
                        {
                            break format!("failed to subscribe to MQTT topic filter: {err}");
                        }
                    }
                    Ok(MqttEvent::Incoming(Incoming::SubAck(_))) => {
                        if let Some(connector) = config.connector.as_ref() {
                            mark_connector_worker_subscribed(&state, connector.connector_id);
                        }
                        let _ = record_mqtt_worker_event(
                            &state,
                            "aion:MqttWorkerSubscribed",
                            EventSeverity::Info,
                            Some("MQTT connector worker subscribed".to_string()),
                            metadata_with_connector(
                                json!({
                                    "broker_url": config.broker_url,
                                    "topic_filter": config.topic_filter
                                }),
                                config.connector.as_ref(),
                            ),
                        );
                        if reconnects_scheduled > 0 && !reconnected_event_emitted {
                            reconnected_event_emitted = true;
                            let _ = record_mqtt_worker_event(
                                &state,
                                "aion:ConnectorWorkerReconnected",
                                EventSeverity::Info,
                                Some("Connector worker reconnected".to_string()),
                                metadata_with_connector(
                                    json!({
                                        "broker_url": config.broker_url,
                                        "topic_filter": config.topic_filter,
                                        "reconnect_attempts": reconnects_scheduled
                                    }),
                                    config.connector.as_ref(),
                                ),
                            );
                        }
                    }
                    Ok(MqttEvent::Incoming(Incoming::Publish(publish))) => {
                        if let Some(connector) = config.connector.as_ref() {
                            mark_connector_worker_message(&state, connector.connector_id);
                        }
                        if let Err(err) = handle_publish(&state, &config, publish).await {
                            eprintln!("mqtt connector ingest failed: {err:?}");
                            if let Some(connector) = config.connector.as_ref() {
                                mark_connector_worker_ingest_failed(
                                    &state,
                                    connector.connector_id,
                                    err.message,
                                );
                            }
                        }
                    }
                    Ok(_) => {}
                    Err(err) => {
                        break format!(
                            "failed to connect to MQTT broker at {}: {err}",
                            config.broker_url
                        );
                    }
                }
            };

            eprintln!("mqtt connector event loop stopped: {failure_message}");
            if let Some(connector) = config.connector.as_ref() {
                mark_connector_worker_failure(
                    &state,
                    connector.connector_id,
                    failure_message.clone(),
                );
                let _ = record_mqtt_worker_event(
                    &state,
                    "aion:ConnectorWorkerDisconnected",
                    EventSeverity::Warning,
                    Some(failure_message.clone()),
                    metadata_with_connector(
                        json!({
                            "broker_url": config.broker_url,
                            "topic_filter": config.topic_filter,
                            "error": failure_message
                        }),
                        config.connector.as_ref(),
                    ),
                );

                let delay = connector_reconnect_delay(reconnects_scheduled);
                reconnects_scheduled = reconnects_scheduled.saturating_add(1);
                let next_reconnect_at = mark_connector_worker_reconnect_scheduled(
                    &state,
                    connector.connector_id,
                    "MQTT connector worker reconnect scheduled".to_string(),
                    delay,
                );
                let _ = record_mqtt_worker_event(
                    &state,
                    "aion:ConnectorWorkerReconnectScheduled",
                    EventSeverity::Warning,
                    Some("Connector worker reconnect scheduled".to_string()),
                    metadata_with_connector(
                        json!({
                            "broker_url": config.broker_url,
                            "topic_filter": config.topic_filter,
                            "delay_seconds": delay.as_secs(),
                            "next_reconnect_at": next_reconnect_at
                        }),
                        config.connector.as_ref(),
                    ),
                );
                tokio::time::sleep(delay).await;
            } else {
                break;
            }
        }
    });

    Ok(Some(handle))
}

fn connector_reconnect_delay(previous_attempts: u32) -> StdDuration {
    let exponent = previous_attempts.min(6);
    let seconds = CONNECTOR_RECONNECT_INITIAL_DELAY_SECS
        .saturating_mul(2_u64.saturating_pow(exponent))
        .min(CONNECTOR_RECONNECT_MAX_DELAY_SECS);
    StdDuration::from_secs(seconds)
}

pub fn readiness(state: &AppState) -> MqttReadiness {
    let worker = state
        .mqtt_state
        .read()
        .map(|guard| guard.clone())
        .unwrap_or_else(|_| MqttWorkerState {
            enabled: true,
            connected: false,
            subscribed: false,
            broker_url: None,
            topic_filter: None,
            last_error: Some("mqtt worker state lock was poisoned".to_string()),
            last_message_at: None,
            last_successful_ingest_at: None,
            last_failed_ingest_at: None,
        });
    let ready = !worker.enabled || (worker.connected && worker.subscribed);
    MqttReadiness {
        ready,
        enabled: worker.enabled,
        connected: worker.connected,
        subscribed: worker.subscribed,
        broker_url: worker.broker_url,
        topic_filter: worker.topic_filter,
        last_error: worker.last_error,
        last_message_at: worker.last_message_at,
        last_successful_ingest_at: worker.last_successful_ingest_at,
        last_failed_ingest_at: worker.last_failed_ingest_at,
    }
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
    let context = match mqtt_publish_to_context(
        &topic,
        &payload,
        config.payload_format.as_deref(),
        config.content_type.as_deref(),
    ) {
        Ok(context) => context,
        Err(err) => {
            let raw_message = store_mqtt_raw_message(
                state,
                &topic,
                None,
                None,
                &payload,
                config.payload_format.as_deref(),
                config.content_type.as_deref(),
                config.connector.as_ref(),
                &err,
            )?;
            record_mqtt_failure(
                state,
                &topic,
                None,
                None,
                raw_message.id,
                "unsupported MQTT payload format",
                config.connector.as_ref(),
                json!({
                    "topic": topic,
                    "reason": "unsupported_payload_format",
                    "error": err
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
        context.content_type.as_deref(),
        config.connector.as_ref(),
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
        metadata_with_connector(
            json!({
                "topic": topic,
                "payload_format": context.payload_format,
                "ingest_source": "mqtt"
            }),
            config.connector.as_ref(),
        ),
    )?;

    let Some(topic_parts) = topic_parts else {
        record_mqtt_rejection(
            state,
            &topic,
            producer_entity_id,
            feature_of_interest_id,
            raw_message.id,
            "invalid MQTT topic",
            config.connector.as_ref(),
            json!({
                "topic": topic,
                "reason": "invalid_topic"
            }),
        )?;
        return Ok(());
    };

    if !entity_exists(state, topic_parts.producer_entity_id)? {
        record_mqtt_rejection(
            state,
            &topic,
            Some(topic_parts.producer_entity_id),
            Some(topic_parts.feature_of_interest_id),
            raw_message.id,
            "MQTT message rejected because producer entity does not exist",
            config.connector.as_ref(),
            json!({
                "topic": topic,
                "reason": "producer_entity_not_found",
                "producer_entity_id": topic_parts.producer_entity_id,
                "feature_of_interest_id": topic_parts.feature_of_interest_id
            }),
        )?;
        return Ok(());
    }

    if !entity_exists(state, topic_parts.feature_of_interest_id)? {
        record_mqtt_rejection(
            state,
            &topic,
            Some(topic_parts.producer_entity_id),
            Some(topic_parts.feature_of_interest_id),
            raw_message.id,
            "MQTT message rejected because feature entity does not exist",
            config.connector.as_ref(),
            json!({
                "topic": topic,
                "reason": "feature_entity_not_found",
                "producer_entity_id": topic_parts.producer_entity_id,
                "feature_of_interest_id": topic_parts.feature_of_interest_id
            }),
        )?;
        return Ok(());
    }

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
                "UltraLight MQTT payloads require a stored producer PayloadProfile attribute_mapping",
                config.connector.as_ref(),
                json!({
                    "topic": topic,
                    "reason": "missing_mapping",
                    "payload_format": context.payload_format,
                    "producer_entity_id": topic_parts.producer_entity_id
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
                config.connector.as_ref(),
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
            metadata_with_connector(
                json!({
                    "topic": topic,
                    "ingest_source": "mqtt"
                }),
                config.connector.as_ref(),
            ),
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
        metadata_with_connector(
            json!({
                "topic": topic,
                "payload_format": context.payload_format,
                "observation_count": observations.len(),
                "ingest_source": "mqtt"
            }),
            config.connector.as_ref(),
        ),
    )?;
    mark_worker_ingest_success(state, config.connector.is_some());
    if let Some(connector) = config.connector.as_ref() {
        mark_connector_worker_ingest_success(state, connector.connector_id);
    }

    Ok(())
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
    configured_content_type: Option<&str>,
    connector: Option<&MqttConnectorMetadata>,
    ingest_reason: &str,
) -> Result<RawMessage, ApiError> {
    let payload_format = configured_payload_format
        .map(ToOwned::to_owned)
        .unwrap_or_else(|| "canonical-json".to_string());
    let content_type = configured_content_type.map(ToOwned::to_owned).or_else(|| {
        default_content_type_for_payload_format(&payload_format).map(ToOwned::to_owned)
    });
    let headers = metadata_with_connector(
        json!({
            "topic": topic,
            "protocol": "mqtt",
            "producer_entity_id": producer_entity_id,
            "feature_of_interest_id": feature_of_interest_id,
            "ingest_source": "mqtt",
            "reason": ingest_reason,
            "payload_format": payload_format,
            "source_endpoint": connector.map(|connector| connector.connector_key.clone()).unwrap_or_else(|| "mqtt".to_string()),
            "topic_or_path": topic,
        }),
        connector,
    );
    let raw_message = RawMessage::new(
        state.tenant_id,
        RawMessageSource::Mqtt,
        Some(topic.to_string()),
        producer_entity_id.map(|id| id.to_string()),
        Some(payload_format.clone()),
        content_type,
        producer_entity_id,
        feature_of_interest_id,
        Some(payload_format.clone()),
        headers,
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
    connector: Option<&MqttConnectorMetadata>,
    metadata: Value,
) -> Result<(), ApiError> {
    let message = message.into();
    state
        .storage
        .mark_raw_message_failed(state.tenant_id, raw_message_id, &message)?;
    mark_worker_ingest_failed(state, message.clone(), connector.is_some());
    if let Some(connector) = connector {
        mark_connector_worker_ingest_failed(state, connector.connector_id, message.clone());
    }
    record_ingest_event_optional(
        state,
        "aion:PayloadIngestionFailed",
        EventSeverity::Error,
        producer_entity_id,
        feature_of_interest_id,
        Some(raw_message_id),
        Some(message.to_string()),
        metadata_with_connector(
            json!({
                "topic": topic,
                "message": message,
                "ingest_source": "mqtt",
                "details": metadata
            }),
            connector,
        ),
    )?;
    Ok(())
}

fn record_mqtt_rejection(
    state: &AppState,
    topic: &str,
    producer_entity_id: Option<Uuid>,
    feature_of_interest_id: Option<Uuid>,
    raw_message_id: Uuid,
    message: impl Into<String>,
    connector: Option<&MqttConnectorMetadata>,
    metadata: Value,
) -> Result<(), ApiError> {
    let message = message.into();
    state
        .storage
        .mark_raw_message_failed(state.tenant_id, raw_message_id, &message)?;
    mark_worker_ingest_failed(state, message.clone(), connector.is_some());
    if let Some(connector) = connector {
        mark_connector_worker_ingest_failed(state, connector.connector_id, message.clone());
    }
    record_ingest_event_optional(
        state,
        "aion:MqttMessageRejected",
        EventSeverity::Warning,
        producer_entity_id,
        feature_of_interest_id,
        Some(raw_message_id),
        Some(message.to_string()),
        metadata_with_connector(
            json!({
                "topic": topic,
                "message": message,
                "ingest_source": "mqtt",
                "details": metadata
            }),
            connector,
        ),
    )?;
    Ok(())
}

fn entity_exists(state: &AppState, entity_id: Uuid) -> Result<bool, ApiError> {
    Ok(state
        .storage
        .get_entity(state.tenant_id, entity_id)?
        .is_some())
}

fn update_worker_state(
    state: &AppState,
    update: impl FnOnce(&mut MqttWorkerState),
    connector_worker: bool,
) {
    if connector_worker {
        return;
    }
    if let Ok(mut worker) = state.mqtt_state.write() {
        update(&mut worker);
    }
}

fn mark_worker_failure(state: &AppState, message: String, connector_worker: bool) {
    update_worker_state(
        state,
        |worker| {
            worker.connected = false;
            worker.subscribed = false;
            worker.last_error = Some(message);
        },
        connector_worker,
    );
}

fn mark_worker_ingest_success(state: &AppState, connector_worker: bool) {
    update_worker_state(
        state,
        |worker| {
            worker.last_successful_ingest_at = Some(Utc::now());
            worker.last_error = None;
        },
        connector_worker,
    );
}

fn mark_worker_ingest_failed(state: &AppState, message: String, connector_worker: bool) {
    update_worker_state(
        state,
        |worker| {
            worker.last_failed_ingest_at = Some(Utc::now());
            worker.last_error = Some(message);
        },
        connector_worker,
    );
}

fn record_mqtt_worker_event(
    state: &AppState,
    event_type: impl Into<String>,
    severity: EventSeverity,
    message: Option<String>,
    metadata: Value,
) -> Result<Event, ApiError> {
    record_ingest_event_optional(
        state, event_type, severity, None, None, None, message, metadata,
    )
}

fn metadata_with_connector(
    mut metadata: Value,
    connector: Option<&MqttConnectorMetadata>,
) -> Value {
    let Some(connector) = connector else {
        return metadata;
    };
    if let Some(object) = metadata.as_object_mut() {
        let connector_metadata = json!({
            "connector_id": connector.connector_id,
            "connector_key": connector.connector_key,
            "connector_profile": connector.connector_profile
        });
        object.insert("connector".to_string(), connector_metadata);
        object.insert("connector_id".to_string(), json!(connector.connector_id));
        object.insert("connector_key".to_string(), json!(connector.connector_key));
        object.insert(
            "connector_profile".to_string(),
            json!(connector.connector_profile),
        );
    }
    metadata
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
    fn parses_mqtt_config_defaults() {
        let config = MqttIngestConfig::from_env_values(MqttEnvValues::default()).unwrap();
        assert!(!config.enabled);
        assert_eq!(config.broker_url, DEFAULT_MQTT_BROKER_URL);
        assert_eq!(config.client_id, DEFAULT_MQTT_CLIENT_ID);
        assert_eq!(config.topic_filter, DEFAULT_MQTT_TOPIC_FILTER);
        assert_eq!(config.payload_format, None);
        assert_eq!(config.content_type, None);
        assert_eq!(config.username, None);
        assert_eq!(config.password, None);
        assert_eq!(config.connector, None);
    }

    #[test]
    fn parses_mqtt_config_with_username_and_password() {
        let config = MqttIngestConfig::from_env_values(MqttEnvValues {
            enabled: Some("true".to_string()),
            username: Some("worker".to_string()),
            password: Some("secret-password".to_string()),
            ..MqttEnvValues::default()
        })
        .unwrap();
        assert!(config.enabled);
        assert_eq!(config.username.as_deref(), Some("worker"));
        assert_eq!(config.password.as_deref(), Some("secret-password"));
    }

    #[test]
    fn mqtt_config_debug_redacts_password() {
        let config = MqttIngestConfig::from_env_values(MqttEnvValues {
            password: Some("secret-password".to_string()),
            ..MqttEnvValues::default()
        })
        .unwrap();
        let debug = format!("{config:?}");
        assert!(debug.contains("<redacted>"));
        assert!(!debug.contains("secret-password"));
    }

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
    fn rejects_invalid_mqtt_topic() {
        let err =
            parse_mqtt_topic("aioncore/not-a-uuid/data").expect_err("topic should be rejected");
        assert!(err.contains("four segments"));
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

    #[tokio::test]
    async fn rejects_mqtt_message_when_producer_entity_does_not_exist() {
        let state = AppState::local();
        let feature = insert_test_entity(&state, "feature", "aion:FeatureOfInterest");
        let producer = Uuid::new_v4();
        let topic = format!("aioncore/{producer}/{feature}/data");
        let config = test_config("senml-json");

        handle_publish(
            &state,
            &config,
            rumqttc::Publish::new(
                topic,
                QoS::AtLeastOnce,
                br#"{ "e": [ { "n": "temperature", "v": 20 } ] }"#.to_vec(),
            ),
        )
        .await
        .unwrap();

        let rejections = state
            .storage
            .query_events(
                state.tenant_id,
                EventFilter {
                    event_type: Some("aion:MqttMessageRejected".to_string()),
                    ..EventFilter::default()
                },
            )
            .unwrap();
        assert_eq!(rejections.len(), 1);
        assert!(rejections[0]
            .message
            .as_deref()
            .unwrap_or_default()
            .contains("producer entity does not exist"));

        let observations = state
            .storage
            .query_observations(state.tenant_id, None, None, None, None, 10)
            .unwrap();
        assert!(observations.is_empty());
    }

    #[tokio::test]
    async fn rejects_mqtt_message_when_feature_entity_does_not_exist() {
        let state = AppState::local();
        let producer = insert_test_entity(&state, "producer", "aion:Sensor");
        let feature = Uuid::new_v4();
        let topic = format!("aioncore/{producer}/{feature}/data");
        let config = test_config("senml-json");

        handle_publish(
            &state,
            &config,
            rumqttc::Publish::new(
                topic,
                QoS::AtLeastOnce,
                br#"{ "e": [ { "n": "temperature", "v": 20 } ] }"#.to_vec(),
            ),
        )
        .await
        .unwrap();

        let rejections = state
            .storage
            .query_events(
                state.tenant_id,
                EventFilter {
                    event_type: Some("aion:MqttMessageRejected".to_string()),
                    ..EventFilter::default()
                },
            )
            .unwrap();
        assert_eq!(rejections.len(), 1);
        assert!(rejections[0]
            .message
            .as_deref()
            .unwrap_or_default()
            .contains("feature entity does not exist"));

        let observations = state
            .storage
            .query_observations(state.tenant_id, None, None, None, None, 10)
            .unwrap();
        assert!(observations.is_empty());
    }

    fn test_config(payload_format: &str) -> MqttIngestConfig {
        MqttIngestConfig {
            enabled: true,
            broker_url: DEFAULT_MQTT_BROKER_URL.to_string(),
            client_id: DEFAULT_MQTT_CLIENT_ID.to_string(),
            topic_filter: DEFAULT_MQTT_TOPIC_FILTER.to_string(),
            payload_format: Some(payload_format.to_string()),
            content_type: None,
            username: None,
            password: None,
            connector: None,
        }
    }

    fn insert_test_entity(state: &AppState, key: &str, entity_type: &str) -> Uuid {
        let entity = Entity::new(
            state.tenant_id,
            key,
            entity_type,
            json!({
                "@context": "https://aioncore.dev/context",
                "@id": format!("urn:aion:test:{key}"),
                "@type": entity_type
            }),
            Utc::now(),
        )
        .unwrap();
        let entity = state.storage.create_entity(entity).unwrap();
        entity.id
    }
}
