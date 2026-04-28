# ADR 0022: MQTT Readiness and Broker Hardening

## Status

Accepted.

## Context

ADR 0021 introduced opt-in MQTT ingestion for the local runtime. The worker could ingest messages into the existing raw-message and canonical-observation flow, but operators needed clearer readiness, safer broker credential handling, and stricter topic/entity validation.

MQTT must remain disabled by default and must not change HTTP ingestion, storage backend behavior, or the default in-memory runtime.

## Decision

Harden the MQTT foundation without making MQTT required.

- Track MQTT worker state in process: enabled, connected, subscribed, broker URL, topic filter, last error, last message time, last successful ingest time, and last failed ingest time.
- Include MQTT state in `GET /ready`.
- Treat disabled MQTT as ready with `mqtt.enabled = false`.
- Treat enabled MQTT as ready only when connected and subscribed.
- Add optional `AIONCORE_MQTT_USERNAME` and `AIONCORE_MQTT_PASSWORD` for broker authentication.
- Redact the MQTT password from debug output.
- Validate MQTT topic shape and referenced producer/feature entities before creating observations.
- Preserve raw MQTT messages when feasible and emit rejection or failed-ingestion events for invalid messages.
- Keep SenML mapping-free and require a stored producer `PayloadProfile.attribute_mapping` for UltraLight MQTT payloads.

## Consequences

The API can remain available for HTTP traffic while `/ready` reports MQTT degradation when the optional worker cannot connect or subscribe.

Operators get lifecycle and message-level events:

- `aion:MqttWorkerStarted`
- `aion:MqttWorkerConnected`
- `aion:MqttWorkerSubscribed`
- `aion:MqttWorkerConnectionFailed`
- `aion:MqttMessageReceived`
- `aion:MqttMessageRejected`
- `aion:PayloadIngested`
- `aion:PayloadIngestionFailed`

Broker username/password support authenticates the AionCore worker to the broker only. Per-device authorization, TLS/mTLS, MQTT command publishing, production MCP transport, Cassandra, dashboard work, and SmartSentinel integration remain out of scope.
