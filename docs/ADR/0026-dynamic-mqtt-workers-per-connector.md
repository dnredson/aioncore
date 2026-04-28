# ADR 0026: Dynamic MQTT Workers Per Connector

## Status

Accepted

## Context

AionCore has a persisted ingestion connector registry and a read-only worker planner. The next runtime step is to consume enabled MQTT connectors without replacing the existing environment-variable MQTT worker or making MQTT required by default.

TTN v3 connectors are modeled in the registry, but TTN uplink payload and topic semantics still require a dedicated adapter. Secrets storage, per-device authorization, and TLS/mTLS are also future work.

## Decision

Add opt-in connector workers controlled by:

```text
AIONCORE_CONNECTOR_WORKERS_ENABLED=true|false
```

The default is `false`. When disabled, no connector-based workers start and `/ingestion/workers/plan` remains available.

When enabled, startup reads the worker planner and starts one MQTT subscriber per valid enabled connector whose profile is `generic-aion-mqtt` or `generic-mqtt`. The existing `AIONCORE_MQTT_ENABLED` worker remains independent, so both worker families may run if both are enabled.

Connector workers use connector defaults for broker URL, client ID, topic filter, payload format, and content type. Raw messages and ingestion events include connector ID, key, and profile. Runtime state is kept in memory and exposed through:

```text
GET /ingestion/workers/status
```

`GET /ready` includes a connector-worker summary, but connector worker plan/status issues do not change the existing readiness contract for storage and the env-var MQTT worker.

TTN v3 connector workers are skipped with an explicit status/event because TTN decoding is not implemented yet.

## Consequences

- Dynamic MQTT ingestion remains safe by default because it is opt-in.
- Multiple generic MQTT connectors can be prepared for runtime consumption.
- Existing HTTP ingestion and env-var MQTT behavior remain unchanged.
- Connector runtime status is process-local and is not durable yet.
- Broker username/password credentials for connector workers are not implemented because secrets storage is not implemented.
- TTN v3 live consumption remains future work.

## Non-Goals

- TTN uplink decoding.
- Dynamic workers for non-MQTT connectors.
- Secrets storage.
- MQTT per-device authorization.
- TLS/mTLS.
- MQTT command publishing.
- Dashboard, Cassandra, production MCP transport, or SmartSentinel integration.
