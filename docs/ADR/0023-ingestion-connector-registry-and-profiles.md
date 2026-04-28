# ADR 0023: Ingestion Connector Registry and Profiles

## Status

Accepted.

## Context

AionCore ingestion must support more than one fixed HTTP path or one MQTT broker/topic convention. Future deployments may ingest from controlled Mosquitto or EMQX brokers, public IoT brokers, The Things Network / The Things Stack, ChirpStack, AWS IoT MQTT, Kafka, HTTP webhooks, CoAP, LwM2M, LoRaWAN gateways, and custom sources.

The platform still needs to keep the MVP simple: HTTP ingestion must remain available, MQTT must remain disabled by default, and existing environment-variable MQTT behavior must keep working.

## Decision

Add an in-memory `IngestionConnector` registry and connector profile model.

An `IngestionConnector` describes where data comes from. A connector profile describes source-specific semantics such as AionCore MQTT topics, generic MQTT mappings, TTN v3 uplinks, or custom HTTP sources.

Supported connector types:

- `http`
- `mqtt`
- `future`

Supported connector profiles:

- `generic-aion-mqtt`
- `generic-mqtt`
- `ttn-v3`
- `custom`

The registry exposes CRUD-style read and enablement endpoints plus connector status. Connector-aware HTTP ingestion is added at:

```text
POST /ingestion/connectors/{connector_id}/ingest
```

This endpoint can resolve omitted request values from connector defaults and stores connector metadata with raw messages and ingestion events.

## Consequences

The existing `POST /ingest/http` endpoint remains unchanged and does not require a connector.

The existing environment-variable MQTT worker remains the default runtime MQTT connector behavior for now. Dynamic MQTT workers per connector are deferred.

`ttn-v3` connectors can be registered and documented, but TTN live connectivity and full uplink decoding are deferred. Future TTN behavior should map TTN device IDs to producer entities, map decoded payload fields to observations, preserve uplink metadata, and handle regional/application topic differences.

The registry is in-memory only in this milestone. Durable connector persistence, secrets storage, connector authentication, TLS/mTLS, MQTT per-device authorization, dashboard work, Cassandra, production MCP transport, and SmartSentinel integration remain out of scope.
