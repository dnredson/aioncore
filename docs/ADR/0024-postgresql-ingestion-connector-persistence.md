# ADR 0024: PostgreSQL Ingestion Connector Persistence

## Status

Accepted.

## Context

ADR 0023 introduced the ingestion connector registry and connector profiles. The initial implementation kept connector storage in memory, which was enough for local tests but not enough for restart-safe connector configuration.

Future dynamic MQTT workers, connector-aware HTTP ingestion, and external broker profiles such as TTN v3 need durable connector records before runtime worker orchestration can be added.

## Decision

Add PostgreSQL persistence for `IngestionConnector`.

The migration set now includes `0007_create_ingestion_connectors.sql`, which creates the `ingestion_connectors` table with tenant scoping, connector key uniqueness per tenant, connector type/profile checks, JSONB metadata, and indexes for tenant, key, type, profile, and enabled state.

`PostgresStorage` now implements the `IngestionConnectorStore` behavior:

- create connector
- list connectors
- get connector by ID
- update connector, including enable/disable state

Connector status remains runtime-derived for now. Last error, last message time, last successful ingest time, and last failed ingest time are not persisted by this milestone.

## Consequences

In-memory storage remains the default runtime. PostgreSQL remains opt-in through the existing backend selection variables.

Connector records for `generic-aion-mqtt`, `generic-mqtt`, `ttn-v3`, and `custom` profiles can now persist in PostgreSQL. TTN v3 configuration can be stored durably, but TTN uplink decoding and live connectivity are still future work.

Secrets are not stored in connector records. Broker passwords, token material, TLS credentials, per-device MQTT authorization, dynamic MQTT worker startup, dashboard work, Cassandra, production MCP transport, and SmartSentinel integration remain out of scope.
