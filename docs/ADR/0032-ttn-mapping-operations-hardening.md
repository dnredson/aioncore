# ADR 0032: TTN Mapping Operations Hardening

## Status

Accepted

## Context

TTN device mappings let AionCore resolve The Things Stack device/application IDs to existing domain entities. The first mapping milestone supported create, list, get, enable, disable, and ingestion-time lookup, but it did not expose general update/delete operations or rich resolution diagnostics.

Operators need to correct mappings without recreating connectors, remove stale mappings, and understand whether an uplink resolved by exact application match, fallback device match, or failed because no safe mapping existed.

## Decision

Add explicit update and delete operations for TTN device mappings:

- `PATCH /ingestion/connectors/{connector_id}/ttn-device-mappings/{mapping_id}`
- `DELETE /ingestion/connectors/{connector_id}/ttn-device-mappings/{mapping_id}`

Updates may change `ttn_application_id`, `ttn_device_id`, `producer_entity_id`, `feature_of_interest_id`, `enabled`, and `metadata`. Mapping identity remains immutable: `id`, `tenant_id`, and `connector_id`.

Enabled mappings are conflict-checked so:

- duplicate exact mappings for the same connector, device, and application are rejected;
- one application-specific mapping and one fallback mapping without application ID can coexist;
- duplicate enabled fallback mappings for the same connector/device are rejected.

Resolution prefers exact application matches over fallback device matches. Successful ingestion event metadata includes `ttn_mapping_id`, `mapping_resolution`, `ttn_device_id`, and `ttn_application_id`. Missing or ambiguous mapping failures preserve the raw message and include `ttn_device_id`, `ttn_application_id`, `connector_id`, and `mapping_resolution_error`.

PostgreSQL uses partial enabled-row uniqueness indexes for exact and fallback mappings. Application/storage logic still performs explicit checks so in-memory and PostgreSQL behavior remain aligned and API errors can be clear.

## Consequences

TTN mapping management is safer for local and durable runtimes, and resolution outcomes are observable through events without requiring a live TTN account or broker.

This milestone does not implement TTN entity auto-provisioning, downlinks, live broker validation, TLS/mTLS, dashboard behavior, Cassandra, production MCP transport, or domain-specific integrations.
