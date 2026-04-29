# ADR 0031: TTN Device-To-Entity Mapping

## Status

Accepted

## Context

TTN v3 uplink JSON decoding can extract device and application identifiers, but AionCore canonical observations require existing producer and feature entity IDs. Automatically creating entities from TTN payloads would mix ingestion with provisioning and could create incorrect domain state.

## Decision

Add explicit `TtnDeviceMapping` rules scoped by tenant and connector. A rule links:

- optional `ttn_application_id`
- required `ttn_device_id`
- existing `producer_entity_id`
- optional `feature_of_interest_id`

Connector-aware TTN ingestion resolves entity IDs in this order:

1. IDs explicitly supplied by the ingestion request.
2. An enabled TTN device mapping, preferring exact application ID matches over connector/device fallback mappings.
3. Connector default entity IDs.

If no producer entity can be resolved, AionCore stores the raw message, marks it failed, emits `aion:TtnDeviceMappingMissing` and `aion:PayloadIngestionFailed`, and creates no observations.

## Consequences

TTN ingestion remains explicit and safe. The platform can decode sample TTN uplinks without live TTN connectivity or auto-provisioning while preserving a clear path for future provisioning workflows.

This milestone does not implement entity auto-provisioning, TTN downlinks, TLS/mTLS, live TTN account validation, or per-device MQTT authorization.
