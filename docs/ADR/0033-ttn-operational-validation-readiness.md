# ADR 0033: TTN Operational Validation And Readiness

## Status

Accepted

## Context

AionCore can register TTN v3 connectors, decode `ttn-uplink-json` payloads, and resolve devices through explicit mappings. Operators still need a deterministic way to inspect whether a TTN connector is plausibly configured before future live MQTT operation.

Live TTN validation would require network access, credentials, account-specific topics, TLS policy, and broker behavior that are intentionally outside the current milestone.

## Decision

Add a non-network connector validation endpoint:

```text
GET /ingestion/connectors/{connector_id}/validate
```

For `ttn-v3` connectors, validation reports connector identity, `valid`, `readiness`, issues, warnings, detected profile, expected topic shape, mapping counts, secret-reference presence, payload-format support, and generation time.

The validation checks are deterministic:

- TTN connectors must use `connector_type = mqtt`.
- `broker_url` should be present.
- `topic_filter` should be present and plausibly shaped like `v3/{application_id}/devices/+/up`.
- `payload_format` must be `ttn-uplink-json`.
- Missing or disabled TTN mappings are warnings.
- Public-looking TTN/The Things Stack broker URLs without `secret_ref_id` produce an authentication warning.
- Disabled connectors are degraded, not invalid solely because they are disabled.

Worker-plan diagnostics now include TTN topic-shape validation issues when applicable. `/ready` does not fail solely because TTN connector validation has warnings or issues; readiness remains focused on storage and runtime health.

Non-TTN connectors return the same response shape with a warning that profile-specific validation is not available.

## Consequences

Users can inspect TTN connector configuration, mapping coverage, and likely authentication gaps without a live TTN broker or account. This keeps local tests deterministic and preserves the explicit mapping model.

This milestone does not implement live TTN broker validation, TTN downlinks, entity auto-provisioning, TLS/mTLS, dashboard behavior, Cassandra, production MCP transport, or SmartSentinel integration.
