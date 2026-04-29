# ADR 0034: TTN Credential Diagnostics

## Status

Accepted

## Context

TTN v3 connectors can reference connector secrets for MQTT authentication, and connector validation can warn when a public-looking TTN broker has no `secret_ref_id`. Operators still need more precise non-network feedback: whether the referenced secret exists, whether its type matches MQTT basic auth, whether a username is present, and whether a secret value exists internally.

The platform must keep secrets redacted and must not perform live broker or credential verification in this milestone.

## Decision

Extend TTN connector validation with credential diagnostics:

- `secret_configured`
- `secret_type`
- `operator_hints`
- `secret_ref_not_found`
- `incompatible_secret_type`
- `missing_secret_username`
- `missing_secret_value`

For TTN v3 connectors, `mqtt_basic_auth` is the compatible secret type for current MQTT worker authentication. Validation checks the referenced secret locally through storage only. It reports the secret type and shape diagnostics, but never returns `secret_value`.

Operator hints explain that public TTN/The Things Stack MQTT brokers usually require authentication, usernames are deployment/application-specific, passwords or API tokens should be stored as connector secrets, topic filters should follow the uplink topic shape, and no live credential verification is performed.

Non-TTN connector validation remains generic and does not include TTN-specific hints.

## Consequences

Operators get safer setup feedback before live MQTT validation exists. Secret values remain write-only through the API and absent from validation, readiness, worker status, event metadata, raw-message headers, and debug output.

This milestone does not implement live TTN broker validation, TTN downlinks, entity auto-provisioning, TLS/mTLS, dashboard behavior, Cassandra, production MCP transport, or SmartSentinel integration.
