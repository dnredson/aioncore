# ADR 0036: TTN Live Validation Preflight

## Status

Accepted

## Context

TTN v3 connector diagnostics can validate local configuration and produce a dry-run readiness plan, but operators still need an explicit way to prove that a configured connector can reach its MQTT broker and subscribe to the configured uplink topic.

This must not change normal runtime behavior. Startup, `/ready`, worker planning, reconciliation, and the default test suite must remain deterministic and non-network. Secret values must remain write-only, and live MQTT messages must not enter the normal ingestion pipeline during preflight.

## Decision

Add:

```text
POST /ingestion/connectors/{connector_id}/ttn-live-validate
```

The endpoint is limited to `connector_profile = "ttn-v3"`. It first runs the dry-run readiness plan. If `safe_to_connect = false`, it returns a structured skipped result with blockers and performs no network operation.

When the dry-run plan passes and `dry_run_only` is not set, the endpoint resolves the connector secret internally, creates a bounded MQTT client, connects to the configured broker, subscribes to the configured topic filter, optionally waits for one matching message, and disconnects. The default timeout is 5 seconds and the maximum accepted timeout is 60 seconds.

The preflight response reports connection/subscription/message flags, timing, redacted broker metadata, warnings, errors, dry-run summary, and `secret_exposed = false`. Received MQTT payloads are not returned, not stored, not decoded, and not converted into observations.

Events are emitted for started, succeeded, failed, and skipped preflights. Event metadata contains only connector identity, safe broker/topic metadata, result flags, issue codes, and no secrets or raw payloads.

## Consequences

Operators can opt in to a bounded live TTN MQTT check without enabling connector workers or ingesting data. Normal tests remain offline; the live test is ignored unless explicitly enabled with `AIONCORE_TEST_TTN_LIVE=1` and the documented TTN MQTT environment variables.

This milestone intentionally does not implement TTN downlinks, auto-provisioning, TLS/mTLS hardening, production secret management, dashboard support, or dynamic worker behavior changes. The preflight currently follows the existing plain `mqtt://host:port` MQTT foundation; production transport hardening remains a future milestone.
