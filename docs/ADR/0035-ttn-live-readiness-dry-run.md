# ADR 0035: TTN Live Readiness Dry Run

## Status

Accepted

## Context

AionCore can validate TTN connector configuration and credential references without network access. Future live validation will need to connect to a TTN/The Things Stack MQTT broker, authenticate, and possibly verify topic subscription behavior. That future behavior must be opt-in and carefully gated because it involves credentials and external network dependencies.

Operators need a deterministic preview of what AionCore would check before any live attempt is allowed.

## Decision

Add a non-network dry-run endpoint:

```text
GET /ingestion/connectors/{connector_id}/ttn-live-readiness-plan
```

For TTN v3 connectors, the plan reports:

- `dry_run = true`
- `can_attempt_live_validation`
- `safe_to_connect`
- readiness
- checks
- blockers
- warnings
- required operator steps

Checks cover TTN connector profile, MQTT connector type, broker URL, topic filter presence and shape, `ttn-uplink-json` payload format, connector secret reference presence and resolution, `mqtt_basic_auth` compatibility, username presence, internal secret value presence, enabled TTN mappings, and an explicit `no_network_call_performed` pass check.

Missing broker URL, topic filter, compatible secret reference, payload format, or enabled TTN mappings are blockers for future live validation. Disabled TTN connectors are not safe to connect until enabled. Non-TTN connectors return a not-applicable plan.

The endpoint never performs DNS resolution, opens sockets, contacts TTN, authenticates, subscribes, validates real credentials, or exposes secret values.

## Consequences

Operators can prepare TTN connectors for future live validation while keeping the current system deterministic and local-testable. Future milestones can add an explicit opt-in live validation path that uses this dry-run plan as a prerequisite gate.

This milestone does not implement live TTN broker validation, TTN downlinks, entity auto-provisioning, TLS/mTLS, dashboard behavior, Cassandra, production MCP transport, or SmartSentinel integration.
