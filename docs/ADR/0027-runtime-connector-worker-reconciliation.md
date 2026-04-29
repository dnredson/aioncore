# ADR 0027: Runtime Connector Worker Reconciliation

## Status

Accepted

## Context

Dynamic MQTT connector workers can start at API startup, but connector lifecycle changes after startup need to affect runtime workers without restarting `aion-api`.

The existing environment-variable MQTT worker must remain independent. Connector workers must remain disabled by default and TTN v3 decoding remains future work.

## Decision

Add an in-process connector worker manager that tracks connector MQTT worker tasks by `connector_id`. Add reconciliation that compares the current ingestion worker plan with tracked runtime workers.

Reconciliation is triggered after connector create, enable, and disable operations, and through:

```text
POST /ingestion/workers/reconcile
```

When `AIONCORE_CONNECTOR_WORKERS_ENABLED=false`, reconciliation does not start connector workers and stops any tracked connector worker. When enabled, reconciliation starts valid enabled `generic-aion-mqtt` and `generic-mqtt` MQTT connector workers, stops workers for disabled or invalid connectors, and restarts a worker when its planned runtime signature changes.

The runtime signature includes broker URL, client ID, topic filter, payload format, content type, and connector profile.

TTN v3 connector workers remain skipped with explicit status/events until TTN uplink decoding is implemented.

## Consequences

- Connector workers can respond to create/enable/disable lifecycle changes without process restart.
- `/ingestion/workers/status` now reports lifecycle fields including `started_at`, `stopped_at`, `restart_count`, and `last_reconciled_at`.
- Runtime state remains in memory and is rebuilt from storage and the worker plan on startup.
- The current public API has no general connector update endpoint, so config-change restart behavior is implemented in the reconciler but not yet reachable through a connector update route.

## Non-Goals

- TTN uplink decoding.
- Secrets storage.
- MQTT per-device authorization.
- TLS/mTLS.
- MQTT command publishing.
- Dashboard, Cassandra, production MCP transport, or SmartSentinel integration.
