# ADR 0074: Connector, TTN, and Worker Support Cleanup

## Status

Accepted

## Context

Milestones 75 through 78 extracted the route layers for:

- HTTP ingestion
- generic connector administration
- TTN mapping and validation operations
- worker plan, status, and reconcile operations

After those route extractions, `apps/aion-api/src/lib.rs` still contained a shared support surface used by multiple route modules plus startup and readiness:

- connector lookup, secret existence checks, and connector lifecycle event helpers
- TTN device-mapping event recording and TTN topic-shape validation used outside a single route file
- worker planning DTOs, worker planner decisions, runtime reconciliation, and connector-worker status mutation helpers

That remaining support code was cohesive enough to extract, but broad enough that moving it earlier would have mixed route extraction with runtime support changes in the same milestone.

## Decision

Extract the shared connector, TTN, and worker support logic from `apps/aion-api/src/lib.rs` into focused internal modules:

- `apps/aion-api/src/connector_support.rs`
- `apps/aion-api/src/ttn_support.rs`
- `apps/aion-api/src/worker_support.rs`

Moved into `connector_support.rs`:

- connector lookup
- connector secret existence checks
- connector event metadata shaping
- connector lifecycle event recording

Moved into `ttn_support.rs`:

- shared TTN topic-shape plausibility helper
- TTN device-mapping event metadata shaping
- TTN device-mapping event recording

Moved into `worker_support.rs`:

- worker plan/spec/config/runtime enums and DTOs
- worker planner construction and validation issue shaping
- worker start-decision logic
- worker enable-flag support
- worker runtime status mutation helpers used by MQTT runtime code
- connector-worker signature comparison
- connector-worker reconciliation orchestration
- MQTT connector worker start/stop bookkeeping

`lib.rs` now re-exports only the shared items still needed by existing modules and startup.

## What Intentionally Remains In `lib.rs`

- top-level application state
- top-level router assembly
- readiness composition
- connector secret HTTP handlers
- generic ingestion decode and raw-message/event helpers
- generic entity/command/action/executor support already used across broader API surfaces
- centralized tests

The ingest decode/event helpers intentionally remain because they are still tightly coupled to raw-message storage, observation creation, and rule evaluation paths shared by HTTP and MQTT ingestion.

## Consequences

### Positive

- `lib.rs` is reduced further without changing endpoint ownership or runtime behavior.
- connector, TTN, and worker support now have clearer internal boundaries that match the route modules extracted in Milestones 76 through 78.
- worker runtime code is easier to reason about separately from the top-level API bootstrap path.
- future historical time-series query work and later dashboard-facing work can proceed with less pressure to touch connector/runtime support in the same file.

### Neutral / Preserved

- Endpoint paths are unchanged.
- Request and response JSON shapes are unchanged.
- Auth semantics are unchanged.
- Readiness behavior is unchanged.
- Connector lifecycle behavior is unchanged.
- TTN validation, mapping, readiness, and live-preflight behavior are unchanged.
- Worker planning, status, reconcile, and dynamic MQTT worker behavior are unchanged.

## Rationale

After route modularization, the next low-risk move was not more route splitting. It was extracting the shared support that those routes and startup code already depended on. Keeping the extraction focused on connector, TTN, and worker support avoided broader refactoring while still shrinking `lib.rs` and improving cohesion.

## Follow-up

- Keep historical observation/time-series query APIs as the next feature milestone.
- Reassess later whether generic ingest helper extraction is worthwhile after historical query and dashboard-facing work clarify the longer-term boundaries.
