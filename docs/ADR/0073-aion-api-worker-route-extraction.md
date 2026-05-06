# ADR 0073: Aion API Worker Route Extraction

## Status

Accepted

## Context

After Milestone 75 extracted HTTP ingestion routes, Milestone 76 extracted generic connector administration, and Milestone 77 extracted TTN-specific mapping and validation routes, the remaining major connector-runtime operator surface in `apps/aion-api/src/lib.rs` was the ingestion worker HTTP surface:

- `GET /ingestion/workers/plan`
- `GET /ingestion/workers/status`
- `POST /ingestion/workers/reconcile`

Those endpoints had their own response DTOs and worker-route-specific response shaping helpers, but `lib.rs` still also contained the deeper runtime machinery for:

- connector worker planning inputs
- dynamic worker reconciliation
- MQTT worker startup and shutdown
- readiness integration
- connector mutation follow-up reconciliation

We want to continue route-level modularization without changing runtime behavior or prematurely moving intertwined runtime code.

## Decision

Extract the ingestion worker plan/status/reconcile route surface from `apps/aion-api/src/lib.rs` into `apps/aion-api/src/routes/workers.rs`.

Moved into `routes/workers.rs`:

- route registration for:
  - `GET /ingestion/workers/plan`
  - `GET /ingestion/workers/status`
  - `POST /ingestion/workers/reconcile`
- worker-route response DTOs:
  - runtime status projection
  - worker readiness projection
  - worker status response
  - worker reconcile response
  - reconcile action projection
- worker-route-specific response shaping helpers:
  - worker plan summary shaping for `/ready`
  - worker runtime status projection from a planned spec
  - worker status response shaping
  - worker readiness counters derived from route-visible worker states
  - reconcile action projection helper

`lib.rs` now merges `routes::workers::router()` and reuses the extracted worker-route DTOs and shaping helpers through `pub(crate)` visibility.

## What Intentionally Remains In `lib.rs`

- `IngestionWorkerPlan`, `IngestionWorkerSpec`, worker validation issues, and worker planning inputs shared by readiness, reconciliation, and tests
- connector worker planner decision logic
- connector worker reconciliation orchestration
- MQTT worker startup, restart, and stop behavior
- worker handle tracking and config-signature comparison
- worker event recording and connector mutation reconciliation hooks
- connector-worker enable flag state and startup initialization

These pieces remain because they are shared with runtime startup and reconciliation behavior, not only the HTTP route surface.

## Consequences

### Positive

- Reduces `lib.rs` by removing the remaining dedicated worker route registration and handlers.
- Isolates worker-route JSON projection concerns from runtime orchestration.
- Preserves the staged modularization pattern established by ingestion, connector-admin, and TTN route extraction.
- Makes later worker-support cleanup lower risk because the route layer is now separate from the runtime engine.

### Neutral / Preserved

- Endpoint paths are unchanged.
- Request and response JSON shapes are unchanged.
- Auth semantics are unchanged.
- `/ready` worker summaries are unchanged.
- Worker planner output is unchanged.
- Dynamic reconciliation behavior is unchanged.
- MQTT connector worker behavior is unchanged.
- TTN worker skip behavior is unchanged.

### Tradeoff

- Worker runtime and planner internals still remain in `lib.rs`, so this milestone improves route cohesion first and defers deeper support-module cleanup.

## Rationale

Worker routes were extracted after connector admin and TTN routes because they sit on top of the connector-runtime engine and share more runtime state than those earlier route groups. Extracting the route layer first keeps the behavior-preserving boundary small: operator-facing HTTP handlers, route-local DTOs, and response shaping moved, while the core planner and reconciliation engine stayed where it is.

That split keeps this milestone low risk and prepares a later cleanup milestone that can reassess which worker internals belong in a dedicated shared support module once route extraction is complete.

## Follow-up

- Review whether worker planner/reconciliation helpers still in `lib.rs` should move into a small shared worker support module.
- Continue modularization with future shared-support cleanup and planned historical time-series API work.
