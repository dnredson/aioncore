# ADR 0072: Aion API TTN Route Extraction

## Status

Accepted

## Context

`apps/aion-api/src/lib.rs` had already been reduced by extracting:

- HTTP ingestion routes into `src/routes/ingestion.rs`
- generic ingestion connector administration routes into `src/routes/connectors.rs`

The remaining TTN-specific HTTP surface still lived in `lib.rs`:

- TTN device mapping CRUD and enable/disable routes
- TTN connector validation route
- TTN live-readiness dry-run route
- TTN live-validation preflight route

Those routes form a cohesive operator-facing surface with TTN-specific DTOs and helper logic, but they are still distinct from:

- generic connector administration
- generic HTTP ingestion
- ingestion worker plan/status/reconcile management

We want continued route-level modularization without changing runtime behavior.

## Decision

Extract the TTN-specific route surface from `apps/aion-api/src/lib.rs` into `apps/aion-api/src/routes/ttn.rs`.

Moved into `routes/ttn.rs`:

- route registration for:
  - `POST /ingestion/connectors/{connector_id}/ttn-device-mappings`
  - `GET /ingestion/connectors/{connector_id}/ttn-device-mappings`
  - `GET /ingestion/connectors/{connector_id}/ttn-device-mappings/{mapping_id}`
  - `PATCH /ingestion/connectors/{connector_id}/ttn-device-mappings/{mapping_id}`
  - `PUT /ingestion/connectors/{connector_id}/ttn-device-mappings/{mapping_id}/enable`
  - `PUT /ingestion/connectors/{connector_id}/ttn-device-mappings/{mapping_id}/disable`
  - `DELETE /ingestion/connectors/{connector_id}/ttn-device-mappings/{mapping_id}`
  - `GET /ingestion/connectors/{connector_id}/validate`
  - `GET /ingestion/connectors/{connector_id}/ttn-live-readiness-plan`
  - `POST /ingestion/connectors/{connector_id}/ttn-live-validate`
- TTN-only request/response DTOs
- TTN mapping response shaping
- TTN validation diagnostics helpers
- TTN credential and topic-shape diagnostics
- TTN live-readiness planning
- TTN live-validation preflight orchestration and response shaping
- TTN live-validation event helpers

`lib.rs` now merges `routes::ttn::router()`.

## What Intentionally Remains In `lib.rs`

- ingestion worker plan/status/reconcile routes
- worker runtime planning, reconciliation, and start/stop logic
- shared connector lookup and shared entity/secret existence helpers
- shared event recording helpers used by both TTN routes and other route/runtime code
- TTN device mapping event recording used by both TTN routes and connector-aware HTTP ingestion

## Consequences

### Positive

- Reduces `lib.rs` size while keeping the extraction bounded to one cohesive route surface.
- Preserves separation between:
  - connector admin operations
  - TTN-specific operator operations
  - ingestion worker management
- Makes the next worker-route extraction lower risk because the remaining `lib.rs` route surface is narrower and more focused.
- Keeps TTN production-hardening work localized for future milestones.

### Neutral / Preserved

- Endpoint paths are unchanged.
- Request and response JSON shapes are unchanged.
- Auth behavior is unchanged.
- Tenant/resource ownership behavior is unchanged.
- TTN mapping resolution and duplicate/conflict behavior are unchanged.
- TTN validation diagnostics, dry-run readiness behavior, and live preflight safety behavior are unchanged.

### Tradeoff

- Some shared helpers still remain in `lib.rs` because moving them now would increase extraction risk without improving cohesion enough in this milestone.

## Rationale

TTN mapping, validation, readiness, and live preflight belong together because they are all profile-specific operator routes with a shared diagnostic model and shared TTN safety constraints. They do not belong in the generic connector-admin module, and they should remain separate from worker management because worker planning/reconciliation is a broader runtime concern that will be extracted later on its own.

## Follow-up

- Extract ingestion worker plan/status/reconcile routes in a later milestone.
- Reassess whether any TTN support still shared with worker/runtime code should move into a small shared support module once worker extraction is complete.
