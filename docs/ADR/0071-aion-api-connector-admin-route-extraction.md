# ADR 0071: aion-api connector admin route extraction

## Status

Accepted

## Context

Milestones 61 through 75 established the staged `aion-api` modularization pattern by extracting shared auth and error foundations first, then moving bounded route groups such as adapters, auth, executors, commands, SmartSentinel, MCP, AI context, provenance, events/raw-messages, observations, entity-centered routes, and HTTP ingestion out of `apps/aion-api/src/lib.rs`.

After Milestone 75, `lib.rs` still contained the connector administration HTTP surface:

- `POST /ingestion/connectors`
- `GET /ingestion/connectors`
- `GET /ingestion/connectors/{connector_id}`
- `PATCH /ingestion/connectors/{connector_id}`
- `PUT /ingestion/connectors/{connector_id}/enable`
- `PUT /ingestion/connectors/{connector_id}/disable`
- `GET /ingestion/connectors/{connector_id}/status`

Those endpoints are cohesive because they share connector create/update DTOs, connector status response shaping, connector lifecycle event emission, and post-mutation worker reconciliation. They are also adjacent to, but meaningfully distinct from:

- HTTP connector-aware ingestion already extracted in `src/routes/ingestion.rs`
- TTN validation and live-readiness/live-validation routes
- TTN device-mapping administration routes
- worker plan, worker status, and worker reconciliation routes

Moving those broader TTN and worker surfaces in the same milestone would increase risk because they still depend on additional validation, mapping, and runtime-planning helpers.

## Decision

Extract the connector administration HTTP surface from `apps/aion-api/src/lib.rs` into:

- `apps/aion-api/src/routes/connectors.rs`

Move into `routes/connectors.rs`:

- route registration for the existing connector administration endpoints
- the create/update connector request DTOs used only by those routes
- the connector status response DTO used only by `GET /ingestion/connectors/{connector_id}/status`
- connector-admin-local helper logic for update-patch application, status response shaping, and runtime-state label mapping

Keep in `lib.rs`:

- shared application state and top-level route assembly
- HTTP ingestion routes in `src/routes/ingestion.rs`
- TTN validation, TTN live-readiness/live-validation, and TTN device-mapping routes
- worker plan, worker status, and worker reconciliation routes
- shared connector helpers still used outside connector admin, including connector lookup, connector event metadata, event recording, connector secret lookup, and worker reconciliation entrypoints
- centralized tests

## Consequences

Positive:

- `lib.rs` continues shrinking through a narrow, behavior-preserving extraction.
- connector administration routes now live in a dedicated module with their route-local DTOs and helpers.
- HTTP ingestion remains separated from connector administration, which keeps connector-aware payload ingestion concerns distinct from connector registry and lifecycle management.
- later TTN and worker modularization can proceed independently with cleaner boundaries.

Neutral / intentional:

- No endpoint paths, auth semantics, tenant/resource ownership behavior, request/response JSON shapes, connector storage behavior, connector lifecycle events, dynamic worker reconciliation behavior, or readiness behavior changed.
- Dev/disabled-mode auth bypass, token-mode `connectors:admin` and `connectors:read` enforcement, and `admin:all` behavior remain unchanged.
- TTN mapping, TTN validation/live operations, and worker-management routes intentionally remain in `lib.rs` to avoid mixing separate route groups into one milestone.
- Shared helpers still needed across route groups intentionally remain in `lib.rs` with `pub(crate)` visibility where necessary.
- Tests intentionally remain in `lib.rs` to minimize churn during staged modularization.

## Future work

- extract TTN device-mapping administration routes in a separate milestone
- extract TTN validation and live operational routes in a separate milestone
- extract worker planning, status, and reconciliation routes in a separate milestone
- continue adding historical observation/time-series APIs separately from this modularization work
