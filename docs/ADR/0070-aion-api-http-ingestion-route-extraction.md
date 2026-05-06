# ADR 0070: aion-api HTTP ingestion route extraction

## Status

Accepted

## Context

Milestones 61 through 74 established the staged `aion-api` modularization pattern by first extracting shared auth and error foundations, then moving bounded route groups such as adapters, auth, executors, commands, SmartSentinel, MCP, AI context, provenance, events/raw-messages, observations, and entity-centered routes out of `apps/aion-api/src/lib.rs`.

After Milestone 74, `lib.rs` still contained the remaining HTTP ingestion route surface:

- `POST /ingest/http`
- `POST /ingestion/connectors/{connector_id}/ingest`

These endpoints are cohesive at the route level because they share request DTOs, HTTP-specific raw-message shaping, connector-default resolution, TTN-over-HTTP route behavior, and response shaping. They are also still entangled with broader connector registry, TTN mapping administration, worker planning, and runtime worker management code that is larger and riskier to move in the same milestone.

## Decision

Extract the HTTP ingestion route surface from `apps/aion-api/src/lib.rs` into:

- `apps/aion-api/src/routes/ingestion.rs`

Move into `routes/ingestion.rs`:

- route registration for `POST /ingest/http`
- route registration for `POST /ingestion/connectors/{connector_id}/ingest`
- the HTTP ingestion request/response DTOs used only by those endpoints
- HTTP-route-local helper logic for connector-aware request resolution, TTN uplink payload extraction for the connector-aware HTTP path, TTN mapping resolution for the HTTP path, raw-message-first HTTP ingestion flow, and response shaping

Keep in `lib.rs`:

- shared application state and top-level route assembly
- connector administration routes
- TTN device-mapping administration routes
- worker planning, status, and reconciliation routes
- broader connector registry and runtime management logic
- shared auth and tenant/resource ownership helpers
- shared decoder selection, payload-format helpers, connector/event metadata helpers, entity existence checks, and event-recording helpers still used outside the HTTP route module
- centralized tests

## Consequences

Positive:

- `lib.rs` continues shrinking through a narrow, behavior-preserving extraction.
- HTTP ingestion routes now live in a dedicated module with their route-local DTOs and helper flow.
- future connector-admin, TTN, and worker-route extraction can proceed independently without re-mixing the HTTP ingestion path.
- later historical observation/time-series API work can build on a cleaner separation between ingestion and read/query surfaces.

Neutral / intentional:

- No endpoint paths, auth semantics, tenant/resource ownership behavior, request/response JSON shapes, payload-decoder behavior, raw-message-first preservation behavior, rule-evaluation behavior, connector-registry behavior, or TTN-over-HTTP behavior changed.
- Dev/disabled-mode auth bypass, token-mode `ingestion:write` enforcement, and `admin:all` behavior remain unchanged.
- Shared helpers that are still used by MQTT ingestion, connector management, or other route groups intentionally remain in `lib.rs` with `pub(crate)` visibility where needed.
- Tests intentionally remain in `lib.rs` to avoid unnecessary churn during staged modularization.

## Future work

- extract connector administration routes in a separate milestone
- extract TTN device-mapping and TTN operational routes in a separate milestone
- extract worker planning/status/reconciliation routes in a separate milestone
- add historical observation/time-series query APIs without changing existing ingestion behavior
