# ADR 0058: Aion API Adapter Route Extraction

## Status

Accepted

## Context

`apps/aion-api/src/lib.rs` remains large after the Milestone 61 auth extraction and the Milestone 62 error/response extraction. The next safe modularization step is to start moving a bounded route group without changing runtime behavior.

The Edge Adapter API is a good first target because it is cohesive and self-contained:

- it owns a small, explicit endpoint set
- it has route-local DTOs
- it has route-adjacent entity projection helpers
- it has route-adjacent event emission helpers
- it already has focused tests covering registration, lookup, heartbeat, status projection, auth scopes, and dev-mode bypass

This milestone is intentionally behavior-preserving. It should improve code organization without changing endpoint paths, JSON shapes, auth semantics, tenant/resource behavior, or storage behavior.

## Decision

Extract the Edge Adapter route group from `apps/aion-api/src/lib.rs` into:

- `apps/aion-api/src/routes/mod.rs`
- `apps/aion-api/src/routes/adapters.rs`

Moved in this milestone:

- route registration for:
  - `POST /adapters`
  - `GET /adapters`
  - `GET /adapters/{adapter_id}`
  - `PUT /adapters/{adapter_id}/heartbeat`
  - `GET /adapters/{adapter_id}/status`
- adapter-only request/response DTOs
- adapter route handlers
- adapter route-adjacent helpers for:
  - adapter record/entity lookup
  - `aion:EdgeAdapter` entity projection upsert
  - adapter status report construction
  - adapter event emission for registration, heartbeat, and status-change flows

Kept in `lib.rs` for now:

- top-level app construction and most route registration
- shared app state, middleware wiring, and startup logic
- shared event recording primitives reused outside adapters
- unrelated route groups and their DTOs/helpers
- existing tests, including adapter tests

## Consequences

Positive:

- route-level modularization begins with a narrowly scoped, well-tested surface
- adapter-specific DTOs and helpers no longer enlarge `lib.rs`
- later route extractions can follow the same `routes/<domain>.rs` pattern
- endpoint behavior remains unchanged because the extraction keeps the same handlers, auth checks, storage calls, entity projection, and event metadata logic

Negative:

- `lib.rs` is still large because this milestone moves only one route group
- tests remain centralized for now, so route ownership is not yet fully localized
- some shared primitives still live in `lib.rs` until additional extractions justify moving them

## Rejected Alternatives

Extract several route groups at once:

- rejected because it would broaden regression risk and make behavior-preservation harder to verify

Move adapter tests into the new route module in the same milestone:

- rejected for now because the existing tests already cover the required behavior and leaving them in place minimizes churn

Move shared event infrastructure out of `lib.rs` immediately:

- rejected because only the adapter-specific event helpers needed to move for this milestone; broader event utility extraction can happen later if multiple route modules need it

## Future Work

- continue extracting other bounded route groups such as connectors, executors, or commands into `apps/aion-api/src/routes/`
- reassess whether shared route infrastructure should move into smaller internal modules once several route groups depend on it
- optionally relocate route-focused tests nearer to their modules after the extraction pattern is stable
