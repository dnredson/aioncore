# ADR 0060: Aion API Executor Route Extraction

## Status

Accepted

## Context

`apps/aion-api/src/lib.rs` remained large after the earlier staged modularization work:

- Milestone 61 extracted the auth foundation into `src/auth.rs`
- Milestone 62 extracted shared API error and response primitives into `src/error.rs`
- Milestone 63 extracted the Edge Adapter route group into `src/routes/adapters.rs`
- Milestone 64 extracted the auth/token route group into `src/routes/auth.rs`

The executor HTTP surface was the next good extraction target because it is cohesive, heavily exercised by tests, and represents a distinct route group with route-local DTOs and handler logic. At the same time, some executor-related helpers are still shared with SmartSentinel command flows and the broader command/lease lifecycle logic, so a safe extraction needed to stop at the route boundary instead of trying to move all executor-adjacent internals at once.

This milestone is intentionally behavior-preserving. It should improve code organization without changing endpoint paths, JSON shapes, auth semantics, tenant/resource ownership behavior, executor lifecycle behavior, or command lease/retry semantics.

## Decision

Extract the executor route group from `apps/aion-api/src/lib.rs` into:

- `apps/aion-api/src/routes/mod.rs`
- `apps/aion-api/src/routes/executors.rs`

Moved in this milestone:

- route registration for:
  - `POST /executors`
  - `GET /executors`
  - `GET /executors/{executor_id}`
  - `PUT /executors/{executor_id}/heartbeat`
  - `PUT /executors/{executor_id}/capabilities`
  - `GET /executors/{executor_id}/capabilities`
  - `PUT /executors/{executor_id}/scopes`
  - `GET /executors/{executor_id}/scopes`
  - `GET /executors/{executor_id}/commands/pending`
  - `POST /executors/{executor_id}/commands/{command_id}/claim`
  - `POST /executors/{executor_id}/commands/{command_id}/complete`
  - `POST /executors/{executor_id}/commands/{command_id}/fail`
- executor-route DTOs used only by that HTTP surface
- executor read-path helper logic that stayed local to the new route module

Kept in `lib.rs` intentionally:

- top-level app construction, middleware wiring, and the rest of route registration
- shared app state and event-recording primitives
- command/action/lease mutation helpers reused by both executor routes and SmartSentinel executor flows
- executor compatibility and lease helpers that are still shared outside the extracted route module
- SmartSentinel executor registration/reporting handlers and their DTOs
- centralized tests, including executor coverage

## Consequences

Positive:

- executor handlers and route-local DTOs no longer enlarge `lib.rs`
- executor HTTP behavior is easier to review in one dedicated module
- the route extraction pattern now extends to another cohesive route group
- behavior remains unchanged because the same auth checks, tenant checks, storage calls, command mutations, and event metadata are preserved

Negative:

- `lib.rs` remains large because shared command/lease and SmartSentinel-related internals still live there
- executor support code is now split between `routes/executors.rs` and `lib.rs`
- tests remain centralized for now to minimize churn during behavior-preserving modularization

## Rejected Alternatives

Move all executor helpers into the new module:

- rejected because several helpers are still shared with SmartSentinel executor routes and command lease flows, and moving them now would broaden regression risk

Extract commands and executors together:

- rejected because this milestone is intentionally limited to one cohesive route group

Move executor tests in the same milestone:

- rejected because the existing tests already cover the intended behavior and leaving them in place keeps the change narrower

## Future Route Extraction Plan

- continue extracting remaining cohesive route groups such as commands, entities, connectors, and MCP into `apps/aion-api/src/routes/`
- reassess whether shared command/lease and authorization helpers should move into smaller internal modules once more route groups depend on them
- optionally move clearly route-owned tests nearer to extracted modules after the modularization pattern is stable
