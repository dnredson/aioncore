# ADR 0057: Aion API Error and Response Modularization Foundation

## Status

Accepted

## Context

`apps/aion-api/src/lib.rs` still contains a broad mix of API responsibilities after the Milestone 61 auth extraction:

- route registration and middleware wiring
- route handlers across entities, observations, ingestion, connectors, MCP, SmartSentinel, adapters, commands, actions, rules, and executors
- DTOs and response models
- shared API error handling
- route-adjacent authorization and tenant checks
- tests

Before route groups can be split safely, they need a stable shared foundation for common API failures and HTTP error responses. Leaving `ApiError` and its JSON response formatting embedded in `lib.rs` would force each future route module to keep depending on a large central file for basic error behavior.

This milestone is intentionally narrow. It should reduce coupling for later route extraction without changing endpoint paths, response JSON shapes, auth behavior, or storage behavior.

## Decision

Extract the shared API error/response primitives from `apps/aion-api/src/lib.rs` into:

- `apps/aion-api/src/error.rs`

Moved in this milestone:

- `ApiError`
- `IntoResponse` for `ApiError`
- the internal HTTP error response body used for JSON error serialization
- the existing generic `ApiError` constructors for bad request, unauthorized, forbidden, not found, and SmartSentinel validation failures
- `From<StorageError>` for `ApiError`

Kept in `lib.rs` for now:

- all route handlers
- route registration and middleware wiring
- route-specific DTOs and response models
- ingestion, connector, SmartSentinel, MCP, adapter, command, action, executor, and rule flows
- route-adjacent authorization/resource lookup helpers
- tests

The SmartSentinel validation report remains defined in `lib.rs` because it is still used primarily by route-local SmartSentinel ingestion logic. Its visibility is widened only to `pub(crate)` so the new error module can preserve the existing validation error response shape.

## Consequences

Positive:

- future route modules can share `ApiError` without depending on the `lib.rs` implementation block where it used to live
- `lib.rs` shrinks further without a risky handler move
- common HTTP error serialization now has a dedicated home
- the extraction pattern for shared non-route API primitives is clearer before broader route modularization

Negative:

- `lib.rs` remains large and still owns most DTOs, helpers, and route logic
- the new `error` module still depends on crate-local SmartSentinel validation types
- route-level modularization is not yet complete

## Rejected Alternatives

Move route groups in the same milestone:

- rejected because the stated goal is a small behavior-preserving foundation change

Extract all DTOs and response models now:

- rejected because many are still domain- and route-specific, and moving them together would increase review and regression risk

Create both `error.rs` and `response.rs` immediately:

- rejected for now because one focused module is enough for the shared primitives currently being extracted

## Future Work

- split cohesive route groups into dedicated modules that depend on `error::ApiError`
- group route-specific DTOs and response models by domain once handlers move out of `lib.rs`
- reassess whether some route-adjacent auth/resource ownership helpers should move into smaller internal modules after route boundaries are clearer
