# ADR 0059: Aion API Auth Route Extraction

## Status

Accepted

## Context

`apps/aion-api/src/lib.rs` remained large after the earlier modularization steps:

- Milestone 61 extracted the auth foundation into `src/auth.rs`
- Milestone 62 extracted shared API error/response primitives into `src/error.rs`
- Milestone 63 extracted the Edge Adapter route group into `src/routes/adapters.rs`

Even after those steps, the auth/token endpoints still lived in `lib.rs` alongside unrelated route groups, DTOs, and helpers. That kept one of the most security-sensitive route surfaces mixed into a broad file, which made focused review and later extraction harder than necessary.

This milestone is intentionally route-scoped and behavior-preserving. It should continue the route-level split without changing endpoint paths, auth semantics, token issuance/validation behavior, event metadata, or JSON response shapes.

## Decision

Extract the auth/token route group from `apps/aion-api/src/lib.rs` into:

- `apps/aion-api/src/routes/mod.rs`
- `apps/aion-api/src/routes/auth.rs`

Moved in this milestone:

- route registration for:
  - `GET /auth/whoami`
  - `POST /auth/tokens`
  - `GET /auth/tokens`
  - `GET /auth/tokens/{token_id}`
  - `POST /auth/tokens/{token_id}/revoke`
- auth-route-specific DTOs and response models:
  - token creation request
  - token record response
  - token creation response including one-time `raw_token`
  - whoami response
- route-adjacent helpers for:
  - token record response projection
  - token created event emission
  - token revoked event emission

Kept in `lib.rs` intentionally:

- top-level app construction, middleware wiring, and startup logic
- `/ready` and unrelated route groups
- shared app state and event-recording primitives
- auth acceptance/rejection/access-denied/scope-denied event helpers still used by `src/auth.rs`
- centralized tests, including auth/token coverage

Kept in `src/auth.rs` intentionally:

- auth configuration and mode handling
- bearer token parsing and hashing
- bootstrap admin token handling
- stored token validation and last-used updates
- shared scope and tenant authorization helpers

## Consequences

Positive:

- auth/token handlers now live in a dedicated route module that matches the existing extraction pattern
- auth route DTOs no longer enlarge `lib.rs`
- the separation between auth foundation logic and auth HTTP surface is clearer
- runtime behavior stays unchanged because the extraction preserves the same endpoint paths, storage calls, scope checks, and event metadata

Negative:

- `lib.rs` is still large because most remaining route groups have not moved yet
- some auth-related event helpers remain split between `lib.rs`, `src/auth.rs`, and `src/routes/auth.rs`
- tests remain centralized for now to avoid unnecessary churn during behavior-preserving extraction

## Rejected Alternatives

Move broader auth internals again in the same milestone:

- rejected because token validation, middleware behavior, and shared authorization helpers were already separated in Milestone 61 and changing them again would increase regression risk

Extract several unrelated route groups at once:

- rejected because the goal is a narrow route-level move with low review risk

Move `/ready` or other shared operational endpoints at the same time:

- rejected because they are not part of the auth/token route surface and would broaden the scope beyond this milestone

## Future Route Extraction Plan

- continue extracting cohesive route groups such as entities, commands, executors, connectors, and MCP into `apps/aion-api/src/routes/`
- reassess whether shared route support code should move into smaller internal modules once multiple route modules reuse the same helpers
- optionally relocate clearly route-owned tests nearer to extracted modules after the modularization pattern is stable
