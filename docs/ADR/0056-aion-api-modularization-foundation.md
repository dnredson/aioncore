# ADR 0056: Aion API Modularization Foundation

## Status

Accepted

## Context

`apps/aion-api/src/lib.rs` has accumulated a large mix of responsibilities:

- auth configuration and token resolution
- DTOs and response models
- route handlers
- storage orchestration
- ingestion and connector flows
- SmartSentinel integration
- MCP and AI-facing endpoints
- command, action, executor, and rule flows
- tests

Keeping all of that in one file increases change risk for unrelated work. Even small auth or route updates now require touching a very large compilation unit, which makes review harder and raises the chance of accidental behavioral changes.

Milestone 61 is intentionally a low-risk foundation milestone. It should reduce file size and create a first extraction pattern without changing runtime behavior, endpoint paths, auth semantics, or storage semantics.

## Decision

Extract the most self-contained auth code from `apps/aion-api/src/lib.rs` into a new module:

- `apps/aion-api/src/auth.rs`

Moved in this milestone:

- `AuthMode`
- `AuthEnforcementLevel`
- `AuthConfig`
- `PrincipalType`
- internal `Principal`
- internal `AuthContext`
- bootstrap admin token validation
- token hashing and parsing helpers
- token principal mapping helpers
- auth-context resolution helpers
- protected endpoint group constants
- generic auth helpers such as `require_authenticated`, `require_scope`, `require_any_scope`, and tenant/auth primitives that are still clearly auth-scoped

Kept in `lib.rs` for now:

- route handlers
- route registration and middleware wiring
- ingestion, connector, SmartSentinel, MCP, adapter, command, action, executor, and rule endpoint implementations
- route-adjacent tenant/resource lookup helpers such as `require_same_tenant_for_target_*`
- general API DTOs and response models not yet grouped cleanly enough for a safe move

The crate now re-exports only the auth types that are already part of the external crate surface and keeps the rest at `pub(crate)` visibility where possible.

## Consequences

Positive:

- `lib.rs` is smaller and less coupled for future work
- auth-specific code now has a clear home for incremental follow-up changes
- the extraction pattern is established without a broad route refactor
- review risk stays lower because route behavior remains in place

Negative:

- `lib.rs` remains large
- auth still depends on crate-local event logging and API error types
- DTOs, route group wiring, and route-adjacent authorization helpers are still centralized in `lib.rs`

## Rejected Alternatives

Perform a large route refactor now:

- rejected because this milestone is explicitly intended to stay small and behavior-preserving

Move all auth- and state-related helpers at once:

- rejected because some helpers remain tightly coupled to route-local resource loading and would make this milestone riskier than necessary

Introduce placeholder route modules without moving code:

- rejected for this milestone because placeholders alone would not materially reduce `lib.rs` risk

## Future Split Plan

Likely next modularization steps:

- extract route-adjacent auth/resource ownership helpers into a focused internal module once their dependencies are clearer
- split route handlers by area, for example `routes/auth`, `routes/entities`, `routes/commands`, `routes/connectors`, and `routes/mcp`
- group DTOs and response models by domain once route modules exist
- evaluate a later `state.rs` extraction only after the runtime state shape is stable enough to avoid churn
