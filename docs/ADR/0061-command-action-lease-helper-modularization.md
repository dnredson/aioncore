# ADR 0061: Command, Action, and Lease Helper Modularization

## Status

Accepted

## Context

Milestones 61 through 65 started a staged modularization of `apps/aion-api/src/lib.rs`:

- Milestone 61 extracted the auth foundation into `src/auth.rs`
- Milestone 62 extracted shared API error and response primitives into `src/error.rs`
- Milestone 63 extracted the Edge Adapter route group into `src/routes/adapters.rs`
- Milestone 64 extracted the auth/token route group into `src/routes/auth.rs`
- Milestone 65 extracted the executor route group into `src/routes/executors.rs`

After Milestone 65, the remaining risk for command-route extraction was not route registration itself. The main issue was that shared command, action, and lease support logic still lived in `apps/aion-api/src/lib.rs` and was reused by:

- generic command lifecycle and lease routes
- executor polling, claim, complete, and fail flows
- the SmartSentinel executor bridge
- command lease recovery and release logic

Moving command routes before isolating those shared helpers would increase circular-dependency pressure and make behavior-preserving extraction harder to review.

This milestone is intentionally behavior-preserving. Endpoint paths, auth semantics, tenant/resource ownership checks, command lifecycle semantics, executor behavior, SmartSentinel behavior, event types, and JSON shapes must remain unchanged.

## Decision

Extract the shared command/action/lease helper layer from `apps/aion-api/src/lib.rs` into:

- `apps/aion-api/src/command_support.rs`

Moved in this milestone:

- executor compatibility checks used by executor routes and the SmartSentinel bridge
- shared command claim and executor-mutation guards
- shared lease expiry, active-lease lookup, release, complete, and fail helpers
- shared command and lease event helpers
- shared raw command mutation helper used outside route-local command handlers
- shared executor-result metadata enrichment
- SmartSentinel command envelope assembly and SmartSentinel report metadata shaping

Intentionally kept in `lib.rs`:

- all command, lease, action, and action-result HTTP route handlers
- route registration and top-level app wiring
- auth and tenant/resource ownership checks performed directly in handlers
- generic route-local DTOs that are not yet shared across route groups
- shared app state and generic event-recording primitives
- centralized tests

## Consequences

Positive:

- shared command/action/lease logic now has a focused internal home
- future command-route extraction can depend on a smaller stable helper surface
- executor routes no longer depend on command/lease internals living directly in `lib.rs`
- behavior remains unchanged because storage calls, mutation rules, auth checks, and event metadata are preserved

Negative:

- `lib.rs` still contains the command and action route handlers for now
- some SmartSentinel-specific DTOs remain in `lib.rs` because moving route handlers is intentionally out of scope
- tests remain centralized to minimize churn during behavior-preserving refactors

## Rejected Alternatives

Extract command routes in the same milestone:

- rejected because the goal is to reduce shared-helper coupling first and keep the refactor easy to verify

Move all command- and action-adjacent DTOs immediately:

- rejected because several DTOs are still route-local and moving them now would broaden the change without improving reuse

Leave helper logic in `lib.rs` until full command-route extraction:

- rejected because the executor and SmartSentinel flows already demonstrate that these helpers are shared beyond one route group

## Behavior Preservation

Behavior is preserved because this milestone changes module boundaries only:

- no endpoint paths changed
- no request or response JSON shapes changed
- no auth scope requirements changed
- no tenant/resource ownership rules changed
- no command lifecycle or lease semantics changed
- no executor or SmartSentinel event metadata changed

## Future Route Extraction Plan

- extract the generic command and command-lease HTTP route group next, using `command_support.rs` as the shared internal dependency
- reassess whether any remaining action-route helpers should move once command handlers are isolated
- continue incremental route extraction instead of combining multiple high-risk surfaces in one step
