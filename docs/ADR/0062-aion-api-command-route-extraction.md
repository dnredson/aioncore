# ADR 0062: Aion API Command Route Extraction

## Status

Accepted

## Context

Milestones 61 through 66 intentionally split `apps/aion-api/src/lib.rs` in low-risk stages:

- Milestone 61 extracted the auth foundation into `src/auth.rs`
- Milestone 62 extracted shared API error and response primitives into `src/error.rs`
- Milestone 63 extracted the Edge Adapter route group into `src/routes/adapters.rs`
- Milestone 64 extracted the auth/token route group into `src/routes/auth.rs`
- Milestone 65 extracted the executor route group into `src/routes/executors.rs`
- Milestone 66 extracted shared command/action/lease internals into `src/command_support.rs`

After Milestone 66, the remaining generic command surface in `lib.rs` was mostly HTTP glue:

- generic command lifecycle routes
- command lease routes
- generic action routes
- generic action-result routes
- route-local DTOs used only by those endpoints

The main requirement for this milestone is behavior preservation. Endpoint paths, auth semantics, tenant/resource ownership checks, command lifecycle behavior, lease/retry semantics, event metadata, executor compatibility, SmartSentinel bridge behavior, and JSON shapes must remain unchanged.

## Decision

Extract the generic command/action/lease HTTP route group from `apps/aion-api/src/lib.rs` into:

- `apps/aion-api/src/routes/commands.rs`

Update route registration so `lib.rs` merges:

- `routes::commands::router()`

Moved in this milestone:

- route registration for `/commands`, `/commands/:command_id*`, `/actions*`, and `/action-results*`
- generic command route handlers
- command lease route handlers
- generic action route handlers
- generic action-result route handlers
- route-local DTOs used only by those generic routes

Intentionally kept in `lib.rs`:

- shared `AppState`, `record_event`, and `EventDraft` primitives
- rule-engine command generation and policy evaluation helpers
- SmartSentinel integration handlers and DTOs
- shared tenant/resource ownership helpers
- centralized tests

## Consequences

Positive:

- `lib.rs` loses another large route-local block without changing runtime behavior
- generic command, action, and lease HTTP behavior now has a focused route module
- `command_support.rs` remains the shared internal layer for executor routes, SmartSentinel flows, and generic command routes
- future route extraction work can proceed with smaller diffs and clearer ownership boundaries

Negative:

- some helper visibility in `lib.rs` remains `pub(crate)` because the new route module still depends on existing tenant/auth and policy helpers
- tests remain centralized to avoid churn in a behavior-preserving milestone
- SmartSentinel-specific command reporting remains in `lib.rs`, so command-adjacent behavior is not fully isolated yet

## Rejected Alternatives

Move executor and SmartSentinel command flows into the same module:

- rejected because those flows already depend on `command_support.rs` and broadening the write scope would increase review risk without helping Milestone 67

Leave command/action DTOs in `lib.rs` and only move handlers:

- rejected because those DTOs are route-local and moving them with the handlers keeps the modularization boundary coherent

Extract additional unrelated route groups in the same milestone:

- rejected because the milestone is explicitly scoped to generic command/action/lease routes only

## Behavior Preservation

Behavior is preserved because the change is limited to module boundaries and route registration:

- endpoint paths are unchanged
- request and response JSON shapes are unchanged
- auth scope checks and dev-bypass behavior are unchanged
- tenant/resource ownership checks are unchanged
- command approval, claim, cancel, execute, fail, lease, and retry behavior are unchanged
- action and action-result creation/read behavior is unchanged
- executor and SmartSentinel compatibility is unchanged because shared internals still come from `command_support.rs`

## Compatibility Notes

Executor compatibility was kept by leaving executor-specific claim and completion flows in `src/routes/executors.rs` and continuing to use the shared helpers in `src/command_support.rs`.

SmartSentinel compatibility was kept by leaving the SmartSentinel bridge handlers in `lib.rs` and preserving their existing dependency on `src/command_support.rs` instead of routing them through the new generic command module.
