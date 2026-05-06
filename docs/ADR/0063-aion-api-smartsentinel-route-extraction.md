# ADR 0063: Aion API SmartSentinel Route Extraction

## Status

Accepted

## Context

Milestones 61 through 67 established the staged `aion-api` modularization pattern:

- shared auth moved to `src/auth.rs`
- shared API error handling moved to `src/error.rs`
- Edge Adapter routes moved to `src/routes/adapters.rs`
- auth/token routes moved to `src/routes/auth.rs`
- executor routes moved to `src/routes/executors.rs`
- shared command/action/lease helpers moved to `src/command_support.rs`
- generic command/action/action-result/lease routes moved to `src/routes/commands.rs`

After Milestone 67, the largest remaining route-local block in `apps/aion-api/src/lib.rs` was the SmartSentinel HTTP surface:

- snapshot ingestion
- snapshot validation and mapping helpers
- SmartSentinel executor bridge registration, polling, claim, and report handlers
- SmartSentinel-only DTOs

At the same time, Milestone 66 had already isolated the shared command/reporting internals used by both generic executor routes and the SmartSentinel bridge. That made SmartSentinel the next low-risk route extraction target.

The main requirement for this milestone is behavior preservation. Endpoint paths, auth semantics, tenant/resource ownership checks, raw-message-first ingestion, entity/relationship/observation/event mapping, provenance/evidence preservation, executor bridge lifecycle behavior, event metadata, and JSON shapes must remain unchanged.

## Decision

Extract the SmartSentinel route group from `apps/aion-api/src/lib.rs` into:

- `apps/aion-api/src/routes/smartsentinel.rs`

Update route registration so `lib.rs` merges:

- `routes::smartsentinel::router()`

Moved in this milestone:

- route registration for `/integrations/smartsentinel/snapshots`
- route registration for `/integrations/smartsentinel/executors/register`
- route registration for `/integrations/smartsentinel/executors/{executor_id}/commands`
- route registration for `/integrations/smartsentinel/executors/{executor_id}/commands/{command_id}/claim`
- route registration for `/integrations/smartsentinel/executors/{executor_id}/commands/{command_id}/report`
- SmartSentinel snapshot request/response and validation DTOs
- SmartSentinel executor bridge DTOs
- SmartSentinel snapshot validation, mapping, provenance, evidence, and executor-bridge helper logic

Intentionally kept in `lib.rs`:

- shared `AppState`, `record_event`, `EventDraft`, and storage-facing primitives
- shared tenant/resource ownership helpers
- shared ingestion helpers reused beyond SmartSentinel
- shared command/action/lease helpers already extracted in `src/command_support.rs`
- centralized tests

## Consequences

Positive:

- `lib.rs` loses another large integration-specific route block without changing runtime behavior
- SmartSentinel HTTP behavior now has a focused module boundary
- SmartSentinel continues to reuse `command_support.rs` for shared command envelope and reporting compatibility
- future route extraction work can proceed with smaller, more isolated diffs

Negative:

- several shared helpers in `lib.rs` remain `pub(crate)` because the new module depends on existing ingestion, rule-evaluation, and event-recording primitives
- tests remain centralized to avoid churn in a behavior-preserving milestone
- SmartSentinel query filtering over events/raw messages/AI context remains in `lib.rs` because that logic is shared with broader read surfaces rather than the SmartSentinel write routes

## Rejected Alternatives

Move SmartSentinel query/filter helpers in the same milestone:

- rejected because those helpers participate in broader event/raw-message/provenance read paths and are not a cohesive write-route group

Move command-support compatibility helpers out of `src/command_support.rs` again:

- rejected because Milestone 66 already established that those helpers are shared by generic executor flows and the SmartSentinel bridge

Extract multiple unrelated route groups at once:

- rejected because the staged modularization plan deliberately limits each milestone to one cohesive route-level boundary

## Behavior Preservation

Behavior is preserved because the change is limited to module boundaries and route registration:

- endpoint paths are unchanged
- request and response JSON shapes are unchanged
- dev-mode bypass and token-mode scope checks are unchanged
- tenant/resource ownership behavior is unchanged
- raw snapshot preservation as `RawMessage` is unchanged
- entity update/reuse semantics are unchanged
- relationship de-duplication is unchanged
- observation and event materialization is unchanged
- provenance/evidence metadata shaping is unchanged
- `uri_fetch_attempted = false` remains unchanged
- SmartSentinel executor registration, polling, claim, and report flows are unchanged
- action and action-result creation from SmartSentinel reports is unchanged

## Compatibility Notes

This extraction relies on the compatibility boundary introduced in Milestone 66:

- `src/command_support.rs` still owns shared SmartSentinel command envelope assembly and SmartSentinel report metadata shaping
- `src/routes/smartsentinel.rs` now owns the SmartSentinel-specific HTTP handlers and DTOs that call into those shared helpers

This keeps the SmartSentinel bridge behavior identical while reducing the amount of SmartSentinel-specific code left in `lib.rs`.
