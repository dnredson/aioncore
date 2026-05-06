# ADR 0083: Dashboard Flow Inventory And Detail API

## Status

Accepted

## Context

Milestone 82 introduced tenant-scoped flow storage and CRUD.

Milestone 87 introduced read-only flow validation and dry-run planning APIs.

A future AionCore dashboard needs flow inventory and detail payloads that are easier to render than raw `/flows` CRUD responses, but the platform still must not execute flows, subscribe to brokers, or expose secrets.

## Decision

Add two read-only dashboard endpoints:

- `GET /dashboard/flows`
- `GET /dashboard/flows/{flow_id}`

These endpoints:

- require `dashboard:read` in token mode
- reuse existing dashboard dev/disabled bypass behavior
- reuse existing tenant-aware flow read behavior
- remain additive and do not change `/flows`
- reuse the Milestone 87 analyzer for validation status, planned path, connector references, planned sinks, and secret redaction

`GET /dashboard/flows` returns compact inventory items only. It includes graph counts and validation summary fields, but it does not include full node configs.

`GET /dashboard/flows/{flow_id}` returns dashboard-oriented detail for a saved flow. It includes:

- flow metadata
- redacted nodes
- edges
- graph summary
- validation summary
- planned path
- referenced connectors
- planned sinks
- `execution_supported = false`
- `execution_status = "not_implemented"`
- `side_effects_performed = false`

## Consequences

Positive:

- future UI work can render flow inventory and graph detail without depending on raw CRUD payloads
- validation and dry-run semantics stay aligned because the same analysis/redaction path is reused
- token-mode tenant filtering and cross-tenant denial stay consistent with the existing flow read surface

Tradeoffs:

- dashboard overview and flow inventory now perform lightweight per-flow analysis at read time
- flow execution remains deferred, so dashboard detail is planning-oriented rather than operational

## Non-Goals

This ADR does not introduce:

- dashboard frontend UI
- drag-and-drop editing
- flow execution
- broker subscriptions
- MQTT publish or HTTP forward execution
- observation, event, command, or DLQ writes
- external AI calls
