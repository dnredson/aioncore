# ADR 0082: Flow Validation And Dry-Run API

## Status

Accepted

## Context

Milestone 82 added a stored flow graph model and `/flows` CRUD.
Milestone 83 documented NiFi and MiNiFi as external reliable-flow runtimes.
Milestone 84 added a DLQ record foundation.
Milestones 85 and 86 added reliable ingestion and batch/backfill APIs.

The remaining gap was operator visibility before any future flow execution exists:

- a stored flow could be present but structurally unusable
- a future dashboard or Node-RED-like builder needs structured validation feedback
- operators need a read-only way to inspect what a flow would conceptually do without causing side effects

## Decision

Add additive validation and non-executing dry-run APIs for flows:

- `POST /flows/validate`
- `GET /flows/{flow_id}/validation`
- `POST /flows/dry-run`
- `POST /flows/{flow_id}/dry-run`

The milestone includes:

- reusable flow validation support
- structured validation issues with severity, code, message, and optional node, edge, and field references
- simple cycle detection
- isolated-node warnings
- connector reference checks when a `connector_id` can be safely verified
- recursive redaction of secret-like config keys
- planning-oriented dry-run responses that report node path, sinks, connector references, and conceptual side effects
- `flows:read` token-mode protection for validation and dry-run, with `POST /flows/validate` also accepting `flows:write`
- tenant-aware stored-flow validation and dry-run checks

## Non-Goals

This milestone does not add:

- flow execution
- MQTT subscribe or publish behavior from flows
- HTTP forwarding
- observation creation
- event creation
- command creation
- DLQ writes
- dashboard UI

## Consequences

Positive:

- future dashboard and flow-builder work now has a stable backend validation contract
- operators can inspect flow shape and intended sink behavior safely
- validation and dry-run stay aligned with the existing no-execution flow boundary
- secret-like config values stay redacted in planning output

Tradeoffs:

- dry-run is conceptual rather than payload-semantic execution
- connector verification is intentionally conservative and only performed when a `connector_id` can be safely checked
- cycle detection remains intentionally simple rather than a full scheduling/runtime planner
