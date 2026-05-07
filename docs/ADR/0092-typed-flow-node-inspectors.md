# ADR 0092: Typed Flow Node Inspectors

## Status

Accepted

## Context

Milestones 82, 87, 88, 89, 92, 93, 94, 95, and 96 establish the backend flow model, validation and dry-run APIs, dashboard flow inventory/detail reads, the no-build static dashboard, the form-based Flow Builder, browser-native module packaging, optional `/ui/*` static hosting, a read-only visual graph layer, and constrained visual draft editing for single-chain proposed flows.

That leaves a usability gap in the constrained builder:

- selected nodes can be renamed and retyped
- source and sink connectors can be chosen where supported
- node `config` is still mostly generic JSON

Before any flow execution exists, operators need clearer typed planning fields for common node kinds so draft intent is easier to understand, safer to edit, and easier to validate.

The milestone must remain:

- static frontend-only
- dependency-free
- limited to existing `/flows`, `/dashboard/flows`, and connector read APIs
- non-executing
- non-branching

## Decision

Extend `apps/aion-dashboard/` with typed node inspectors for selected draft nodes in the constrained Flow Builder.

Supported typed inspectors:

- source kinds: `mqtt_subscribe`, `http_input`, `ttn_uplink`, `internal_observation`
- decoder kinds: `senml_decode`, `ultralight_decode`
- transform and filter kinds: `canonical_json`, `json_map`, `filter_condition`
- rule kind: `threshold_rule`
- sink and DLQ kinds: `internal_observation_store`, `raw_message_store`, `mqtt_publish`, `http_forward`, `event_create`, `command_create`, `dlq`

Inspector behavior:

- edits update the selected draft node config
- linear draft edges are regenerated automatically as before
- redacted preview JSON stays visible
- existing validate, dry-run, and create actions continue to use the same backend APIs
- advanced JSON override still disables constrained visual editing until cleared

Stored-flow copy-to-draft remains limited to safely representable single-chain flows, but rejection messages are made more explicit for:

- branching graphs
- multiple sources
- multiple terminal sinks or DLQ terminals
- cycles
- missing source or terminal nodes
- unsupported node kinds or node types

## Rationale

### Why typed inspectors now

The constrained builder already gives operators a safe linear planning surface. Typed inspectors make that surface more understandable without committing to runtime execution or a more complex graph editor.

### Why execution is still deferred

The current validation and dry-run APIs are planning-oriented and explicitly non-executing. Preserving that boundary avoids accidental runtime semantics in a static frontend milestone.

### Why arbitrary graph editing is still deferred

Arbitrary graph editing would require broader decisions about:

- branching and merge semantics
- layout persistence
- patch vs replace update behavior
- more complex node-kind-specific authoring and validation UX
- execution/runtime boundaries for richer graphs

Typed inspectors improve the current safe linear workflow without forcing those broader decisions early.

### Why this still fits the future Node-RED-like direction

Typed inspectors are a bridge, not a replacement. They make common node kinds legible in the constrained builder while preserving the backend flow model and the eventual path to a richer Node-RED-like authoring surface.

## Consequences

Positive:

- draft configuration is clearer for common node kinds
- known config fields become safer than free-form JSON-only editing
- stored-flow copy failures become easier to understand
- backend API behavior remains unchanged

Trade-offs:

- only known node kinds receive typed inspectors
- unsupported node kinds still require advanced JSON or future builder work
- the builder remains intentionally linear and non-branching

## Non-Goals

This ADR does not introduce:

- flow execution
- broker subscriptions from flows
- MQTT publish execution
- HTTP forward execution
- observation, event, command, or DLQ writes from UI actions
- arbitrary graph editing
- drag-and-drop editing
- backend API changes
- frontend build tooling

## Follow-Up

- add richer typed validation hints for mapping and rule config
- expand typed inspector coverage when more node kinds become stable
- define clearer PATCH/update semantics before any in-place stored-flow graph editing
- revisit a broader Node-RED-like builder after execution and persistence boundaries are clearer
