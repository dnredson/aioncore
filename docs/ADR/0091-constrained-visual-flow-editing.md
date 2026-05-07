# ADR 0091: Constrained Visual Flow Editing

## Status

Accepted

## Context

Milestones 82, 87, 88, 89, 92, 93, 94, and 95 establish the flow model, validation and dry-run APIs, dashboard flow inventory and detail reads, the no-build static dashboard, a form-based builder foundation, browser-native module packaging, optional `/ui/*` static hosting, and a first read-only visual graph layer.

Operators now need a small step toward a future Node-RED-like builder, but the platform is still not ready for:

- arbitrary drag-and-drop authoring
- arbitrary edge creation
- arbitrary branching
- persisted graph layout editing
- stored-flow graph mutation in place
- flow execution
- broker subscriptions driven by flows
- MQTT publish or HTTP forwarding from UI actions
- observation, event, command, or DLQ creation from UI actions

The milestone must remain static frontend-only, dependency-free, and limited to safe known patterns.

## Decision

Extend `apps/aion-dashboard/` with constrained visual editing for proposed builder drafts only.

The draft graph stays limited to a single linear pattern:

- `source`
- zero or more `decoder`, `transform`, `filter`, or `rule` nodes
- `sink` or `dlq`

Allowed visual edit operations:

- select a draft node
- edit selected node name
- edit selected node kind from allowed options for that node type
- edit selected node `connector_id` through the existing connector selector when the node kind supports it
- add one safe middle node of type `transform`, `filter`, or `rule`
- remove a selected middle node
- reorder middle nodes within the linear chain
- regenerate edges automatically after every draft change

Stored flows remain read-only in the graph detail view. If a stored flow matches the constrained linear pattern and uses supported node kinds, the UI may copy it into a proposed builder draft for safe editing.

The implementation continues to use:

- existing `/flows`, `/flows/validate`, and `/flows/dry-run` APIs
- existing `/dashboard/flows*` reads
- existing `/ingestion/connectors` reads
- inline SVG and browser-native ES modules only

## Rationale

### Why editing is constrained

The current safe planning path is strong for simple linear drafts, but not yet for arbitrary graph mutation. Restricting edits to a known pattern keeps the UI understandable, keeps edge generation deterministic, and avoids new backend semantics.

### Why arbitrary drag-and-drop is deferred

Arbitrary graph editing would require decisions about:

- layout persistence
- graph diff and undo behavior
- partial update semantics
- branching validation UX
- richer node-kind-specific configuration surfaces
- clearer runtime and execution boundaries

Those decisions should follow a proven safe draft workflow rather than be bundled into a static frontend milestone.

### Why stored flows stay read-only

Stored flows are operational records. Allowing direct graph mutation from the dashboard would blur the line between inspection and update behavior, especially before a clear PATCH workflow and richer graph edit constraints exist.

### Why execution stays out of scope

Validation and dry-run already provide planning semantics with `execution_supported = false` and `side_effects_performed = false`. This milestone preserves that boundary and does not create a hidden execution path in the UI.

## Consequences

Positive:

- proposed drafts become easier to refine without introducing arbitrary graph editing
- operators can inspect and adjust linear graph order visually
- the current read-only graph layer evolves toward a future Node-RED-like builder with lower risk
- backend API behavior remains unchanged

Trade-offs:

- editing is intentionally limited to supported linear patterns
- advanced JSON override remains text-based and temporarily disables constrained visual editing
- stored-flow copy-to-draft works only for safe supported graph shapes and kinds

## Non-Goals

This ADR does not introduce:

- drag-and-drop graph editing
- arbitrary graph branching
- arbitrary edge creation
- stored-flow graph mutation in place
- flow execution
- broker subscriptions
- MQTT publish or HTTP forward execution
- observation, event, command, or DLQ writes
- frontend build tooling
- external graph libraries or CDNs

## Follow-Up

- add richer typed inspectors for supported node kinds
- expand constrained copy-to-draft handling where low risk
- define explicit stored-flow patch workflows before any in-place visual editing
- revisit broader Node-RED-like graph authoring after execution and persistence boundaries are clearer
