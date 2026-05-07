# ADR 0090: Visual Flow Graph Layer

## Status

Accepted

## Context

Milestones 82, 87, 88, 89, 92, 93, and 94 establish the backend flow model, validation and dry-run planning APIs, dashboard-oriented flow read models, a static no-build dashboard shell, a form-based builder foundation, browser-native module packaging, and optional `aion-api` static hosting under `/ui`.

Operators now need the first Node-RED-like visual layer for flow inspection and preview, but the platform is not ready for:

- drag-and-drop authoring
- arbitrary graph editing
- persisted graph layout edits
- flow execution
- broker subscriptions driven by flows
- MQTT publish or HTTP forwarding
- observation, event, command, or DLQ creation from UI actions

The milestone must stay frontend-only, dependency-free, and aligned with the existing safe planning APIs.

## Decision

Add a read-only and preview-only visual flow graph layer to `apps/aion-dashboard/`.

The layer:

- renders stored flows from `GET /dashboard/flows/{flow_id}`
- renders proposed flows from the guided builder draft or advanced JSON override
- uses `POST /flows/validate` and `GET /flows/{flow_id}/validation` to show structured issue summaries and node-level issue markers when possible
- uses `POST /flows/dry-run` and `POST /flows/{flow_id}/dry-run` to show conceptual sink effects such as observation store, MQTT publish, HTTP forward, event create, command create, and DLQ use
- keeps all node detail panels read-only and redacted

Implementation uses inline SVG and existing browser APIs only. No graph library, CDN dependency, frontend build step, or backend change is introduced.

## Rationale

### Why read-only first

The existing backend contract is strong enough for safe inspection and planning, but not yet for arbitrary in-browser graph mutation or execution. A read-only layer lets operators understand stored and proposed flows without creating new runtime or persistence risks.

### Why no drag-and-drop yet

Drag-and-drop authoring would require decisions about:

- authoritative layout persistence
- unsaved graph diff handling
- arbitrary graph mutation constraints
- richer node-kind editing UX
- backend update semantics for partial graph edits

Those concerns are better handled after the inspection surface proves useful and after execution boundaries remain explicit.

### Why dependency-free SVG

The dashboard is intentionally:

- static
- no-build
- framework-free
- local-demo-friendly

Inline SVG fits those constraints while still supporting node boxes, edges, labels, issue badges, and click-to-select interaction.

### Why no execution or side effects

Validation and dry-run already provide safe planning semantics with `execution_supported = false` and `side_effects_performed = false`. The dashboard must preserve that safety boundary and must not become an execution path before the platform defines stronger runtime controls.

## Consequences

Positive:

- operators gain a visual mental model for flows without new backend risk
- proposed and stored flow inspection become more Node-RED-like
- validation and dry-run output become easier to interpret through node markers and effect highlights
- the existing form-based builder remains useful while richer editing is deferred

Negative:

- layout remains intentionally simple rather than optimal
- no panning, zooming, drag-and-drop, or arbitrary graph edits are available
- stored and proposed graphs still rely on existing API shapes and issue payload quality

## Follow-Up

Future milestones can build on this layer by adding:

- constrained visual editing for known linear patterns
- explicit layout persistence rules
- richer node-kind-specific inspectors
- deeper dry-run/simulation overlays
- eventual execution controls with separate security and operational guardrails
