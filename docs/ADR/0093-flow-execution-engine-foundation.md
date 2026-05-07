# ADR 0093: Flow Execution Engine Foundation

## Status

Accepted

## Context

Milestones 82, 87, 92, 95, 96, and 97 established:

- a persisted flow graph model
- validation and dry-run APIs
- dashboard inventory and detail reads
- a form-based builder
- a read-only visual graph layer
- constrained visual draft editing
- typed node inspectors

What was still missing was a backend execution foundation that could interpret known node kinds against explicit input without changing runtime behavior or performing external side effects.

## Decision

Add a dedicated internal flow execution module and two explicit APIs:

- `POST /flows/execute`
- `POST /flows/{flow_id}/execute`

This milestone keeps execution strictly simulated:

- `simulated = true`
- `side_effects_performed = false`
- explicit request-only invocation
- no runtime subscription or sink dispatch

The engine now:

- validates the flow before execution
- resolves explicit input from `sample_payload` or tenant-owned `RawMessage`
- interprets supported source, decoder, transform, filter, rule, sink, and DLQ node kinds
- returns preview artifacts and per-node/per-sink execution results

## Consequences

Positive:

- AionCore now has a backend execution contract distinct from dry-run.
- Future real sink delivery can build on the same node and sink result model.
- Operators and tests can exercise flow logic safely without broker or HTTP side effects.
- Tenant-aware stored flow and raw-message execution behavior stays aligned with existing read protections.

Tradeoffs:

- execution semantics remain intentionally partial
- previews are useful but not yet authoritative runtime behavior
- branch semantics are still simple compared with a future full engine
- no dashboard execution UI is introduced yet

## Rejected Alternatives

### Reuse Dry-Run As The Execution Surface

Rejected because dry-run is planning-oriented and intentionally non-interpreting. Overloading it would blur the distinction between structural planning and input-driven node execution.

### Add Real Sink Delivery Immediately

Rejected because it would widen risk too early:

- MQTT publish
- HTTP forward
- command creation
- DLQ writes

Those remain future milestones after the safe execution contract is established.

### Wire Execution Into Flow Enablement Or Connector Workers

Rejected because this milestone must not modify ingestion behavior, connector worker behavior, or existing `/flows` runtime semantics.
