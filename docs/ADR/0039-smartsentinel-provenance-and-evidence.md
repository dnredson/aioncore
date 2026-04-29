# ADR 0039: SmartSentinel Provenance And Evidence

## Status

Accepted

## Context

SmartSentinel snapshots can contain useful operational evidence: incident identifiers, alert identifiers, workflow runs, traces, logs, metrics, screenshots, reports, and URLs. AionCore should preserve these references for audit and explainability without becoming the SmartSentinel runtime or an evidence retrieval service.

Milestones 40 and 41 established optional snapshot ingestion, raw-message preservation, mapping into AionCore semantic records, validation, and reconciliation. Milestone 42 adds structured provenance and evidence metadata to that same optional path.

## Decision

Extend SmartSentinel snapshots with optional `source`, `provenance`, and `evidence` fields.

`source` may describe the agent or collector:

- `agent_id`
- `agent_version`
- `host_id`
- `environment`
- `collector`

`provenance` may describe the collection or workflow context:

- `run_id`
- `cycle_id`
- `trace_id`
- `correlation_id`
- `workflow_id`
- `external_refs`

`evidence` is an array of metadata-only references. AionCore preserves references such as logs, metrics, traces, packet captures, screenshots, reports, URLs, and command output, but does not fetch or validate external network resources.

Snapshot events may carry `incident_id`, `alert_id`, `workflow_id`, `run_id`, `trace_id`, and `evidence_refs`. Snapshot observations may carry `evidence_refs` and source references. These are stored in event and observation metadata. Evidence references related to materialized entities are also included in the SmartSentinel JSON-LD projection for those entities.

The snapshot response includes provenance summary fields:

- `provenance_present`
- `evidence_count`
- `external_ref_count`
- `correlation_id`
- `trace_id`
- `run_id`
- `cycle_id`

## Consequences

Positive:

- AionCore can explain why operational state was materialized.
- AI context queries can surface recent event and observation metadata containing incident, alert, trace, run, and evidence references.
- Raw snapshots remain the full source of truth.
- Evidence URL handling remains safe because URLs are preserved but never fetched.

Negative:

- Evidence references are metadata only; there is no first-class evidence table yet.
- Evidence schema validation is intentionally light.
- Consumers must follow external evidence references themselves if authorized outside AionCore.

## Non-Goals

- No SmartSentinel runtime.
- No polling.
- No recovery action execution.
- No evidence URL fetching.
- No authentication.
- No dashboard, Cassandra adapter, production MCP transport, or external AI integration.
