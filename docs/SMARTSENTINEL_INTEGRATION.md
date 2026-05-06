# SmartSentinel Integration

SmartSentinel is an optional operational integration for AionCore. It is not a core dependency.

AionCore remains domain-agnostic. SmartSentinel can participate as an observer, executor, or evidence source in operational and infrastructure monitoring deployments.

For deployments that also use NiFi or MiNiFi for reliable transport, replay, or store-and-forward, use the shared external provenance conventions documented in [NiFi Integration Model](NIFI_INTEGRATION_MODEL.md).

## Integration Roles

### Optional Observer

SmartSentinel can observe infrastructure, services, jobs, incidents, dependencies, and operational state.

AionCore can ingest SmartSentinel snapshots as raw messages. Relevant snapshot elements can then be materialized as:

- Entities.
- Relationships.
- Observations.
- Events.

Examples:

- Service entity from a monitored application.
- Dependency relationship from service A to database B.
- Observation that service health is degraded.
- Event that a deployment failed.
- Event that an alert opened.

### Optional Executor

SmartSentinel can also act as an executor when explicitly configured and policy-approved.

Relevant execution records can be materialized as:

- Commands.
- Actions.
- ActionResults.

Examples:

- Command: run diagnostic for service S.
- Action: SmartSentinel diagnostic workflow started.
- ActionResult: diagnostic completed with summary and evidence references.
- Command: restart service S.
- ActionResult: remediation failed due to permission error.

Critical remediation must not be automatic by default. AionCore should require explicit capability, policy, and authorization design before allowing SmartSentinel or any integration to execute critical actions.

## Relationship To Reliable Flow Engines

SmartSentinel and NiFi/MiNiFi solve different problems and can coexist:

- SmartSentinel focuses on operational context, evidence, and optional executor workflows.
- NiFi and MiNiFi focus on transport reliability, buffering, retry, routing, and replay.
- AionCore remains the semantic destination and audit surface.

## Snapshot Ingestion

SmartSentinel snapshots can be ingested through the same payload-agnostic raw message path as other integrations.

Milestone 40 added the first optional ingestion skeleton. Milestone 41 hardened reconciliation and diagnostics. Milestone 42 adds provenance and evidence references:

```text
POST /integrations/smartsentinel/snapshots
```

The endpoint accepts a simplified SmartSentinel/DATUM-like snapshot JSON document and stores it as a raw message before any mapping is attempted.

Raw message metadata:

- `protocol`: `http`.
- `payload_format`: `smartsentinel-snapshot-json`.
- `source_type`: `http`.
- `source_ref`: `/integrations/smartsentinel/snapshots`.
- `connector_profile`: `smartsentinel` in raw-message headers only; this does not make SmartSentinel a required connector dependency.
- `received_at`: AionCore receive time.

Raw snapshot payloads are preserved before validation and mapping. If validation fails, the raw message is marked failed and a failure event is recorded when feasible.

## Current Snapshot Shape

The initial skeleton supports:

- `snapshot_id`
- `node_id`
- `observed_at`
- `entities`
- `relationships`
- `observations`
- `events`
- `source`
- `provenance`
- `evidence`

Snapshot entities become AionCore JSON-LD entities with stable keys shaped as:

```text
smartsentinel:{node_id}:{snapshot_entity_id}
```

For example, `service:mosquitto` from node `fog-01` becomes `smartsentinel:fog-01:service:mosquitto`.

## Materialization Strategy

A SmartSentinel snapshot may contain more data than AionCore should store semantically. AionCore should materialize only the parts needed for semantic context, decision support, audit, and closed-loop verification.

Materialize as Entities:

- Services.
- Hosts.
- Containers.
- Databases.
- Queues.
- APIs.
- Jobs.
- Incidents.
- Environments.

Materialize as Relationships:

- Service depends on database.
- Service runs on host.
- Job belongs to environment.
- Alert affects service.
- Incident relates to deployment.

Materialize as Observations:

- Health status.
- Deployment status.
- Latency summary.
- Error-rate summary.
- Saturation summary.
- Backup freshness.
- Current incident count.

Materialize as Events:

- Alert opened.
- Alert resolved.
- Deployment failed.
- Dependency became unavailable.
- Backup completed.
- SLO breach detected.

The current endpoint materializes only entities, relationships, observations, and events. It also records AionCore lifecycle events:

- `aion:SmartSentinelSnapshotReceived`
- `aion:SmartSentinelSnapshotMapped`
- `aion:SmartSentinelSnapshotMappingFailed`

Lifecycle event metadata includes snapshot and node identifiers, mapping counts, validation issue counts, and skipped item counts. Raw snapshot content is not copied into lifecycle event metadata.

## Reconciliation Behavior

Entity mapping uses stable keys:

```text
smartsentinel:{node_id}:{snapshot_entity_id}
```

When a snapshot entity does not exist, AionCore creates it. When a snapshot entity already exists:

- `id` and `entity_key` are preserved.
- JSON-LD is rebuilt from the latest snapshot entity.
- `entity_type`, `sentinel:status`, `sentinel:properties`, and snapshot reference fields are updated.
- `updated_at` is refreshed only when the materialized entity changes.
- Existing entities with unchanged materialized JSON-LD are reused.

The response reports:

- `entities_created`
- `entities_updated`
- `entities_reused`
- `entities_skipped`

Relationship mapping de-duplicates by:

- `tenant_id`
- `source_entity_id`
- `relationship_type`
- `target_entity_id`

If the relationship already exists, it is reused instead of inserted again. Self-referential relationships are skipped. The response reports:

- `relationships_created`
- `relationships_reused`
- `relationships_skipped`

Missing entities from later snapshots are not deleted.

## Validation and Diagnostics

Validation runs after raw-message preservation and before mapping. Fatal validation errors prevent partial mapping.

Validated snapshot fields:

- `snapshot_id`: required.
- `node_id`: required.
- `observed_at`: optional, but must be RFC3339 when present.

Validated entity fields:

- `id`: required.
- `type`: required.
- `name`: optional.
- `properties`: optional object.

Validated relationships:

- `source`: required.
- `type`: required.
- `target`: required.
- `source` and `target` must reference snapshot entities or already existing SmartSentinel-mapped entities for the same node.

Validated observations:

- `entity_id`: required.
- `observed_property`: required.
- `value`: required.
- `observed_at`: optional, but must be RFC3339 when present.

Validated events:

- `event_type`: required.
- `severity`: optional, defaults to `info`; when present it must be `debug`, `info`, `warning`, `error`, or `critical`.
- `target_entity_id` and `source_entity_id`: optional; when present they must resolve to snapshot entities or already existing SmartSentinel-mapped entities for the same node.

The snapshot endpoint returns mapping diagnostics:

- `validation_warnings`
- `validation_errors`
- `skipped_items`

Validation errors are also returned in the error response body when validation fails.

## Provenance And Evidence

Snapshots may include a `source` object:

- `agent_id`
- `agent_version`
- `host_id`
- `environment`
- `collector`

Snapshots may include a `provenance` object:

- `run_id`
- `cycle_id`
- `trace_id`
- `correlation_id`
- `workflow_id`
- `external_refs`

Snapshots may include an `evidence` array. Evidence references are metadata only; AionCore does not fetch evidence URIs or validate external network resources.

Supported evidence fields:

- `evidence_id`
- `evidence_type`: `log`, `metric`, `trace`, `packet_capture`, `screenshot`, `report`, `url`, `command_output`, or `custom`
- `title`
- `description`
- `uri`
- `external_id`
- `collected_at`
- `related_entity_id`
- `related_event_type`
- `metadata`

If `evidence_type` is missing, the reference is treated as `custom` and a validation warning is returned. If `uri` is present but not a string, the evidence item is skipped with a validation warning. AionCore does not dereference or fetch `uri`.

Snapshot source, provenance, and evidence are preserved in raw-message metadata and in SmartSentinel lifecycle event metadata. Snapshot events may carry:

- `incident_id`
- `alert_id`
- `workflow_id`
- `run_id`
- `trace_id`
- `evidence_refs`

These fields are preserved in AionCore event metadata. Snapshot observations may carry `evidence_refs` and `source`, which are preserved in observation metadata. Evidence entries with `related_entity_id` are also included in the related entity JSON-LD projection when that entity is materialized.

The snapshot response includes:

- `provenance_present`
- `evidence_count`
- `external_ref_count`
- `correlation_id`
- `trace_id`
- `run_id`
- `cycle_id`

AI context endpoints already include recent events and observations with their metadata, so provenance and evidence references become visible through `/ai/context/entity/{entity_id}` without adding a separate AI feature.

## Provenance Query Ergonomics

Milestone 43 adds query filters over already-preserved SmartSentinel provenance metadata. These filters do not introduce a SmartSentinel runtime, polling, execution, URI fetching, or external AI calls.

Event queries support:

```text
GET /events?incident_id=inc-001
GET /events?alert_id=alert-001
GET /events?trace_id=trace-abc
GET /events?run_id=run-42
GET /events?workflow_id=wf-remediate
GET /events?cycle_id=cycle-7
GET /events?evidence_id=ev-log-1
GET /events?external_id=log-001
```

The existing event filters remain available and can be combined with these metadata filters. Matching is exact and is evaluated against event metadata fields, nested `provenance`, `evidence_refs`, evidence entries, and provenance `external_refs` where applicable.

Raw-message queries support:

```text
GET /raw-messages?trace_id=trace-abc
GET /raw-messages?run_id=run-42
GET /raw-messages?workflow_id=wf-remediate
GET /raw-messages?cycle_id=cycle-7
GET /raw-messages?correlation_id=corr-123
GET /raw-messages?snapshot_id=snap-evidence-001
GET /raw-messages?node_id=fog-02
GET /raw-messages?connector_profile=smartsentinel
GET /raw-messages?connector_key=...
GET /raw-messages?connector_id=...
```

Raw-message matching is exact and uses raw-message headers, including the SmartSentinel provenance metadata preserved at ingestion time. The response remains the existing raw-message response shape.

An aggregate provenance search endpoint is also available:

```text
GET /provenance/search?trace_id=trace-abc
GET /provenance/search?incident_id=inc-001
```

The response includes:

- `matching_events`
- `matching_raw_messages`
- `matching_observations`
- `counts`
- `query`

This endpoint is intended for operator audit and explainability workflows, such as finding all AionCore records tied to a SmartSentinel trace, run, cycle, incident, or evidence reference. Observation matching is metadata-based and returns only observations that already carry matching provenance or evidence metadata.

The implementation applies provenance metadata filters in the API layer after using existing storage queries. This keeps SmartSentinel optional and avoids new required storage schema fields. PostgreSQL JSONB indexes for these query fields are future optimization work rather than a Milestone 43 requirement.

Materialize as Commands:

- Run diagnostic.
- Create incident.
- Trigger remediation workflow.
- Restart service.
- Roll back deployment.

Materialize as Actions:

- SmartSentinel workflow invoked.
- API call sent to an operational tool.
- Diagnostic started.
- Remediation attempted.

Materialize as ActionResults:

- Diagnostic completed.
- Workflow succeeded.
- Workflow failed.
- Remediation partially completed.
- External tool returned timeout.

## Executor Bridge

Milestone 44 adds SmartSentinel-friendly executor bridge endpoints over the generic ExecutorAgent APIs:

```text
POST /integrations/smartsentinel/executors/register
GET /integrations/smartsentinel/executors/{executor_id}/commands
POST /integrations/smartsentinel/executors/{executor_id}/commands/{command_id}/claim
POST /integrations/smartsentinel/executors/{executor_id}/commands/{command_id}/report
```

The bridge is optional. It does not implement a SmartSentinel runtime, does not execute host commands, and does not call Docker, systemctl, kubectl, or any recovery tool. It only lets a SmartSentinel-like external executor discover compatible AionCore Commands, claim them through existing lease semantics, and report attempted outcomes.

Registration creates or reuses an ExecutorAgent with:

- `agent_type = smartsentinel`
- `agent_key`
- optional `display_name`
- optional `metadata`

Registration can also declare command capabilities such as:

- `sentinel:RestartService`
- `sentinel:RestartContainer`
- `sentinel:NotifyOperator`
- `sentinel:RunDiagnostic`

Executor scopes reuse the generic scope model and can target:

- `target_entity_id`
- `entity_type`
- `relationship_type`

Polling returns pending compatible commands using the existing executor capability and scope matcher. The SmartSentinel response wraps the command with the latest lease, target entity metadata when available, and a small set of recent provenance/evidence metadata from related events when it is cheap to collect.

Claiming a command reuses the generic command claim path. Commands that require approval remain unclaimable until the normal command approval lifecycle approves them. Active command leases continue to block competing executors.

Reporting accepts:

- `action_type`
- `status`: `executed` or `failed`
- `verified`
- `result_payload`
- optional `evidence_refs`
- optional `incident_id`, `alert_id`, `workflow_id`, `run_id`, `trace_id`, `correlation_id`
- optional `message`
- optional `metadata`

Reporting creates an Action and ActionResult, marks the command executed or failed through the existing lifecycle, updates the active lease, and records:

```text
aion:SmartSentinelCommandReported
```

Provenance and evidence fields are preserved in ActionResult and event metadata. The reported `executed` status means the external executor says it attempted or completed the action; AionCore itself still performs no recovery action.

The generic ExecutorAgent endpoints remain available and unchanged. The SmartSentinel bridge is an ergonomic integration layer, not a separate lifecycle implementation.

## Metrics Boundary

High-frequency operational metrics should remain in specialized metric backends when needed.

Examples:

- Per-second CPU samples.
- High-cardinality traces.
- Raw logs.
- Detailed request histograms.
- Full incident timelines already stored in an incident platform.

AionCore should store:

- Semantic state.
- Summaries.
- Events.
- Decisions.
- Commands.
- Action results.
- Entity and relationship context.
- References to external evidence.

This keeps AionCore useful for context and decision support without becoming a replacement for metric, log, trace, or incident systems.

## References

AionCore records should preserve references back to SmartSentinel where useful:

- Snapshot ID.
- Workspace or environment.
- External entity ID.
- Alert ID.
- Incident ID.
- Workflow run ID.
- Evidence URL or opaque reference.

References should be stored as metadata or relationship properties, not as required core schema fields.

## Non-Goals

The SmartSentinel integration should not:

- Make SmartSentinel mandatory for AionCore.
- Replace AionCore ingestion, context, or observation models.
- Force operational monitoring assumptions into core migrations.
- Store all high-frequency metrics inside AionCore.
- Allow AI clients to execute critical actions by default.

## Current Limitations

- No SmartSentinel agent runtime is implemented.
- No live SmartSentinel runtime polling loop is implemented; only HTTP polling endpoints are available.
- No authentication is implemented for the integration endpoint.
- No recovery action execution is implemented.
- Evidence URIs are never fetched by AionCore.
- No delete or graph reconciliation is performed when later snapshots omit an entity.
- The mapper accepts a simplified snapshot shape only.
- Relationship de-duplication is limited to exact source/type/target matches.
- Provenance filters are exact-match metadata filters and do not perform full-text search or external system lookups.
- PostgreSQL-specific JSONB indexes for provenance filters are not added yet.
