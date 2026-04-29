# SmartSentinel Integration

SmartSentinel is an optional operational integration for AionCore. It is not a core dependency.

AionCore Core remains domain-agnostic. SmartSentinel can participate as an observer, executor, or evidence source in operational and infrastructure monitoring deployments.

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

## Snapshot Ingestion

SmartSentinel snapshots can be ingested through the same payload-agnostic raw message path as other integrations.

Milestone 40 added the first optional ingestion skeleton. Milestone 41 hardens the same endpoint with reconciliation and diagnostics:

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
- No live polling is implemented.
- No authentication is implemented for the integration endpoint.
- No recovery action execution is implemented.
- No delete or graph reconciliation is performed when later snapshots omit an entity.
- The mapper accepts a simplified snapshot shape only.
- Relationship de-duplication is limited to exact source/type/target matches.
