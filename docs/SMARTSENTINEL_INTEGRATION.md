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

Recommended raw message metadata:

- `protocol`: `http`, `webhook`, `file`, or integration-specific value.
- `payload_format`: `smartsentinel_snapshot`.
- `source_type`: integration source.
- `source_ref`: SmartSentinel workspace, project, environment, or snapshot reference.
- `received_at`: AionCore receive time.

Raw snapshot payloads should be preserved before normalization.

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
