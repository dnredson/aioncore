# AionCore Documentation Index

This index groups the main AionCore documentation so model docs, usage guides, ADRs, and validation notes can be reached without relying on the root `README.md`.

## Model Docs

- [Architecture](ARCHITECTURE.md)
- [Domain Model](DOMAIN_MODEL.md)
- [Observation Model](OBSERVATION_MODEL.md)
- [Ingestion Model](INGESTION_MODEL.md)
- [Aion Edge Adapter Model](EDGE_ADAPTER_MODEL.md)
- [SmartSentinel Integration Model](SMARTSENTINEL_INTEGRATION.md)
- [Persistence Model](PERSISTENCE_MODEL.md)
- [AI and MCP Model](AI_MCP_MODEL.md)
- [Action Model](ACTION_MODEL.md)
- [Security Model](SECURITY_MODEL.md)
- [Dashboard Model](DASHBOARD_MODEL.md)
- [DLQ Model](DLQ_MODEL.md)
- [Flow Model](FLOW_MODEL.md)
- [Flow Execution Model](FLOW_EXECUTION_MODEL.md)
- [NiFi Integration Model](NIFI_INTEGRATION_MODEL.md)

## Usage Guides

- [Configuration](CONFIGURATION.md)
- [MVP Demo Scenario](MVP_DEMO_SCENARIO.md)
- [MVP Scope Freeze](MVP_SCOPE_FREEZE.md)
- [MVP Runbook](MVP_RUNBOOK.md)
- [Release Checklist](RELEASE_CHECKLIST.md)
- [Authentication Usage](AUTH_USAGE.md)
- [Ingestion Usage](INGESTION_USAGE.md)
- [Time-Series Usage](TIMESERIES_USAGE.md)
- [Dashboard Usage](DASHBOARD_USAGE.md)
- [Dashboard Frontend](../apps/aion-dashboard/README.md)
- [DLQ Usage](DLQ_USAGE.md)
- [DLQ Replay Usage](DLQ_REPLAY_USAGE.md)
- [Reliable Ingestion Usage](RELIABLE_INGESTION_USAGE.md)
- [Sync Session Model](SYNC_SESSION_MODEL.md)
- [Sync Session Usage](SYNC_SESSION_USAGE.md)
- [Batch Ingestion Usage](BATCH_INGESTION_USAGE.md)
- [Flow Usage](FLOW_USAGE.md)
- [Flow Validation Usage](FLOW_VALIDATION_USAGE.md)
- [Flow Execution Usage](FLOW_EXECUTION_USAGE.md)
- [TTN Usage](TTN_USAGE.md)
- [SmartSentinel Usage](SMARTSENTINEL_USAGE.md)
- [MCP Usage](MCP_USAGE.md)
- [NiFi/MiNiFi Usage](NIFI_USAGE.md)
- [Commands, Rules, and Executors Usage](COMMANDS_RULES_EXECUTORS_USAGE.md)
- [Runtime Validation](RUNTIME_VALIDATION.md)
- [MVP Demo Script](../scripts/demo-mvp-memory.ps1)

## Planning And ADRs

- [Roadmap](ROADMAP.md)
- [Architecture Decision Records](ADR)

Recent dashboard and flow ADRs:

- [ADR 0101: MVP Demo Scenario And Documentation Freeze](ADR/0101-mvp-demo-scenario-documentation-freeze.md)
- [ADR 0099: DLQ Replay Processing Foundation](ADR/0099-dlq-replay-processing-foundation.md)

- [ADR 0095: Richer simulated flow execution semantics](ADR/0095-richer-simulated-flow-execution-semantics.md)
- [ADR 0094: Flow execution UI integration](ADR/0094-flow-execution-ui-integration.md)
- [ADR 0093: Flow execution engine foundation](ADR/0093-flow-execution-engine-foundation.md)
- [ADR 0092: Typed flow node inspectors](ADR/0092-typed-flow-node-inspectors.md)
- [ADR 0091: Constrained visual flow editing](ADR/0091-constrained-visual-flow-editing.md)
- [ADR 0090: Visual flow graph layer](ADR/0090-visual-flow-graph-layer.md)
- [ADR 0089: Optional `aion-api` static dashboard hosting](ADR/0089-optional-aion-api-static-dashboard-hosting.md)
- [ADR 0088: Dashboard static packaging and maintainability](ADR/0088-dashboard-static-packaging-maintainability.md)
- [ADR 0087: Flow Builder UI foundation](ADR/0087-flow-builder-ui-foundation.md)
- [ADR 0086: Time-series explorer UI](ADR/0086-timeseries-explorer-ui.md)
- [ADR 0085: Connector and broker management UI](ADR/0085-connector-broker-management-ui.md)
- [ADR 0084: Dashboard frontend skeleton](ADR/0084-dashboard-frontend-skeleton.md)
- [ADR 0083: Dashboard flow inventory and detail API](ADR/0083-dashboard-flow-inventory-detail-api.md)
- [ADR 0082: Flow validation and dry-run API](ADR/0082-flow-validation-dry-run-api.md)

## Validation And Deployment

- [Configuration](CONFIGURATION.md)
- [MVP Runbook](MVP_RUNBOOK.md)
- [Release Checklist](RELEASE_CHECKLIST.md)
- [Runtime Validation](RUNTIME_VALIDATION.md)
- [README Quick Local Start](../README.md#quick-local-start)
- [ADR 0096: Flow Execution Side-Effect Authorization](ADR/0096-flow-execution-side-effect-authorization.md)

- [ADR 0097: Safe Internal Flow Side Effects](ADR/0097-safe-internal-flow-side-effects.md)

- [Flow execution model](FLOW_EXECUTION_MODEL.md) now covers preview execution, safe internal side effects, and guarded MQTT/HTTP sink execution.

- [ADR 0100: Batch Sync-Session Tracking API](ADR/0100-batch-sync-session-tracking-api.md)
