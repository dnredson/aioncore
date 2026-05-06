# Flow Model

Milestone 82 adds the first backend model and API foundation for AionCore flows and pipelines.

## Intent

AionCore flows represent operator-configured processing graphs such as:

- source broker -> decode -> transform -> internal storage
- source broker -> filter -> MQTT forward sink
- source broker -> rule -> event, command, or DLQ path
- HTTP, TTN, or MQTT input -> normalize -> sink

This model is intentionally inspired by Node-RED-style operational graphs while staying compatible with AionCore's existing domain and ingestion architecture.

## Current Scope

The current flow foundation adds:

- a tenant-scoped `Flow` domain model
- node and edge graph storage
- in-memory and PostgreSQL persistence
- CRUD plus enable and disable APIs
- token-mode auth scopes and tenant filtering
- lifecycle audit events

The current milestone does not add:

- visual UI
- drag-and-drop editing
- flow execution
- runtime packet forwarding
- MQTT publish or HTTP forward sinks
- broker subscriptions driven by flows
- DLQ runtime processing
- external flow-engine execution, including NiFi or MiNiFi execution

## Core Types

`Flow`

- `id`
- `tenant_id`
- `flow_key`
- `name`
- `description`
- `enabled`
- `nodes`
- `edges`
- `metadata`
- `created_at`
- `updated_at`

`FlowNode`

- `node_id`
- `node_type`
- `name`
- `config`
- `position`

`FlowEdge`

- `edge_id`
- `source_node_id`
- `target_node_id`
- `label`
- `metadata`

## Node Types

The initial `FlowNodeType` enum is intentionally small and generic:

- `source`
- `decoder`
- `transform`
- `filter`
- `rule`
- `sink`
- `dlq`

## Config Conventions

Node execution behavior is not implemented yet, but node `config` JSON already follows conventions that future runtimes and dashboard tooling can rely on.

### Source Kinds

Examples:

- `mqtt_subscribe`
- `http_input`
- `ttn_uplink`
- `internal_observation`
- `schedule`

Common source config fields:

- `kind`
- `connector_id`
- `topic_filter`

### Decoder And Transform Kinds

Examples:

- `senml_decode`
- `ultralight_decode`
- `canonical_json`
- `json_map`
- `filter_condition`

### Sink Kinds

Examples:

- `internal_observation_store`
- `raw_message_store`
- `mqtt_publish`
- `http_forward`
- `event_create`
- `command_create`
- `dlq`

## Relationship To Existing AionCore Models

- `IngestionConnector` remains the current runtime source configuration and worker entry point.
- `Observation` remains the canonical telemetry storage model.
- `Rule`, `Command`, and `Event` remain the existing policy and control-plane primitives.
- `RawMessage` remains the required pre-normalization persistence layer.
- future `dlq` flow nodes are intended to model operational error paths, but no DLQ runtime is introduced in this milestone.

Flows may also reference external operational flow engines through metadata only. For example, a Flow can document that its real transport or replay path is implemented in NiFi or MiNiFi while AionCore continues to own the semantic destination model. See [NiFi Integration Model](NIFI_INTEGRATION_MODEL.md).

Flows do not replace these models. They provide an explicit graph representation for how operators expect source, processing, and sink stages to be configured.

## Validation Rules

The current API validates:

- `flow_key` is required on create
- `name` is required on create
- `node_id` values must be unique within the flow
- every edge source and target must reference an existing node
- `node_type` must be one of the supported enum values

The current API does not validate external connector existence or sink reachability.

## Future Direction

This model is the backend contract for later milestones:

- dashboard flow list and detail views
- Node-RED-like graph editing
- flow execution engine
- runtime source binding and sink dispatch
- operational validation and simulation
- DLQ visibility and replay tooling

External flow references should use stable metadata keys where applicable:

- `external.source_system`
- `external.flow_id`
- `external.flow_name`
- `external.process_group_id`
