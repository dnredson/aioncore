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

The current flow foundation now adds:

- a tenant-scoped `Flow` domain model
- node and edge graph storage
- in-memory and PostgreSQL persistence
- CRUD plus enable and disable APIs
- validation and non-executing dry-run APIs
- dashboard inventory and detail read APIs
- token-mode auth scopes and tenant filtering
- lifecycle audit events

The current flow milestones still do not add:

- visual graph editing
- drag-and-drop editing
- flow execution
- runtime packet forwarding
- MQTT publish or HTTP forward sinks
- broker subscriptions driven by flows
- DLQ runtime processing
- external flow-engine execution, including NiFi or MiNiFi execution
- any side effects from validation or dry-run

Milestone 95 adds the first static dashboard visual graph layer for inspection and preview only. It renders existing node and edge data, validation markers, and dry-run effect hints, but it does not change the flow model, persist graph layout edits, or introduce runtime behavior.

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
- the Milestone 84 DLQ API foundation now provides a typed destination model for later DLQ-oriented flow execution, but flows still do not route to it automatically

Flows may also reference external operational flow engines through metadata only. For example, a Flow can document that its real transport or replay path is implemented in NiFi or MiNiFi while AionCore continues to own the semantic destination model. See [NiFi Integration Model](NIFI_INTEGRATION_MODEL.md).

Flows do not replace these models. They provide an explicit graph representation for how operators expect source, processing, and sink stages to be configured.

## Validation Rules

Create and update still validate:

- `flow_key` is required on create
- `name` is required on create
- `node_id` values must be unique within the flow
- every edge source and target must reference an existing node
- `node_type` must be one of the supported enum values

The dedicated validation API adds richer structured checks without changing `/flows` CRUD behavior:

- flow has at least one node
- node IDs are unique
- edge IDs are unique when present
- every edge source and target exists
- at least one source node exists
- at least one sink or `dlq` node exists
- isolated nodes are reported as warnings
- simple cycle detection is performed and reported as an error when detected
- source and sink `connector_id` references are checked when they can be safely verified
- secret-like config fields are redacted in validation and dry-run output

Structured issues include:

- `severity`
- `code`
- `message`
- optional `node_id`
- optional `edge_id`
- optional `field`

## Dry-Run Model

Dry-run is planning-oriented and non-executing.

It can report:

- whether the flow is structurally valid
- the reachable node path from one source or all sources
- the sources, transforms, sinks, and DLQ nodes present in the plan
- referenced connectors
- whether the flow would conceptually store observations, publish MQTT, forward HTTP, create events, create commands, or use DLQ

Dry-run does not:

- subscribe to brokers
- publish MQTT
- forward HTTP
- create observations
- create events
- create commands
- create DLQ records
- change stored flow state

## Future Direction

This model is the backend contract for later milestones:

- dashboard flow list and detail views
- a form-based dashboard builder foundation before graph editing exists
- a read-only visual graph dashboard layer before graph editing exists
- Node-RED-like graph editing
- dashboard-driven validation and dry-run inspection
- flow execution engine
- runtime source binding and sink dispatch
- deeper operational validation and simulation
- DLQ visibility and replay tooling

External flow references should use stable metadata keys where applicable:

- `external.source_system`
- `external.flow_id`
- `external.flow_name`
- `external.process_group_id`

## Dashboard Read Relationship

The flow dashboard endpoints are additive and do not replace `/flows`:

- `/flows` remains the operational CRUD surface.
- `/flows/{flow_id}/validation` and `/flows/{flow_id}/dry-run` remain the canonical validation and planning surfaces.
- `/dashboard/flows` and `/dashboard/flows/{flow_id}` provide inventory/detail shapes optimized for future UI panels and graph rendering.

Dashboard flow detail redacts secret-like node config keys using the same behavior as validation and dry-run output.

## Static Flow Builder Relationship

Milestone 92 adds a static frontend-only Flow Builder foundation under `apps/aion-dashboard/`.

That UI:

- uses a guided source -> transform -> sink form rather than arbitrary graph editing
- generates linear edges from the form
- shows a redacted JSON preview before save
- renders a dependency-free read-only graph preview for stored and proposed flows
- allows an optional advanced JSON override for low-risk manual editing
- uses `POST /flows/validate` and `POST /flows/dry-run` before create
- uses `GET /flows/{flow_id}/validation` and `POST /flows/{flow_id}/dry-run` for stored flow inspection
- still does not execute flows or create side effects

This keeps the backend model stable while giving operators a safe authoring surface before a future Node-RED-like builder arrives.
