# Flow Execution Model

Milestone 98 adds the first internal flow execution engine foundation for AionCore.

## Intent

This milestone adds a backend-only simulation surface that can evaluate a stored or proposed flow against:

- an explicit `sample_payload`
- a tenant-owned existing `RawMessage`

The execution engine is intentionally internal and side-effect-free.

## Endpoints

- `POST /flows/execute`
- `POST /flows/{flow_id}/execute`

## Current Behavior

Execution in this milestone always runs with:

- `mode = simulate`
- `simulated = true`
- `side_effects_performed = false`

The engine can return:

- validation status and structured validation issues
- per-node execution results
- per-sink simulated action results
- observation previews
- event previews
- command previews
- DLQ previews

It does not:

- subscribe to brokers
- publish MQTT
- forward HTTP
- create observations
- create events
- create commands
- create DLQ records
- change stored flow state
- change ingestion behavior
- change connector worker behavior

## Source Input

Supported simulated source input:

- `sample_payload`
- stored `RawMessage.payload` when `raw_message_id` is provided to `POST /flows/{flow_id}/execute`

Source node kinds such as `mqtt_subscribe`, `http_input`, `ttn_uplink`, and `internal_observation` remain declarative. The engine uses the explicit execution input instead of opening live subscriptions.

## Supported Node Kinds

Current source handling:

- `mqtt_subscribe`
- `http_input`
- `ttn_uplink`
- `internal_observation`

Current decoder and transform handling:

- `senml_decode`
- `ultralight_decode`
- `canonical_json`
- `json_map`
- `filter_condition`
- `threshold_rule`

Current sink and DLQ handling:

- `internal_observation_store`
- `raw_message_store`
- `mqtt_publish`
- `http_forward`
- `event_create`
- `command_create`
- `dlq`

## Execution Semantics

- `senml_decode`, `ultralight_decode`, and `canonical_json` reuse existing payload decoder helpers when possible and emit preview measurements.
- `json_map` validates or parses the configured mapping JSON and emits a simple mapping preview.
- `filter_condition` can evaluate `eq`, `neq`, `gt`, `gte`, `lt`, `lte`, `contains`, and `exists`.
- a false `filter_condition` skips downstream nodes on that path.
- `threshold_rule` attempts the same simple condition evaluation when the configured condition is parseable JSON.
- sink nodes emit `would_*` action previews only.

## Response Shape

Execution responses now include:

- `execution_id`
- `simulated`
- `side_effects_performed`
- `node_results`
- `sink_results`
- `observations_preview`
- `events_preview`
- `commands_preview`
- `dlq_preview`

Per-sink actions currently include:

- `would_store_observation`
- `would_publish_mqtt`
- `would_forward_http`
- `would_create_event`
- `would_create_command`
- `would_write_dlq`
- `no_op`

## Relationship To Validation And Dry-Run

- validation checks graph structure and safe references
- dry-run reports conceptual path and conceptual sink behavior
- execute now interprets nodes against explicit input and returns preview artifacts, but still performs no side effects

## Limitations

- execution is opt-in through explicit API calls only
- execution is not wired to flow enablement or runtime workers
- no broker subscriptions are created
- no replay or DLQ automation exists
- no command, event, observation, or DLQ persistence occurs
- `json_map` remains intentionally simple
- rule evaluation is limited to simple condition previewing

## Future Work

Later milestones may add:

- real sink delivery
- runtime source binding
- DLQ routing and replay
- connector-driven execution
- richer mapping and rule semantics
- dashboard execution inspection surfaces
