# Flow Execution Model

Milestone 98 adds the first internal flow execution engine foundation for AionCore.

## Intent

This milestone adds a backend-only simulation surface that can evaluate a stored or proposed flow against:

- an explicit `sample_payload`
- a tenant-owned existing `RawMessage`

The execution engine started as internal and side-effect-free. Later milestones add explicitly authorized side-effect paths while keeping simulation as the default and requiring opt-in execution intent.

## Endpoints

- `POST /flows/execute`
- `POST /flows/{flow_id}/execute`

## Current Behavior

Execution defaults to preview/simulation behavior. Real side effects are only considered when the request explicitly declares side-effect intent and token-mode authorization allows it.

The engine can return:

- validation status and structured validation issues
- per-node execution results
- per-sink simulated action results
- observation previews
- event previews
- command previews
- DLQ previews

By default it does not subscribe to brokers, publish MQTT, forward HTTP, create observations, create events, create commands, create DLQ records, change stored flow state, change ingestion behavior, or change connector worker behavior. Milestone 102 adds explicitly authorized internal observation/event side effects. Milestone 103 adds explicitly requested connector-gated MQTT publish and HTTP forward side effects. Command creation, DLQ writes, broker subscriptions, and automatic enabled-flow runtime execution remain deferred.

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
- sink nodes emit `would_*` action previews by default. Explicitly authorized milestones may perform selected sink actions while preserving detailed previews and audit metadata.

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

Milestone 99 adds static dashboard UI integration for this simulated execute surface:

- proposed draft execution from the Flow Builder uses `POST /flows/execute`
- stored flow execution from the detail view uses `POST /flows/{flow_id}/execute`
- the dashboard renders redacted request previews, execution result panels, node execution highlighting, and sink conceptual actions
- the dashboard does not add any real side-effecting execution path

## Limitations

- execution is opt-in through explicit API calls only
- execution is not wired to flow enablement or runtime workers
- no broker subscriptions are created
- no replay or DLQ automation exists
- command creation and DLQ persistence are still preview-only
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
- future real execute authorization, approval, and sink-delivery controls

## Milestone 100: Richer Simulated Semantics

Milestone 100 keeps execution simulated and side-effect-free, but makes the execution preview more expressive for future flow authoring and review.

Additional simulation behavior includes:

- `edge_results` in execution responses, so branching decisions can be inspected independently from node and sink results.
- edge-level conditions through edge `metadata.condition`, `metadata.when`, or `metadata.filter`.
- compound condition evaluation using `all`, `any`, and `not`.
- additional operators: `not_exists`, `missing`, `between`, and `in`.
- richer `json_map` previews with nested target paths, source path objects, defaults, literal values, and simple `{field.path}` templates.

These additions are still preview-only. They do not persist transformed payloads, create observations, write DLQ records, publish MQTT, call HTTP endpoints, or create commands/events.

### Edge Conditions

A flow edge may carry conditional metadata such as:

```json
{
  "condition": {
    "field": "temperature",
    "operator": "gte",
    "value": 30
  }
}
```

When the condition evaluates to `true`, the edge is reported as `traversed`. When it evaluates to `false`, the edge is reported as `skipped` and the downstream path is marked skipped. If the condition is invalid, the edge is reported as `failed` and downstream nodes are not executed.

### Compound Conditions

Filter and rule conditions can now use:

```json
{
  "all": [
    { "field": "temperature", "operator": "between", "value": [30, 40] },
    { "field": "state", "operator": "in", "value": ["warning", "critical"] }
  ]
}
```

Supported composition keys are:

- `all`
- `any`
- `not`

### Richer JSON Mapping Preview

`json_map` remains intentionally simple, but now supports clearer mapping conventions:

```json
{
  "entity.id": "device.id",
  "reading.value": { "from": "temperature", "default": 0 },
  "reading.unit": { "literal": "Cel" },
  "topic": { "template": "devices/{device.id}/temperature" }
}
```

Target keys may use dot paths to create nested output objects. Mapping values may be direct source paths, `{ "from": ... }`, `{ "path": ... }`, `{ "default": ... }`, `{ "literal": ... }`, `{ "value": ... }`, or `{ "template": ... }`.

## Updated Limitations

- branching is simulated through edge traversal and skipped-path reporting only;
- edge condition support is intentionally small and declarative;
- `json_map` does not execute arbitrary code or expressions;
- conditions are evaluated only over the current payload and decoded observation previews;
- no side-effecting execution is enabled by these semantics.

## Side-effect authorization boundary

Simulated execution is still the only supported execution mode. However, execution requests can now declare future side-effect intent using `allow_side_effects`, `requested_sink_actions`, `operator_reason`, and `approval_reference`.

In token mode, simulated execution requires `flows:read`. If a request asks for side effects, the API also requires `flows:execute` or `admin:all`. This is a forward-compatible authorization boundary only: real side effects are still disabled and responses continue to report `simulated=true` and `side_effects_performed=false`.

The execution response includes an `authorization` object with the current runtime policy. In this milestone, `authorization.real_side_effects_supported=false` and `authorization.policy="preview_only_no_side_effects"`.

## Milestone 102: Safe Internal Side Effects

Milestone 102 introduces the first real, but tightly constrained, side-effecting execution path. External side effects remain disabled. When a request explicitly sets `allow_side_effects=true` or names supported `requested_sink_actions`, and the principal has `flows:execute` or `admin:all`, the execution engine may perform only these internal writes:

- `internal_observation_store` -> persist observations.
- `event_create` -> persist events.

The response still reports `simulated=true` because flow sources, broker subscriptions, and external delivery are not active. However, `side_effects_performed` may be `true` when one of the supported internal sinks writes data. Each `sink_result` also reports whether its side effect was performed.

Unsupported sinks remain preview-only, including MQTT publish, HTTP forward, command creation, raw-message storage, and DLQ writes.

Real observation storage requires enough node configuration to create a valid `Observation`, including `producer_entity_id` or `source_entity_id`, `feature_of_interest_id`, and `observed_property`. Event creation uses the node's `event_type`, `severity`, optional source/target entity IDs, and records execution metadata without storing the full payload as trusted proof.

Safe internal observation writes validate that `producer_entity_id`/`source_entity_id` and `feature_of_interest_id` belong to the execution tenant before persisting. These writes do not yet trigger the rule engine; late-data and rule/command policies remain future work.


## Milestone 103: MQTT/HTTP Sink Execution

Milestone 103 adds the first external sink side effects, still behind explicit authorization and connector gates.

Real MQTT publish and HTTP forward are only attempted when all of the following are true:

- the request declares side-effect intent;
- token mode authorization satisfies `flows:execute` or `admin:all`;
- `requested_sink_actions` explicitly includes `publish_mqtt`/`mqtt_publish` or `forward_http`/`http_forward`;
- the sink node references a tenant-owned enabled connector through `config.connector_id`;
- the connector type matches the sink kind.

MQTT publish currently supports `mqtt://` connectors, optional `mqtt_basic_auth` connector secrets, explicit non-wildcard publish topics, QoS selection, retain flag, payload templates, and bounded publish attempts. TTN v3 connectors remain excluded from publish execution because they are modeled as subscriber/uplink connectors.

HTTP forward currently supports connector-gated `http://` endpoints with `POST`, `PUT`, or `PATCH`, bounded timeouts, redacted endpoint reporting, and no connector-secret use. HTTPS, custom headers, and secret-backed HTTP credentials remain future work.

The following remain non-executing:

- broker subscriptions from flows;
- command creation;
- DLQ writes;
- automatic enabled-flow runtime execution;
- MQTT/HTTP execution without explicit `requested_sink_actions`.
