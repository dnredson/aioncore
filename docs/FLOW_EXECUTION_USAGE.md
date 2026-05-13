# Flow Execution Usage

This guide covers the Milestone 98 simulated flow execution API foundation.

## Endpoints

- `POST /flows/execute`
- `POST /flows/{flow_id}/execute`

## Key Rules

- execution is preview-only unless side effects are explicitly requested and authorized
- token mode uses `flows:read` for preview execution and `flows:execute` for requested side effects
- MQTT publish and HTTP forward require explicit `requested_sink_actions` entries and connector references
- command creation and DLQ writes remain preview-only

## Proposed Flow Execution

Use `POST /flows/execute` to simulate a proposed unsaved flow against an explicit payload.

```powershell
$body = @'
{
  "flow_key": "execute-proposed",
  "name": "Execute Proposed",
  "nodes": [
    {
      "node_id": "source-1",
      "node_type": "source",
      "config": { "kind": "http_input" }
    },
    {
      "node_id": "decoder-1",
      "node_type": "decoder",
      "config": { "kind": "senml_decode" }
    },
    {
      "node_id": "sink-1",
      "node_type": "sink",
      "config": { "kind": "internal_observation_store" }
    }
  ],
  "edges": [
    { "source_node_id": "source-1", "target_node_id": "decoder-1" },
    { "source_node_id": "decoder-1", "target_node_id": "sink-1" }
  ],
  "sample_payload": [
    { "n": "temperature", "v": 21.4, "u": "Cel" }
  ],
  "payload_format": "senml-json"
}
'@

Invoke-RestMethod -Method Post -Uri "http://localhost:8080/flows/execute" -ContentType "application/json" -Body $body
```

## Stored Flow Execution With Sample Payload

```powershell
$body = @'
{
  "sample_payload": {
    "temperature": 31.2
  },
  "payload_format": "application/json"
}
'@

Invoke-RestMethod -Method Post -Uri "http://localhost:8080/flows/<flow-id>/execute" -ContentType "application/json" -Body $body
```

## Stored Flow Execution With Raw Message

```powershell
$body = @'
{
  "raw_message_id": "00000000-0000-0000-0000-000000000000"
}
'@

Invoke-RestMethod -Method Post -Uri "http://localhost:8080/flows/<flow-id>/execute" -ContentType "application/json" -Body $body
```

`raw_message_id` execution is tenant-aware in `token` mode and remains preview-only.

## Response Highlights

Execution responses include:

- `execution_id`
- `valid`
- `validation_issues`
- `node_results`
- `sink_results`
- `observations_preview`
- `events_preview`
- `commands_preview`
- `dlq_preview`
- `simulated`
- `side_effects_performed`

Typical sink actions:

- `would_store_observation`
- `would_publish_mqtt`
- `would_forward_http`
- `would_create_event`
- `would_create_command`
- `would_write_dlq`

## Validation Vs Dry-Run Vs Execute

- `POST /flows/validate` checks whether the graph is structurally acceptable.
- `POST /flows/dry-run` reports the conceptual path and conceptual sink effects without interpreting node logic.
- `POST /flows/execute` now interprets supported nodes against explicit input and returns previews, but still performs no external or persistent side effects.

## Dashboard UI

Milestone 99 wires these endpoints into `apps/aion-dashboard/`.

The Flow Builder uses:

- `POST /flows/execute` for unsaved proposed flows

Stored flow detail uses:

- `POST /flows/{flow_id}/execute` for saved flows

The dashboard execute surface is still simulation-only:

- it shows redacted request previews and execution result previews
- it highlights returned `node_results` statuses on the immutable graph
- it shows sink conceptual actions such as `would_store_observation`, `would_publish_mqtt`, `would_forward_http`, `would_create_event`, `would_create_command`, `would_write_dlq`, and `no_op`
- it does not publish MQTT, forward HTTP, create observations, create events, create commands, or write DLQ records

## Token Mode

Execution routes require:

- `flows:read` for `POST /flows/execute`
- `flows:read` for `POST /flows/{flow_id}/execute`
- `admin:all` also satisfies both

Token-mode behavior:

- missing token returns `401`
- wrong scope returns `403`
- cross-tenant stored flow execution returns `403`

## Current Limitations

- only `mode = simulate` is supported
- no runtime worker integration exists
- no external sink delivery exists
- no automatic DLQ handling exists
- no real execute UI is added in this milestone; the dashboard only calls simulated execute

## Richer Simulated Mapping, Rules, And Branching

Milestone 100 extends simulated execution previews while keeping `side_effects_performed = false`.

### Conditional Branch Example

A proposed flow edge can include conditional metadata:

```json
{
  "edge_id": "hot-edge",
  "source_node_id": "source-1",
  "target_node_id": "sink-hot",
  "metadata": {
    "condition": {
      "field": "temperature",
      "operator": "gte",
      "value": 30
    }
  }
}
```

The response includes `edge_results` with `traversed`, `skipped`, or `failed` statuses. This is useful for dashboards that need to explain why one branch was followed and another branch was skipped.

### Compound Rule Example

```json
{
  "kind": "threshold_rule",
  "condition": {
    "all": [
      { "field": "temperature", "operator": "between", "value": [30, 40] },
      { "field": "state", "operator": "in", "value": ["warning", "critical"] }
    ]
  }
}
```

Supported condition composition keys are `all`, `any`, and `not`. Supported operators include `eq`, `neq`, `gt`, `gte`, `lt`, `lte`, `contains`, `exists`, `not_exists`, `between`, and `in`.

### JSON Mapping Example

```json
{
  "kind": "json_map",
  "mapping": {
    "entity.id": "device.id",
    "reading.value": { "from": "temperature", "default": 0 },
    "reading.unit": { "literal": "Cel" },
    "topic": { "template": "devices/{device.id}/temperature" }
  }
}
```

The execution response shows the mapped output as preview data only. No transformed payload is persisted by simulated execution.

## Requesting future side effects safely

Execution remains simulated, but operators can already test the authorization boundary that will protect future real sinks.

```json
{
  "sample_payload": {"temperature": 31.5},
  "allow_side_effects": true,
  "requested_sink_actions": ["would_store_observation"],
  "operator_reason": "maintenance dry-run before enabling execution",
  "approval_reference": "ticket-123"
}
```

In token mode this request requires both `flows:read` and `flows:execute`. The response still reports no real side effects because the current runtime policy is preview-only.

## Safe Internal Side Effects

To request internal side effects, include `allow_side_effects=true` and use a token with `flows:execute` or `admin:all`. To restrict the operation to specific supported sinks, pass `requested_sink_actions`, for example:

```json
{
  "sample_payload": {"temperature": 31.5},
  "allow_side_effects": true,
  "requested_sink_actions": ["store_observation", "create_event"],
  "operator_reason": "manual validation in lab",
  "approval_reference": "ticket-123"
}
```

Milestone 102 supports only internal observation and event writes. MQTT publish, HTTP forward, command creation, DLQ writes, raw-message writes, and broker subscriptions remain preview-only.


## MQTT Publish Execution

Milestone 103 allows an explicitly authorized MQTT publish sink. The sink must reference an enabled tenant-owned MQTT connector and the request must include `requested_sink_actions` such as `publish_mqtt` or `mqtt_publish`.

```powershell
$body = @'
{
  "sample_payload": { "device": { "id": "sensor-01" }, "temperature": 31.5 },
  "allow_side_effects": true,
  "requested_sink_actions": ["publish_mqtt"],
  "operator_reason": "controlled lab publish test",
  "approval_reference": "ticket-123"
}
'@

Invoke-RestMethod -Method Post `
  -Uri "http://localhost:8080/flows/<flow-id>/execute" `
  -Headers @{ Authorization = "Bearer <flows-read-and-execute-token>" } `
  -ContentType "application/json" `
  -Body $body
```

The MQTT sink node should include a config similar to:

```json
{
  "kind": "mqtt_publish",
  "connector_id": "<mqtt-connector-id>",
  "topic_template": "devices/{device.id}/temperature",
  "qos": "at_least_once",
  "retain": false
}
```

MQTT publish does not run when `requested_sink_actions` is omitted, even if `allow_side_effects=true`.

## HTTP Forward Execution

Milestone 103 also allows explicitly authorized HTTP forward through an enabled tenant-owned HTTP connector. Only `http://` endpoints are supported in this foundation. HTTPS, custom headers, and secret-backed HTTP credentials are deferred.

The request must include `requested_sink_actions` such as `forward_http` or `http_forward`. The sink node should include:

```json
{
  "kind": "http_forward",
  "connector_id": "<http-connector-id>",
  "method": "POST"
}
```

The connector endpoint or the node `endpoint_url` supplies the destination. Endpoint values are redacted in responses.
