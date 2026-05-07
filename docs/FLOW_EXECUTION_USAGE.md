# Flow Execution Usage

This guide covers the Milestone 98 simulated flow execution API foundation.

## Endpoints

- `POST /flows/execute`
- `POST /flows/{flow_id}/execute`

## Key Rules

- execution is always simulated in this milestone
- `simulated` always returns `true`
- `side_effects_performed` always returns `false`
- no MQTT publish, HTTP forward, command creation, observation storage, or DLQ write happens

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
- no dashboard execution UI is added in this milestone
