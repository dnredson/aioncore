# Flow Validation And Dry-Run Usage

This guide covers the Milestone 87 flow validation and dry-run API foundation.

## Intent

These endpoints support future dashboard and flow-builder workflows by letting operators inspect a stored or proposed flow without executing it.

They are read-only and planning-oriented.

## Endpoints

- `POST /flows/validate`
- `GET /flows/{flow_id}/validation`
- `POST /flows/dry-run`
- `POST /flows/{flow_id}/dry-run`

Related dashboard read-only endpoints:

- `GET /dashboard/flows`
- `GET /dashboard/flows/{flow_id}`

## Validation Vs Dry-Run

Validation answers: is the graph structurally usable?

Dry-run answers: what path, sinks, and side effects would this flow conceptually imply if execution existed?

Neither endpoint executes the flow.

## No Side Effects

Validation and dry-run do not:

- subscribe to brokers
- publish MQTT
- forward HTTP
- create observations
- create events
- create commands
- write DLQ records
- change flow enablement or stored flow state

Dry-run always returns:

- `execution_supported = false`
- `simulated = true`
- `side_effects_performed = false`

## Proposed Flow Validation

```powershell
$body = @{
  flow_key = "mqtt-normalize-store"
  name = "MQTT Normalize Store"
  nodes = @(
    @{
      node_id = "source-1"
      node_type = "source"
      config = @{
        kind = "mqtt_subscribe"
        connector_id = "connector-01"
      }
    },
    @{
      node_id = "sink-1"
      node_type = "sink"
      config = @{
        kind = "internal_observation_store"
      }
    }
  )
  edges = @(
    @{
      edge_id = "edge-1"
      source_node_id = "source-1"
      target_node_id = "sink-1"
    }
  )
} | ConvertTo-Json -Depth 8

Invoke-RestMethod -Method Post -Uri "http://localhost:8080/flows/validate" -ContentType "application/json" -Body $body
```

## Stored Flow Validation

```powershell
Invoke-RestMethod -Method Get -Uri "http://localhost:8080/flows/<flow-id>/validation"
```

## Proposed Dry-Run

```powershell
$body = @{
  flow_key = "mqtt-forward"
  nodes = @(
    @{
      node_id = "source-1"
      node_type = "source"
      config = @{
        kind = "mqtt_subscribe"
      }
    },
    @{
      node_id = "sink-1"
      node_type = "sink"
      config = @{
        kind = "mqtt_publish"
        topic = "alerts/high-temperature"
      }
    }
  )
  edges = @(
    @{
      source_node_id = "source-1"
      target_node_id = "sink-1"
    }
  )
  sample_payload = @{
    temperature = 31.2
  }
  payload_format = "application/json"
} | ConvertTo-Json -Depth 8

Invoke-RestMethod -Method Post -Uri "http://localhost:8080/flows/dry-run" -ContentType "application/json" -Body $body
```

## Stored Dry-Run

```powershell
$body = @{
  sample_payload = @{
    temperature = 31.2
  }
  payload_format = "application/json"
} | ConvertTo-Json -Depth 8

Invoke-RestMethod -Method Post -Uri "http://localhost:8080/flows/<flow-id>/dry-run" -ContentType "application/json" -Body $body
```

## Structured Issues

Validation and dry-run include `validation_issues` entries with:

- `severity`
- `code`
- `message`
- optional `node_id`
- optional `edge_id`
- optional `field`

## Redaction

Secret-like config keys are recursively redacted before they are returned in validation or dry-run output.

Redacted key patterns include:

- `password`
- `secret`
- `token`
- `api_key`
- `access_key`
- `private_key`
- `credential`

## Future Dashboard Usage

A future dashboard or Node-RED-like flow builder should use:

- `GET /flows/{flow_id}/validation` for stored flow inspection
- `POST /flows/{flow_id}/dry-run` for planning-oriented previews
- `POST /flows/validate` and `POST /flows/dry-run` for unsaved draft graphs

The dashboard flow inventory and detail endpoints complement these APIs. They provide inventory counts, redacted node detail, graph summaries, and validation summaries for saved flows, but they still do not execute anything and are not a substitute for the validation and dry-run endpoints.

## Static Flow Builder Usage

Milestone 92 adds a static Flow Builder foundation in `apps/aion-dashboard/`.

The UI uses:

- `POST /flows/validate` for unsaved proposed flows
- `POST /flows/dry-run` for unsaved proposed flows with optional `sample_payload`
- `GET /flows/{flow_id}/validation` for stored flows
- `POST /flows/{flow_id}/dry-run` for stored flows with optional `sample_payload`

The builder remains form-based and intentionally does not implement drag-and-drop, arbitrary visual graph editing, or execution. It shows redacted preview JSON and result panels so operators can inspect `validation_issues`, `planned_path`, `planned_sinks`, and connector references before taking any write action.

Milestone 95 extends that static usage with a read-only visual graph layer:

- proposed drafts can render as a dependency-free SVG graph before validation
- stored flows can render from `GET /dashboard/flows/{flow_id}` and be enriched by `GET /flows/{flow_id}/validation`
- `validation_issues` with `node_id` can be surfaced as node-level issue markers and node detail warnings
- dry-run sink flags can be surfaced as graph highlights for conceptual effect inspection only

This visual layer does not execute flows, edit graph structure, or introduce any side effects.
