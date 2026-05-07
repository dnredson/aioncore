# Flow Usage

This guide covers the current flow foundation, including CRUD, validation, dry-run, and simulated execute APIs.

## Endpoints

- `POST /flows`
- `GET /flows`
- `GET /flows/{flow_id}`
- `PATCH /flows/{flow_id}`
- `POST /flows/validate`
- `GET /flows/{flow_id}/validation`
- `POST /flows/dry-run`
- `POST /flows/{flow_id}/dry-run`
- `POST /flows/execute`
- `POST /flows/{flow_id}/execute`
- `PUT /flows/{flow_id}/enable`
- `PUT /flows/{flow_id}/disable`
- `DELETE /flows/{flow_id}`

## Create A Flow

Example: MQTT source -> decoder -> internal observation store

```powershell
$body = @{
  flow_key = "mqtt-normalize-store"
  name = "MQTT Normalize Store"
  description = "MQTT uplink to canonical observation storage"
  enabled = $false
  nodes = @(
    @{
      node_id = "source-1"
      node_type = "source"
      name = "MQTT Source"
      config = @{
        kind = "mqtt_subscribe"
        connector_id = "connector-01"
        topic_filter = "devices/+/up"
      }
      position = @{ x = 40; y = 80 }
    },
    @{
      node_id = "decoder-1"
      node_type = "decoder"
      name = "SenML Decode"
      config = @{ kind = "senml_decode" }
      position = @{ x = 220; y = 80 }
    },
    @{
      node_id = "sink-1"
      node_type = "sink"
      name = "Observation Store"
      config = @{ kind = "internal_observation_store" }
      position = @{ x = 420; y = 80 }
    }
  )
  edges = @(
    @{
      edge_id = "edge-1"
      source_node_id = "source-1"
      target_node_id = "decoder-1"
    },
    @{
      edge_id = "edge-2"
      source_node_id = "decoder-1"
      target_node_id = "sink-1"
    }
  )
  metadata = @{
    category = "ingestion"
    notes = "execution not implemented yet"
  }
} | ConvertTo-Json -Depth 8

Invoke-RestMethod -Method Post -Uri "http://localhost:8080/flows" -ContentType "application/json" -Body $body
```

Example: MQTT source -> filter -> MQTT forward sink

```powershell
$body = @{
  flow_key = "mqtt-filter-forward"
  name = "MQTT Filter Forward"
  enabled = $false
  nodes = @(
    @{
      node_id = "source-1"
      node_type = "source"
      config = @{
        kind = "mqtt_subscribe"
        connector_id = "connector-02"
      }
    },
    @{
      node_id = "filter-1"
      node_type = "filter"
      config = @{
        kind = "filter_condition"
        expression = "temperature > 30"
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
    @{ edge_id = "edge-1"; source_node_id = "source-1"; target_node_id = "filter-1" },
    @{ edge_id = "edge-2"; source_node_id = "filter-1"; target_node_id = "sink-1" }
  )
} | ConvertTo-Json -Depth 8

Invoke-RestMethod -Method Post -Uri "http://localhost:8080/flows" -ContentType "application/json" -Body $body
```

The second example stores only configuration. AionCore does not publish to MQTT from flows yet.

## Validation

Use `POST /flows/validate` to validate a proposed flow without saving it.

```powershell
Invoke-RestMethod -Method Post -Uri "http://localhost:8080/flows/validate" -ContentType "application/json" -Body $body
```

Use `GET /flows/{flow_id}/validation` to validate a stored flow.

```powershell
Invoke-RestMethod -Method Get -Uri "http://localhost:8080/flows/<flow-id>/validation"
```

Validation returns:

- `valid`
- `validation_issues`
- `node_inventory`
- `referenced_connectors`
- `planned_sinks`

Validation is read-only. It does not enable, execute, publish, write observations, or create DLQ records.

The static dashboard Flow Builder uses this endpoint before create and shows structured issue detail without auto-saving.
The visual graph layer can also show node-level validation markers when returned issues include `node_id`.
Milestone 97 also lets the constrained builder write known node config fields through typed inspectors before the flow is saved.

## Dry-Run

Use `POST /flows/dry-run` to dry-run a proposed flow, or `POST /flows/{flow_id}/dry-run` for a stored flow.

```powershell
$dryRun = @{
  sample_payload = @{
    temperature = 21.4
  }
  payload_format = "senml-json"
} | ConvertTo-Json -Depth 8

Invoke-RestMethod -Method Post -Uri "http://localhost:8080/flows/<flow-id>/dry-run" -ContentType "application/json" -Body $dryRun
```

Dry-run returns planning-oriented fields such as:

- `execution_supported = false`
- `simulated = true`
- `planned_path`
- `node_plan`
- `planned_sinks`
- `referenced_connectors`
- `would_store_observation`
- `would_publish_mqtt`
- `would_forward_http`
- `would_create_event`
- `would_create_command`
- `would_use_dlq`
- `side_effects_performed = false`

Dry-run does not execute the flow or perform any side effects.

## Execute

Use `POST /flows/execute` for a proposed flow or `POST /flows/{flow_id}/execute` for a stored flow.

```powershell
$execute = @'
{
  "sample_payload": [
    { "n": "temperature", "v": 21.4, "u": "Cel" }
  ],
  "payload_format": "senml-json"
}
'@

Invoke-RestMethod -Method Post -Uri "http://localhost:8080/flows/<flow-id>/execute" -ContentType "application/json" -Body $execute
```

Execute returns input-driven preview fields such as:

- `execution_id`
- `node_results`
- `sink_results`
- `observations_preview`
- `events_preview`
- `commands_preview`
- `dlq_preview`
- `simulated = true`
- `side_effects_performed = false`

Execute is still side-effect-free in this milestone. It does not publish MQTT, call HTTP, create observations, create commands, create events, or write DLQ records.

The static dashboard Flow Builder uses this endpoint for both proposed drafts and stored flows. It surfaces planning fields such as `planned_path`, `planned_sinks`, `referenced_connectors`, and the conceptual sink flags while keeping `execution_supported = false`.
Milestone 95 also lets the static dashboard highlight conceptual sink nodes in its graph layer when these flags are present.
Milestone 97 keeps that same API usage but improves the draft authoring surface with typed inspectors for known source, middle-node, sink, and DLQ kinds.

## List And Get

```powershell
Invoke-RestMethod -Method Get -Uri "http://localhost:8080/flows"
Invoke-RestMethod -Method Get -Uri "http://localhost:8080/flows/<flow-id>"
```

## Update

```powershell
$patch = @{
  name = "Updated MQTT Normalize Store"
  metadata = @{
    updated_by = "ops"
  }
} | ConvertTo-Json -Depth 6

Invoke-RestMethod -Method Patch -Uri "http://localhost:8080/flows/<flow-id>" -ContentType "application/json" -Body $patch
```

## Enable And Disable

```powershell
Invoke-RestMethod -Method Put -Uri "http://localhost:8080/flows/<flow-id>/enable"
Invoke-RestMethod -Method Put -Uri "http://localhost:8080/flows/<flow-id>/disable"
```

## Delete

```powershell
Invoke-WebRequest -Method Delete -Uri "http://localhost:8080/flows/<flow-id>"
```

The static dashboard uses an explicit browser `confirm()` prompt before calling delete.

## Token Mode

Flow routes require dedicated scopes in `AIONCORE_AUTH_MODE=token`:

- `flows:read` for `GET /flows` and `GET /flows/{flow_id}`
- `flows:write` for `POST`, `PATCH`, `PUT /enable`, `PUT /disable`, and `DELETE`
- `admin:all` satisfies both

Read example:

```powershell
$headers = @{ Authorization = "Bearer <token-with-flows-read>" }
Invoke-RestMethod -Method Get -Headers $headers -Uri "http://localhost:8080/flows"
```

Write example:

```powershell
$headers = @{ Authorization = "Bearer <token-with-flows-write>" }
Invoke-RestMethod -Method Post -Headers $headers -Uri "http://localhost:8080/flows" -ContentType "application/json" -Body $body
```

Token-mode behavior:

- missing or invalid bearer token: `401`
- valid token missing required flow scope: `403`
- non-admin principals only see and manage their own tenant flows
- `admin:all` can read and manage flows across tenants

Validation and dry-run scope rules:

- `POST /flows/validate`: `flows:read` or `flows:write`
- `GET /flows/{flow_id}/validation`: `flows:read`
- `POST /flows/dry-run`: `flows:read`
- `POST /flows/{flow_id}/dry-run`: `flows:read`
- `POST /flows/execute`: `flows:read`
- `POST /flows/{flow_id}/execute`: `flows:read`

Validation and dry-run also preserve tenant-aware flow ownership checks for stored flows in token mode.

## Static Dashboard Builder Notes

Milestone 92 adds a form-based builder in `apps/aion-dashboard/` that consumes the existing flow APIs only.

It supports:

- guided source -> zero or more constrained middle nodes -> sink or dlq draft creation
- typed node inspectors for known constrained-builder node kinds
- redacted preview JSON
- constrained SVG graph editing for the current draft
- read-only SVG graph preview for the selected stored flow
- optional advanced JSON override
- proposed validation and dry-run
- stored-flow validation and dry-run
- copy stored flow to a constrained draft when the stored graph is a safe linear chain
- explicit create, enable, disable, and delete operations

It still does not support drag-and-drop, arbitrary graph editing, graph panning/zooming, in-place stored-flow graph editing, or flow execution.
