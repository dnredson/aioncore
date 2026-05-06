# Flow Usage

This guide covers the Milestone 82 flow and pipeline model foundation.

## Endpoints

- `POST /flows`
- `GET /flows`
- `GET /flows/{flow_id}`
- `PATCH /flows/{flow_id}`
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
