# Dashboard Usage

This guide covers the Milestone 81 read-only dashboard API foundation.

## Scope

- `GET /dashboard/overview`
- `GET /dashboard/timeseries/entities`
- `GET /dashboard/connectors/overview`

These endpoints do not replace `/timeseries/query`. They provide compact summaries that a future dashboard UI can use for discovery and navigation.

## Overview

```powershell
Invoke-RestMethod -Method Get -Uri "http://localhost:8080/dashboard/overview"
```

Example response:

```json
{
  "entities_count": 12,
  "observations_count": 482,
  "raw_messages_count": 241,
  "events_count": 38,
  "flows_count": 4,
  "enabled_flows_count": 2,
  "dlq_pending_count": 3,
  "dlq_total_count": 7,
  "connectors_count": 3,
  "enabled_connectors_count": 2,
  "workers_running_count": 1,
  "workers_degraded_count": 1,
  "generated_at": "2026-05-06T12:00:00Z"
}
```

## Time-Series Entity Discovery

```powershell
Invoke-RestMethod -Method Get -Uri "http://localhost:8080/dashboard/timeseries/entities"
```

Example response:

```json
{
  "generated_at": "2026-05-06T12:00:00Z",
  "entities": [
    {
      "entity_id": "7d3df2c8-d59c-4b78-9686-f81cfc313ca5",
      "entity_key": "plot-north-01",
      "entity_type": "aion:Plot",
      "display_name": "North Plot",
      "observed_property_count": 2,
      "observation_count": 42,
      "last_observed_at": "2026-05-05T12:42:00Z",
      "properties": [
        {
          "observed_property": "soil.moisture",
          "observation_count": 21,
          "last_observed_at": "2026-05-05T12:42:00Z",
          "units": ["%"]
        }
      ]
    }
  ]
}
```

## Connector Overview

```powershell
Invoke-RestMethod -Method Get -Uri "http://localhost:8080/dashboard/connectors/overview"
```

Example response:

```json
{
  "generated_at": "2026-05-06T12:00:00Z",
  "connectors": [
    {
      "connector_id": "7738c91a-40dd-444f-aa40-69442646cba8",
      "connector_key": "field-mqtt-01",
      "connector_type": "mqtt",
      "connector_profile": "generic_mqtt",
      "enabled": true,
      "status": "reconnecting",
      "readiness": "reconnecting",
      "broker_url": "mqtt://broker.example:1883",
      "topic_filter": "sensors/+/up",
      "payload_format": "senml-json",
      "worker_kind": "mqtt_subscriber",
      "worker_status": "reconnecting",
      "running": false,
      "reconnecting": true,
      "degraded": true,
      "last_error": "connection timeout",
      "secret_configured": true
    }
  ]
}
```

Secret values are never returned. `broker_url` is intended for safe display and redacts embedded credentials.

## Token Mode

Dashboard endpoints require `dashboard:read` in `AIONCORE_AUTH_MODE=token`.

```powershell
$headers = @{ Authorization = "Bearer <token-with-dashboard-read>" }
Invoke-RestMethod -Method Get -Headers $headers -Uri "http://localhost:8080/dashboard/overview"
```

Token-mode behavior:

- missing or invalid bearer token: `401`
- valid token without `dashboard:read`: `403`
- `admin:all`: allowed
- non-admin principals: limited to dashboard data owned by the principal tenant

The overview response now also includes flow and DLQ inventory counts. It still does not render or execute flows and does not provide a dashboard UI yet.
