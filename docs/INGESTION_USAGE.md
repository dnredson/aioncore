# Ingestion Usage

This guide collects the operational ingestion examples that were previously embedded in the root `README.md`.

For the architecture background, also see [Ingestion Model](INGESTION_MODEL.md), [Observation Model](OBSERVATION_MODEL.md), [Persistence Model](PERSISTENCE_MODEL.md), and [Aion Edge Adapter Model](EDGE_ADAPTER_MODEL.md).

## Local Runtime Basics

Default memory backend:

```powershell
cargo run -p aion-api
```

PostgreSQL backend:

```powershell
$env:AIONCORE_STORAGE_BACKEND = "postgres"
$env:AIONCORE_DATABASE_URL = "postgres://user:password@localhost:5432/aioncore"
cargo run -p aion-api
```

Backend selection variables:

- `AIONCORE_STORAGE_BACKEND=memory|postgres`
- `AIONCORE_DATABASE_URL` when `AIONCORE_STORAGE_BACKEND=postgres`
- `AIONCORE_AUTH_MODE=dev|disabled|token`

Health and readiness:

```powershell
Invoke-RestMethod -Method Get -Uri "http://localhost:8080/health"
Invoke-RestMethod -Method Get -Uri "http://localhost:8080/ready"
Invoke-RestMethod -Method Get -Uri "http://localhost:8080/auth/whoami"
```

`/health` is a lightweight liveness check. `/ready` checks storage and MQTT readiness. In memory mode storage returns ready immediately. In postgres mode it verifies database connectivity and does not fall back to memory if the database is unavailable.

## HTTP Ingestion

Simple HTTP ingestion:

```powershell
Invoke-RestMethod `
  -Method Post `
  -Uri "http://localhost:8080/ingest/http" `
  -ContentType "application/json" `
  -Body (@{
    producer_entity_id = "11111111-1111-1111-1111-111111111111"
    feature_of_interest_id = "22222222-2222-2222-2222-222222222222"
    payload = @(
      @{
        bn = "urn:aion:farm:01:soil-sensor:01:"
        n = "soil_moisture"
        u = "%"
        v = 19.4
      }
    )
  } | ConvertTo-Json -Depth 8)
```

Supported payload formats in this milestone are `senml-json`, `ultralight`, and `canonical-json`. SenML can decode without a mapping. UltraLight ingestion requires a stored producer payload profile mapping.

## Connector Registry

HTTP connector:

```powershell
$httpConnector = Invoke-RestMethod `
  -Method Post `
  -Uri "http://localhost:8080/ingestion/connectors" `
  -ContentType "application/json" `
  -Body (@{
    connector_key = "farm-http"
    connector_type = "http"
    connector_profile = "custom"
    enabled = $true
    protocol = "http"
    http_path = "/ingestion/connectors/{connector_id}/ingest"
    payload_format = "senml-json"
    content_type = "application/senml+json"
    default_producer_entity_id = "11111111-1111-1111-1111-111111111111"
    default_feature_of_interest_id = "22222222-2222-2222-2222-222222222222"
  } | ConvertTo-Json -Depth 8)
```

Generic MQTT connector:

```powershell
$genericMqttConnector = Invoke-RestMethod `
  -Method Post `
  -Uri "http://localhost:8080/ingestion/connectors" `
  -ContentType "application/json" `
  -Body (@{
    connector_key = "farm-mqtt"
    connector_type = "mqtt"
    connector_profile = "generic-aion-mqtt"
    enabled = $false
    broker_url = "mqtt://127.0.0.1:1883"
    client_id = "aioncore-farm-mqtt"
    topic_filter = "aioncore/+/+/data"
    payload_format = "senml-json"
  } | ConvertTo-Json -Depth 8)
```

Connector inspection example:

```powershell
Invoke-RestMethod `
  -Method Get `
  -Uri "http://localhost:8080/ingestion/connectors/$($httpConnector.id)"
```

## Connector-Aware HTTP Ingestion

Connector-aware HTTP ingestion can use connector defaults:

```powershell
$ingest = Invoke-RestMethod `
  -Method Post `
  -Uri "http://localhost:8080/ingestion/connectors/$($httpConnector.id)/ingest" `
  -ContentType "application/json" `
  -Body (@{
    payload = @(
      @{
        n = "soil_moisture"
        u = "%"
        v = 18.5
      }
    )
  } | ConvertTo-Json -Depth 8)

$raw = Invoke-RestMethod `
  -Method Get `
  -Uri "http://localhost:8080/raw-messages/$($ingest.raw_message_id)"

$raw.connector_id
$raw.connector_key
$raw.connector_profile
```

Another connector-aware example with an explicit connector record:

```powershell
$createdConnector = Invoke-RestMethod `
  -Method Post `
  -Uri "http://localhost:8080/ingestion/connectors" `
  -ContentType "application/json" `
  -Body (@{
    connector_key = "field-http-01"
    connector_type = "http"
    connector_profile = "custom"
    enabled = $true
    protocol = "http"
    endpoint = "/ingestion/connectors/{connector_id}/ingest"
    http_path = "/ingestion/connectors/{connector_id}/ingest"
    payload_format = "senml-json"
    content_type = "application/senml+json"
  } | ConvertTo-Json -Depth 8)

Invoke-RestMethod `
  -Method Post `
  -Uri "http://localhost:8080/ingestion/connectors/$($createdConnector.id)/ingest" `
  -ContentType "application/json" `
  -Body (@{
    payload = @(
      @{
        bn = "urn:aion:farm:01:soil-sensor:01:"
        n = "soil_moisture"
        u = "%"
        v = 19.4
      }
    )
  } | ConvertTo-Json -Depth 8)
```

## Raw Messages And Observations

Inspect normalized and preserved records after ingestion:

```powershell
$observations = Invoke-RestMethod `
  -Method Get `
  -Uri "http://localhost:8080/observations?raw_message_id=$($ingest.raw_message_id)"

$raw = Invoke-RestMethod `
  -Method Get `
  -Uri "http://localhost:8080/raw-messages/$($ingest.raw_message_id)"

$events = Invoke-RestMethod `
  -Method Get `
  -Uri "http://localhost:8080/events?raw_message_id=$($ingest.raw_message_id)"
```

This is the key ingestion invariant: raw messages are stored before normalization, and valid telemetry is then materialized as canonical observations and related events.

## MQTT Ingestion Via Environment Variables

MQTT ingestion is optional and remains disabled unless you enable it explicitly:

```powershell
$env:AIONCORE_MQTT_ENABLED = "true"
$env:AIONCORE_MQTT_BROKER_URL = "mqtt://127.0.0.1:1883"
$env:AIONCORE_MQTT_CLIENT_ID = "aioncore-ingest"
$env:AIONCORE_MQTT_TOPIC_FILTER = "aioncore/+/+/data"
$env:AIONCORE_MQTT_PAYLOAD_FORMAT = "senml-json"
cargo run -p aion-api
```

If `AIONCORE_MQTT_ENABLED` is unset or false, the API starts as before and does not attempt a broker connection.

Optional broker username/password authentication:

```powershell
$env:AIONCORE_MQTT_ENABLED = "true"
$env:AIONCORE_MQTT_BROKER_URL = "mqtt://127.0.0.1:1883"
$env:AIONCORE_MQTT_USERNAME = "aioncore-worker"
$env:AIONCORE_MQTT_PASSWORD = "change-me"
cargo run -p aion-api
```

The password is used only to authenticate the worker to the broker and is not logged by runtime config debug output. This is not per-device authorization.

Topic convention:

```text
aioncore/{producer_entity_id}/{feature_of_interest_id}/data
```

Example publish with `mosquitto_pub`:

```powershell
mosquitto_pub.exe `
  -h 127.0.0.1 `
  -p 1883 `
  -t "aioncore/11111111-1111-1111-1111-111111111111/22222222-2222-2222-2222-222222222222/data" `
  -m '{ "e": [ { "n": "water_level", "u": "%", "v": 12 } ] }' `
  -V mqttv5
```

Broker-authenticated publish:

```powershell
mosquitto_pub.exe `
  -h 127.0.0.1 `
  -p 1883 `
  -u "aioncore-worker" `
  -P "change-me" `
  -t "aioncore/11111111-1111-1111-1111-111111111111/22222222-2222-2222-2222-222222222222/data" `
  -m '{ "e": [ { "n": "water_level", "u": "%", "v": 12 } ] }' `
  -V mqttv5
```

## Dynamic Connector Workers

Planner example:

```powershell
$plan = Invoke-RestMethod `
  -Method Get `
  -Uri "http://localhost:8080/ingestion/workers/plan"

$plan.planned_workers
$plan.skipped_workers
$plan.invalid_workers
$plan.unsupported_workers
$plan.specs | Select-Object connector_key, worker_kind, status, validation_issues
```

Planner behavior:

- disabled connectors appear as `skipped`
- valid HTTP connectors plan `http_listener`
- valid MQTT connectors plan `mqtt_subscriber`
- TTN v3 connectors with `payload_format = "ttn-uplink-json"` plan `mqtt_subscriber`
- missing `broker_url` or `topic_filter` makes MQTT specs `invalid`

Connector workers are disabled unless explicitly enabled:

```powershell
$env:AIONCORE_CONNECTOR_WORKERS_ENABLED = "false"
cargo run -p aion-api
```

Enable connector-based MQTT workers:

```powershell
$env:AIONCORE_CONNECTOR_WORKERS_ENABLED = "true"
cargo run -p aion-api
```

If both `AIONCORE_MQTT_ENABLED=true` and `AIONCORE_CONNECTOR_WORKERS_ENABLED=true` are set, the env-var MQTT worker and connector-based MQTT workers may run at the same time.

Runtime reconciliation:

```powershell
$reconcile = Invoke-RestMethod `
  -Method Post `
  -Uri "http://localhost:8080/ingestion/workers/reconcile"

$reconcile.actions
```

Worker inspection:

```powershell
Invoke-RestMethod -Method Get -Uri "http://localhost:8080/ingestion/workers/plan"
Invoke-RestMethod -Method Get -Uri "http://localhost:8080/ingestion/workers/status"
Invoke-RestMethod -Method Get -Uri "http://localhost:8080/ready"
```

Status fields include `planned`, `starting`, `running`, `reconnecting`, `stopped`, `degraded`, `skipped`, `invalid`, `error`, or `unsupported` per connector, plus reconnect and restart diagnostics.

## Connector Secret Example

Connector MQTT workers can use connector secret references for broker username/password authentication. Secret values are accepted on write, stored outside the `IngestionConnector` record itself, and never returned by the API:

```powershell
$brokerSecret = Invoke-RestMethod `
  -Method Post `
  -Uri "http://localhost:8080/secrets/connectors" `
  -ContentType "application/json" `
  -Body (@{
    secret_key = "farm-broker-basic-auth"
    secret_type = "mqtt_basic_auth"
    username = "aioncore-worker"
    secret_value = "change-me"
    metadata = @{
      purpose = "local broker auth"
    }
  } | ConvertTo-Json -Depth 8)
```

Create a connector that references the secret by ID:

```powershell
$genericMqttConnector = Invoke-RestMethod `
  -Method Post `
  -Uri "http://localhost:8080/ingestion/connectors" `
  -ContentType "application/json" `
  -Body (@{
    connector_key = "farm-mqtt-auth"
    connector_type = "mqtt"
    connector_profile = "generic-mqtt"
    enabled = $true
    broker_url = "mqtt://127.0.0.1:1883"
    client_id = "aioncore-farm-mqtt-auth"
    topic_filter = "aioncore/+/+/data"
    payload_format = "senml-json"
    secret_ref_id = $brokerSecret.id
  } | ConvertTo-Json -Depth 8)
```

Inspect the connector and worker status without exposing the secret:

```powershell
Invoke-RestMethod `
  -Method Get `
  -Uri "http://localhost:8080/ingestion/connectors/$($genericMqttConnector.id)"

Invoke-RestMethod `
  -Method Get `
  -Uri "http://localhost:8080/ingestion/workers/status"
```

## Edge Adapter Note

The Aion Edge Adapter remains optional future work for edge and fog deployments. It is not required by the current AionCore runtime. Current server-side ingestion paths remain valid without it:

- direct HTTP ingestion
- connector-aware HTTP ingestion
- environment MQTT ingestion
- dynamic MQTT connector workers

See [Aion Edge Adapter Model](EDGE_ADAPTER_MODEL.md) for the future adapter contract.

## See Also

- [Ingestion Model](INGESTION_MODEL.md)
- [Observation Model](OBSERVATION_MODEL.md)
- [Persistence Model](PERSISTENCE_MODEL.md)
- [Aion Edge Adapter Model](EDGE_ADAPTER_MODEL.md)
- [ADR 0003: Payload-Agnostic Ingestion](ADR/0003-payload-agnostic-ingestion.md)
- [ADR 0023: Ingestion Connector Registry and Profiles](ADR/0023-ingestion-connector-registry-and-profiles.md)
- [ADR 0024: PostgreSQL Ingestion Connector Persistence](ADR/0024-postgresql-ingestion-connector-persistence.md)
- [ADR 0029: Connector Secret References](ADR/0029-connector-secret-references.md)
