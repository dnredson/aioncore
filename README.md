# AionCore

AionCore is an open-source, AI-native IoT platform for interoperable sensing and closed-loop decision support.

The platform combines JSON-LD domain entities, payload-agnostic ingestion, canonical observations, and MCP-ready semantic context. Its first MVP focuses on a small but durable foundation: register domain entities, ingest telemetry from multiple payload formats, store every raw message before normalization, and expose normalized observations for applications and AI-facing tools.

## MVP Scope

The initial MVP includes:

- JSON-LD domain entity registry.
- Entity relationship registry.
- Raw message storage before normalization.
- Canonical observation model.
- Payload decoder interface.
- Initial decoder designs for SenML JSON, UltraLight, and JSON mapping.
- HTTP ingestion.
- MQTT ingestion foundation for local runtime, disabled by default.
- PostgreSQL and TimescaleDB persistence design.
- Docker Compose local development design.
- Minimal read-only MCP design for querying entities and observations.

The first MVP does not include:

- A dashboard.
- A complex rule engine.
- Paid cloud dependencies.
- LLM-controlled critical actions by default.

## Architecture Direction

AionCore starts as a modular monolith implemented in Rust, with service boundaries kept explicit from the beginning. The first deployable should be an all-in-one API process that contains context, ingest, normalizer, observation, MCP, gateway, and identity modules behind clear internal interfaces.

The architecture should later support distributed deployment, where HTTP ingestion, MQTT ingestion, normalization, observation storage, public API, and MCP serving can become separate services.

## Documentation

- [Architecture](docs/ARCHITECTURE.md)
- [Domain Model](docs/DOMAIN_MODEL.md)
- [Observation Model](docs/OBSERVATION_MODEL.md)
- [Ingestion Model](docs/INGESTION_MODEL.md)
- [Aion Edge Adapter Model](docs/EDGE_ADAPTER_MODEL.md)
- [SmartSentinel Integration](docs/SMARTSENTINEL_INTEGRATION.md)
- [Persistence Model](docs/PERSISTENCE_MODEL.md)
- [AI and MCP Model](docs/AI_MCP_MODEL.md)
- [Security Model](docs/SECURITY_MODEL.md)
- [Architecture Decision Records](docs/ADR)

## Key Principles

- Domain/context entities are represented using JSON-LD.
- Sensor payloads are payload-agnostic at ingestion.
- All valid telemetry is normalized into canonical observations.
- Raw messages are always stored before normalization.
- Multiple payload decoders are supported.
- MCP/LLM integration is read-oriented by default.
- Critical actions require explicit non-default control paths.

## Suggested Technology Stack

- Backend: Rust with Axum.
- Database: PostgreSQL with TimescaleDB.
- Messaging/event bus: NATS.
- MQTT broker: Mosquitto or EMQX.
- Deployment: Docker Compose first.

## Project Status

AionCore currently has a minimal Rust workspace, core domain models, SQL migrations, and a local in-memory API runtime for early testing.

The PostgreSQL and TimescaleDB migration foundation now covers the current in-memory models, including commands, actions, events, executor agents, command leases, and rules. Runtime persistence is not wired yet; the local API still uses in-memory storage.

## Authentication Status

Most current AionCore APIs are unauthenticated. This is acceptable for local development and tests only, not for public or production exposure.

Local development warning:

- run the API on trusted local networks only
- do not expose `/mcp`, `/mcp/tools`, or `/ai/context/*` publicly
- do not treat current connector-secret redaction as a complete production security model

The planned production direction is documented in [Security Model](docs/SECURITY_MODEL.md) and [ADR 0044](docs/ADR/0044-security-model-and-auth-roadmap.md). The staged roadmap starts with auth middleware and a development-mode bypass, then adds API tokens, machine-principal protection for adapters and executors, connector-secret protection, and MCP hardening.

## Run Locally Without Docker

The default local runtime uses in-memory storage. It does not require Docker, PostgreSQL, TimescaleDB, NATS, or Mosquitto.

Run the default memory backend:

```powershell
cargo run -p aion-api
```

Run the PostgreSQL backend:

```powershell
$env:AIONCORE_STORAGE_BACKEND = "postgres"
$env:AIONCORE_DATABASE_URL = "postgres://user:password@localhost:5432/aioncore"
cargo run -p aion-api
```

The backend selection variables are:

- `AIONCORE_STORAGE_BACKEND=memory|postgres`
- `AIONCORE_DATABASE_URL` when `AIONCORE_STORAGE_BACKEND=postgres`

If `AIONCORE_STORAGE_BACKEND` is unset, the API uses memory.

Health and readiness:

```powershell
Invoke-RestMethod -Method Get -Uri "http://localhost:8080/health"
Invoke-RestMethod -Method Get -Uri "http://localhost:8080/ready"
```

`/health` is a lightweight liveness check. It reports the active storage backend and should not perform a database probe. `/ready` checks storage and MQTT readiness. In memory mode storage returns ready immediately. In postgres mode it verifies database connectivity and does not fall back to memory if the database is unavailable. When MQTT is disabled, `/ready` still succeeds and reports `mqtt.enabled = false`. Connector workers are also disabled by default and reported under `connector_workers.enabled = false`.

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

Broker username/password authentication is optional:

```powershell
$env:AIONCORE_MQTT_ENABLED = "true"
$env:AIONCORE_MQTT_BROKER_URL = "mqtt://127.0.0.1:1883"
$env:AIONCORE_MQTT_USERNAME = "aioncore-worker"
$env:AIONCORE_MQTT_PASSWORD = "change-me"
cargo run -p aion-api
```

The password is only used to authenticate the AionCore worker to the broker and is not logged by the runtime config debug output. This is not per-device authorization; device-level MQTT authorization is future work.

With MQTT enabled, `/ready` reports the worker state:

- ready when `mqtt.connected = true` and `mqtt.subscribed = true`
- not ready when MQTT is enabled but the worker has not connected or subscribed
- ready with `mqtt.enabled = false` when MQTT is disabled

The topic convention for this MVP is:

```text
aioncore/{producer_entity_id}/{feature_of_interest_id}/data
```

Example SenML publish with `mosquitto_pub` if it is installed:

```powershell
mosquitto_pub.exe `
  -h 127.0.0.1 `
  -p 1883 `
  -t "aioncore/11111111-1111-1111-1111-111111111111/22222222-2222-2222-2222-222222222222/data" `
  -m '{ "e": [ { "n": "water_level", "u": "%", "v": 12 } ] }' `
  -V mqttv5
```

For a broker that requires username/password:

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

Supported payload formats in this milestone are `senml-json`, `ultralight`, and `canonical-json`. SenML can decode without a mapping. UltraLight ingestion requires a stored producer payload profile mapping. MQTT worker broker authentication is supported, but per-device MQTT authorization, TLS/mTLS, and command publishing are future work.

Ingestion connector registry examples:

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

```powershell
$ttnConnector = Invoke-RestMethod `
  -Method Post `
  -Uri "http://localhost:8080/ingestion/connectors" `
  -ContentType "application/json" `
  -Body (@{
    connector_key = "ttn-demo"
    connector_type = "mqtt"
    connector_profile = "ttn-v3"
    enabled = $false
    broker_url = "mqtt://eu1.cloud.thethings.network:1883"
    topic_filter = "v3/demo-application/devices/+/up"
    payload_format = "ttn-uplink-json"
  } | ConvertTo-Json -Depth 8)
```

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

## Aion Edge Adapter

The Aion Edge Adapter is future optional work for edge/fog deployments that need local collection from multiple protocols and brokers before publishing to AionCore. It is intended for sources such as MQTT, HTTP, CoAP, serial, SDI-12, CSV, UltraLight, TTN JSON, ChirpStack JSON, and future parser plugins.

The adapter is not required by the AionCore runtime. Current server-side ingestion remains valid: direct HTTP ingestion, connector-aware HTTP ingestion, environment MQTT ingestion, and dynamic MQTT connector workers can continue to run without an edge adapter.

The future adapter model covers local parser plugins, output modes such as `senml-json`, `canonical-json`, and future `aion-observation-batch`, local DLQ/offline buffering, retry/backoff behavior, safe local credential handling, and publishing to AionCore HTTP or MQTT ingestion. See [Aion Edge Adapter Model](docs/EDGE_ADAPTER_MODEL.md).

Edge adapter registration and status reporting are documented as a future optional contract and do not change current runtime behavior:

```powershell
$adapter = Invoke-RestMethod `
  -Method Post `
  -Uri "http://localhost:8080/adapters" `
  -ContentType "application/json" `
  -Body (@{
    adapter_key = "fog-01-mqtt"
    display_name = "Fog 01 MQTT Adapter"
    adapter_type = "edge"
    status = "online"
    version = "1.0.0"
    host_id = "fog-01"
    site_id = "site-01"
    environment = "fog"
    metadata = @{
      source = "manual-registration"
    }
  } | ConvertTo-Json -Depth 8)

$heartbeat = Invoke-RestMethod `
  -Method Put `
  -Uri "http://localhost:8080/adapters/$($adapter.adapter.id)/heartbeat" `
  -ContentType "application/json" `
  -Body (@{
    status = "degraded"
    observed_at = "2026-04-29T15:00:00Z"
    dlq_depth = 7
    dlq_oldest_record_at = "2026-04-29T14:30:00Z"
    last_publish_success_at = "2026-04-29T14:59:00Z"
    last_error = "broker unavailable"
    metadata = @{
      dlq_replayed = $false
    }
  } | ConvertTo-Json -Depth 8)

Invoke-RestMethod -Method Get -Uri "http://localhost:8080/adapters/$($adapter.adapter.id)/status"
Invoke-RestMethod -Method Get -Uri "http://localhost:8080/events?event_type=aion:EdgeAdapterHeartbeat"
```

Current server-side connectors remain valid and continue to work independently of this future adapter path.

Optional SmartSentinel snapshot ingestion can be tested without a SmartSentinel runtime. AionCore stores the full snapshot as a raw message and maps selected operational-domain data into domain-agnostic entities, relationships, observations, and events:

```powershell
$snapshot = @{
  snapshot_id = "snap-001"
  node_id = "fog-01"
  observed_at = "2026-04-29T12:00:00Z"
  entities = @(
    @{
      id = "host:fog-01"
      type = "sentinel:Host"
      name = "fog-01"
      properties = @{}
    }
    @{
      id = "service:mosquitto"
      type = "sentinel:Service"
      name = "mosquitto"
      status = "healthy"
      properties = @{}
    }
  )
  relationships = @(
    @{
      source = "host:fog-01"
      type = "sentinel:runs"
      target = "service:mosquitto"
    }
  )
  observations = @(
    @{
      entity_id = "service:mosquitto"
      observed_property = "sentinel:ServiceStatus"
      value = "healthy"
      observed_at = "2026-04-29T12:00:01Z"
    }
  )
  events = @(
    @{
      event_type = "sentinel:ServiceDegraded"
      target_entity_id = "service:mosquitto"
      severity = "warning"
      message = "API service degraded"
    }
  )
}

$sentinelIngest = Invoke-RestMethod `
  -Method Post `
  -Uri "http://localhost:8080/integrations/smartsentinel/snapshots" `
  -ContentType "application/json" `
  -Body ($snapshot | ConvertTo-Json -Depth 12)

$sentinelIngest
$sentinelIngest.relationships_created
$sentinelIngest.relationships_reused
```

Query the materialized records:

```powershell
$entities = Invoke-RestMethod -Method Get -Uri "http://localhost:8080/entities"
$service = $entities | Where-Object { $_.entity_key -eq "smartsentinel:fog-01:service:mosquitto" }

Invoke-RestMethod -Method Get -Uri "http://localhost:8080/raw-messages/$($sentinelIngest.raw_message_id)"
Invoke-RestMethod -Method Get -Uri "http://localhost:8080/entities"
Invoke-RestMethod -Method Get -Uri "http://localhost:8080/observations?feature_of_interest_id=$($service.id)"
Invoke-RestMethod -Method Get -Uri "http://localhost:8080/events?raw_message_id=$($sentinelIngest.raw_message_id)"
Invoke-RestMethod -Method Get -Uri "http://localhost:8080/ai/context/entity/$($service.id)"
```

Submit the same snapshot again to verify relationship de-duplication:

```powershell
$sentinelIngestAgain = Invoke-RestMethod `
  -Method Post `
  -Uri "http://localhost:8080/integrations/smartsentinel/snapshots" `
  -ContentType "application/json" `
  -Body ($snapshot | ConvertTo-Json -Depth 12)

$sentinelIngestAgain.relationships_created
$sentinelIngestAgain.relationships_reused
$sentinelIngestAgain.entities_reused
```

Submit an invalid snapshot to inspect structured validation errors:

```powershell
$invalidSnapshot = @{
  snapshot_id = "snap-invalid"
  observed_at = "2026-04-29T12:00:00Z"
  entities = @()
}

try {
  Invoke-RestMethod `
    -Method Post `
    -Uri "http://localhost:8080/integrations/smartsentinel/snapshots" `
    -ContentType "application/json" `
    -Body ($invalidSnapshot | ConvertTo-Json -Depth 12)
} catch {
  $body = $_.ErrorDetails.Message | ConvertFrom-Json
  $body.error
  $body.validation_errors
}
```

Submit a snapshot with provenance and evidence references:

```powershell
$snapshotWithEvidence = @{
  snapshot_id = "snap-evidence-001"
  node_id = "fog-02"
  observed_at = "2026-04-29T13:00:00Z"
  source = @{
    agent_id = "agent-fog-02"
    agent_version = "0.4.2"
    host_id = "fog-02"
    environment = "fog"
    collector = "smartsentinel-snapshot"
  }
  provenance = @{
    run_id = "run-42"
    cycle_id = "cycle-7"
    trace_id = "trace-abc"
    correlation_id = "corr-123"
    workflow_id = "wf-remediate"
    external_refs = @(
      @{ system = "incident-platform"; external_id = "inc-001" }
    )
  }
  evidence = @(
    @{
      evidence_id = "ev-log-1"
      evidence_type = "log"
      title = "API error log"
      uri = "https://evidence.example.invalid/logs/api"
      external_id = "log-001"
      collected_at = "2026-04-29T13:00:02Z"
      related_entity_id = "service:api"
      metadata = @{ line_count = 20 }
    }
  )
  entities = @(
    @{ id = "host:fog-02"; type = "sentinel:Host"; name = "fog-02"; properties = @{} }
    @{ id = "service:api"; type = "sentinel:Service"; name = "api"; status = "degraded"; properties = @{} }
  )
  relationships = @(
    @{ source = "host:fog-02"; type = "sentinel:runs"; target = "service:api" }
  )
  observations = @(
    @{
      entity_id = "service:api"
      observed_property = "sentinel:LatencyP95"
      value = 832
      unit = "ms"
      observed_at = "2026-04-29T13:00:03Z"
      evidence_refs = @("ev-log-1")
      source = @{ collector = "metrics-summary" }
    }
  )
  events = @(
    @{
      event_type = "sentinel:IncidentOpened"
      target_entity_id = "service:api"
      severity = "warning"
      message = "API latency degraded"
      incident_id = "inc-001"
      alert_id = "alert-001"
      workflow_id = "wf-remediate"
      run_id = "run-42"
      trace_id = "trace-abc"
      evidence_refs = @("ev-log-1")
    }
  )
}

$evidenceIngest = Invoke-RestMethod `
  -Method Post `
  -Uri "http://localhost:8080/integrations/smartsentinel/snapshots" `
  -ContentType "application/json" `
  -Body ($snapshotWithEvidence | ConvertTo-Json -Depth 16)

$evidenceIngest.provenance_present
$evidenceIngest.evidence_count
$evidenceIngest.correlation_id
$evidenceIngest.trace_id
$evidenceIngest.run_id
$evidenceIngest.cycle_id
```

Query evidence/provenance metadata through events, observations, and AI context. AionCore stores evidence references only; it does not fetch evidence URLs.

```powershell
$entities = Invoke-RestMethod -Method Get -Uri "http://localhost:8080/entities"
$apiService = $entities | Where-Object { $_.entity_key -eq "smartsentinel:fog-02:service:api" }

$events = Invoke-RestMethod -Method Get -Uri "http://localhost:8080/events?raw_message_id=$($evidenceIngest.raw_message_id)"
$observations = Invoke-RestMethod -Method Get -Uri "http://localhost:8080/observations?feature_of_interest_id=$($apiService.id)"
$aiContext = Invoke-RestMethod -Method Get -Uri "http://localhost:8080/ai/context/entity/$($apiService.id)"

$events | Select-Object event_type, metadata
$observations | Select-Object observed_property, metadata
$aiContext.recent_events | Select-Object event_type, metadata
```

Query SmartSentinel operational provenance directly by external references:

```powershell
$incidentEvents = Invoke-RestMethod -Method Get -Uri "http://localhost:8080/events?incident_id=inc-001"
$alertEvents = Invoke-RestMethod -Method Get -Uri "http://localhost:8080/events?alert_id=alert-001"
$traceRawMessages = Invoke-RestMethod -Method Get -Uri "http://localhost:8080/raw-messages?trace_id=trace-abc&run_id=run-42&cycle_id=cycle-7"
$provenanceSearch = Invoke-RestMethod -Method Get -Uri "http://localhost:8080/provenance/search?trace_id=trace-abc"

$incidentEvents | Select-Object event_type, metadata
$alertEvents | Select-Object event_type, metadata
$traceRawMessages | Select-Object raw_message_id, payload_format, connector_profile
$provenanceSearch.counts
```

Register a SmartSentinel-like executor bridge and report a dry-run command result. These endpoints do not execute recovery actions inside AionCore and do not call Docker, systemctl, kubectl, or host commands.

```powershell
$policy = Invoke-RestMethod `
  -Method Put `
  -Uri "http://localhost:8080/policies" `
  -ContentType "application/json" `
  -Body (@(
    @{
      target_entity_id = $service.id
      command_type = "sentinel:RunDiagnostic"
      requires_approval = $false
      auto_execute_allowed = $false
      metadata = @{ source = "readme-smartsentinel-bridge" }
    }
  ) | ConvertTo-Json -Depth 8)

$command = Invoke-RestMethod `
  -Method Post `
  -Uri "http://localhost:8080/commands" `
  -ContentType "application/json" `
  -Body (@{
    target_entity_id = $service.id
    command_type = "sentinel:RunDiagnostic"
    payload = @{
      diagnostic = "service-health-summary"
      dry_run = $true
    }
    requested_by = "operator"
    reason = "Inspect SmartSentinel-mapped service state"
  } | ConvertTo-Json -Depth 8)

$sentinelExecutor = Invoke-RestMethod `
  -Method Post `
  -Uri "http://localhost:8080/integrations/smartsentinel/executors/register" `
  -ContentType "application/json" `
  -Body (@{
    agent_key = "sentinel-fog-01"
    display_name = "SmartSentinel fog-01 bridge"
    capabilities = @("sentinel:RunDiagnostic", "sentinel:RestartService", "sentinel:NotifyOperator")
    scopes = @(
      @{ target_entity_id = $service.id }
      @{ entity_type = "sentinel:Service" }
      @{ relationship_type = "sentinel:runs" }
    )
    metadata = @{
      node_id = "fog-01"
      bridge_mode = "report-only"
    }
  } | ConvertTo-Json -Depth 10)

$sentinelCommands = Invoke-RestMethod `
  -Method Get `
  -Uri "http://localhost:8080/integrations/smartsentinel/executors/$($sentinelExecutor.executor.id)/commands"

$claimed = Invoke-RestMethod `
  -Method Post `
  -Uri "http://localhost:8080/integrations/smartsentinel/executors/$($sentinelExecutor.executor.id)/commands/$($command.id)/claim" `
  -ContentType "application/json" `
  -Body (@{
    lease_duration_seconds = 60
    max_retries = 1
    metadata = @{ source = "readme-smoke" }
  } | ConvertTo-Json -Depth 8)

$reported = Invoke-RestMethod `
  -Method Post `
  -Uri "http://localhost:8080/integrations/smartsentinel/executors/$($sentinelExecutor.executor.id)/commands/$($command.id)/report" `
  -ContentType "application/json" `
  -Body (@{
    action_type = "sentinel:RunDiagnostic"
    status = "executed"
    verified = $true
    result_payload = @{
      dry_run = $true
      service_state = "healthy"
      note = "External executor reported result only"
    }
    evidence_refs = @("ev-log-1")
    incident_id = "inc-001"
    alert_id = "alert-001"
    workflow_id = "wf-remediate"
    run_id = "run-42"
    trace_id = "trace-abc"
    correlation_id = "corr-123"
    message = "SmartSentinel bridge reported diagnostic result"
    metadata = @{ operator = "readme" }
  } | ConvertTo-Json -Depth 10)

$reported.command.status
$reported.action_result.metadata
Invoke-RestMethod -Method Get -Uri "http://localhost:8080/events?event_type=aion:SmartSentinelCommandReported"
Invoke-RestMethod -Method Get -Uri "http://localhost:8080/provenance/search?incident_id=inc-001"
```

If the command policy requires approval, approve the command before the bridge claim:

```powershell
Invoke-RestMethod -Method Post -Uri "http://localhost:8080/commands/$($command.id)/approve"
```

TTN v3 uplink JSON can be tested locally through connector-aware HTTP ingestion without a live TTN broker. Create existing AionCore entities, a TTN connector, and an explicit device mapping:

```powershell
$ttnProducer = Invoke-RestMethod `
  -Method Post `
  -Uri "http://localhost:8080/entities" `
  -ContentType "application/json" `
  -Body (@{
    entity_key = "ttn-soil-node-01"
    entity_type = "aion:Sensor"
    jsonld = @{
      "@context" = @{
        aion = "https://w3id.org/aion/"
      }
      "@id" = "urn:aion:device:ttn-soil-node-01"
      "@type" = "aion:Sensor"
    }
  } | ConvertTo-Json -Depth 8)

$ttnFeature = Invoke-RestMethod `
  -Method Post `
  -Uri "http://localhost:8080/entities" `
  -ContentType "application/json" `
  -Body (@{
    entity_key = "plot-01"
    entity_type = "aion:Plot"
    jsonld = @{
      "@context" = @{
        aion = "https://w3id.org/aion/"
      }
      "@id" = "urn:aion:plot:01"
      "@type" = "aion:Plot"
    }
  } | ConvertTo-Json -Depth 8)

$ttnConnector = Invoke-RestMethod `
  -Method Post `
  -Uri "http://localhost:8080/ingestion/connectors" `
  -ContentType "application/json" `
  -Body (@{
    connector_key = "ttn-http-demo"
    connector_type = "mqtt"
    connector_profile = "ttn-v3"
    enabled = $true
    broker_url = "mqtt://eu1.cloud.thethings.network:1883"
    topic_filter = "v3/demo-application/devices/+/up"
    payload_format = "ttn-uplink-json"
    content_type = "application/json"
    metadata = @{
      unit_mapping = @{
        temperature = "Cel"
        soil_moisture = "%"
      }
    }
  } | ConvertTo-Json -Depth 10)

$ttnFallbackMapping = Invoke-RestMethod `
  -Method Post `
  -Uri "http://localhost:8080/ingestion/connectors/$($ttnConnector.id)/ttn-device-mappings" `
  -ContentType "application/json" `
  -Body (@{
    ttn_device_id = "soil-node-01"
    producer_entity_id = $ttnProducer.id
    feature_of_interest_id = $ttnFeature.id
    metadata = @{
      source = "fallback-local-demo"
    }
  } | ConvertTo-Json -Depth 8)

$ttnMapping = Invoke-RestMethod `
  -Method Post `
  -Uri "http://localhost:8080/ingestion/connectors/$($ttnConnector.id)/ttn-device-mappings" `
  -ContentType "application/json" `
  -Body (@{
    ttn_application_id = "farm-app"
    ttn_device_id = "soil-node-01"
    producer_entity_id = $ttnProducer.id
    feature_of_interest_id = $ttnFeature.id
    metadata = @{
      source = "local-demo"
    }
  } | ConvertTo-Json -Depth 8)
```

The fallback mapping applies to the connector/device when no application-specific mapping exists. The `farm-app` mapping above can coexist with the fallback and is preferred for uplinks whose TTN `application_id` is `farm-app`.

Ingest a sample TTN uplink without passing `producer_entity_id`; AionCore resolves it through the application-specific mapping:

```powershell
$ttnIngest = Invoke-RestMethod `
  -Method Post `
  -Uri "http://localhost:8080/ingestion/connectors/$($ttnConnector.id)/ingest" `
  -ContentType "application/json" `
  -Body (@{
    payload = @{
      end_device_ids = @{
        device_id = "soil-node-01"
        application_ids = @{
          application_id = "farm-app"
        }
      }
      received_at = "2026-04-29T12:00:00Z"
      uplink_message = @{
        received_at = "2026-04-29T12:01:02Z"
        f_port = 1
        f_cnt = 42
        frm_payload = "AQID"
        decoded_payload = @{
          temperature = 21.5
          soil_moisture = 44
          state = "ok"
          battery_low = $false
        }
      }
    }
  } | ConvertTo-Json -Depth 12)

$observations = Invoke-RestMethod `
  -Method Get `
  -Uri "http://localhost:8080/observations?raw_message_id=$($ttnIngest.raw_message_id)"

$raw = Invoke-RestMethod `
  -Method Get `
  -Uri "http://localhost:8080/raw-messages/$($ttnIngest.raw_message_id)"

$events = Invoke-RestMethod `
  -Method Get `
  -Uri "http://localhost:8080/events?raw_message_id=$($ttnIngest.raw_message_id)"

$observations | Select-Object observed_property,unit,metadata
$raw.connector_profile
$events.metadata
```

Validate the connector without contacting TTN:

```powershell
$validation = Invoke-RestMethod `
  -Method Get `
  -Uri "http://localhost:8080/ingestion/connectors/$($ttnConnector.id)/validate"

$validation.valid
$validation.readiness
$validation.issues
$validation.warnings
$validation.mapping_count
$validation.enabled_mapping_count
$validation.secret_configured
$validation.secret_type
$validation.operator_hints
```

Validation readiness is configuration-oriented:

- `ready`: no blocking issues, connector enabled, and at least one enabled TTN device mapping exists.
- `degraded`: no blocking issues, but the connector is disabled or has warnings such as no mappings or missing public-broker credentials.
- `invalid`: deterministic configuration checks found blocking issues such as missing broker URL, missing/implausible topic filter, wrong connector type, or unsupported payload format.

Missing mappings and likely missing public TTN broker authentication are warnings:

```powershell
$ttnNoMappings = Invoke-RestMethod `
  -Method Post `
  -Uri "http://localhost:8080/ingestion/connectors" `
  -ContentType "application/json" `
  -Body (@{
    connector_key = "ttn-no-mappings"
    connector_type = "mqtt"
    connector_profile = "ttn-v3"
    enabled = $true
    broker_url = "mqtt://eu1.cloud.thethings.network:1883"
    topic_filter = "v3/demo-application/devices/+/up"
    payload_format = "ttn-uplink-json"
  } | ConvertTo-Json -Depth 8)

Invoke-RestMethod `
  -Method Get `
  -Uri "http://localhost:8080/ingestion/connectors/$($ttnNoMappings.id)/validate"
```

Create a connector secret for TTN MQTT basic auth, attach it to the TTN connector, and validate again:

```powershell
$ttnSecret = Invoke-RestMethod `
  -Method Post `
  -Uri "http://localhost:8080/secrets/connectors" `
  -ContentType "application/json" `
  -Body (@{
    secret_key = "ttn-demo-mqtt-auth"
    secret_type = "mqtt_basic_auth"
    username = "demo-application@tenant"
    secret_value = "replace-with-ttn-api-key-or-password"
    metadata = @{
      purpose = "ttn-mqtt-auth"
    }
  } | ConvertTo-Json -Depth 8)

$ttnSecret.secret_value

$ttnConnector = Invoke-RestMethod `
  -Method Patch `
  -Uri "http://localhost:8080/ingestion/connectors/$($ttnConnector.id)" `
  -ContentType "application/json" `
  -Body (@{
    secret_ref_id = $ttnSecret.id
  } | ConvertTo-Json -Depth 8)

$credentialValidation = Invoke-RestMethod `
  -Method Get `
  -Uri "http://localhost:8080/ingestion/connectors/$($ttnConnector.id)/validate"

$credentialValidation.secret_configured
$credentialValidation.secret_type
$credentialValidation.operator_hints
$credentialValidation | ConvertTo-Json -Depth 8
```

`$ttnSecret.secret_value` is empty because secret values are write-only in API responses. Validation reports whether a secret is configured and its non-secret type, but never returns the stored password/token.

Preview the future live-validation checklist without connecting to TTN:

```powershell
$livePlan = Invoke-RestMethod `
  -Method Get `
  -Uri "http://localhost:8080/ingestion/connectors/$($ttnConnector.id)/ttn-live-readiness-plan"

$livePlan.dry_run
$livePlan.safe_to_connect
$livePlan.can_attempt_live_validation
$livePlan.blockers
$livePlan.required_operator_steps
$livePlan.checks | Select-Object check_key,status,reason,future_live_check
```

The dry-run plan always includes `no_network_call_performed`. Missing `broker_url`, `topic_filter`, `payload_format = "ttn-uplink-json"`, `secret_ref_id`, a compatible `mqtt_basic_auth` connector secret, or an enabled TTN device mapping appear as blockers before AionCore would allow future live validation.

Run the same preflight endpoint in dry-run-only mode. This uses the live-validation response shape but still does not open a network connection:

```powershell
$preflightDryRun = Invoke-RestMethod `
  -Method Post `
  -Uri "http://localhost:8080/ingestion/connectors/$($ttnConnector.id)/ttn-live-validate" `
  -ContentType "application/json" `
  -Body (@{
    dry_run_only = $true
    timeout_seconds = 5
  } | ConvertTo-Json -Depth 8)

$preflightDryRun.result
$preflightDryRun.attempted_live_connection
$preflightDryRun.dry_run_plan_summary
$preflightDryRun.secret_exposed
```

Optional live TTN MQTT preflight, only when you intentionally want AionCore to connect to the configured broker and subscribe to the uplink topic:

```powershell
$livePreflight = Invoke-RestMethod `
  -Method Post `
  -Uri "http://localhost:8080/ingestion/connectors/$($ttnConnector.id)/ttn-live-validate" `
  -ContentType "application/json" `
  -Body (@{
    timeout_seconds = 5
    expect_message = $false
    client_id_suffix = "manual-preflight"
  } | ConvertTo-Json -Depth 8)

$livePreflight.result
$livePreflight.connected
$livePreflight.subscribed
$livePreflight.message_received
$livePreflight.errors
```

When `expect_message = $false`, success means the MQTT connection and subscription completed. When `expect_message = $true`, success also requires at least one matching message before the timeout. Preflight messages are not ingested, raw payloads are not returned, and secret values remain write-only.

Update and delete mappings explicitly:

```powershell
$updatedMapping = Invoke-RestMethod `
  -Method Patch `
  -Uri "http://localhost:8080/ingestion/connectors/$($ttnConnector.id)/ttn-device-mappings/$($ttnMapping.id)" `
  -ContentType "application/json" `
  -Body (@{
    enabled = $true
    metadata = @{
      source = "updated-local-demo"
    }
  } | ConvertTo-Json -Depth 8)

Invoke-RestMethod `
  -Method Delete `
  -Uri "http://localhost:8080/ingestion/connectors/$($ttnConnector.id)/ttn-device-mappings/$($ttnMapping.id)"
```

Duplicate enabled mappings for the same connector, device, and application are rejected with a conflict. Duplicate enabled fallback mappings for the same connector/device are also rejected:

```powershell
Invoke-RestMethod `
  -Method Post `
  -Uri "http://localhost:8080/ingestion/connectors/$($ttnConnector.id)/ttn-device-mappings" `
  -ContentType "application/json" `
  -Body (@{
    ttn_application_id = "farm-app"
    ttn_device_id = "soil-node-01"
    producer_entity_id = $ttnProducer.id
    feature_of_interest_id = $ttnFeature.id
  } | ConvertTo-Json -Depth 8)
```

If no enabled mapping matches and the request does not provide a producer entity, AionCore preserves the raw message, marks it failed, emits `aion:TtnDeviceMappingMissing`, and creates no observations. Failure event metadata includes `ttn_device_id`, `ttn_application_id`, `connector_id`, and `mapping_resolution_error`. Successful `aion:PayloadIngested` metadata includes `ttn_mapping_id` and `mapping_resolution`, with values `exact_application_match` or `fallback_device_match`.

Plan ingestion workers without starting them:

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
- TTN v3 connectors with another payload format are marked invalid with a validation issue
- missing `broker_url` or `topic_filter` makes MQTT specs `invalid`
- future connector types are `unsupported`

`GET /ingestion/workers/plan` is read-only. It does not connect to brokers or start dynamic workers.

The connector registry is available in memory and through the PostgreSQL backend. Existing env-var MQTT config remains the default runtime MQTT connector behavior. Dynamic MQTT workers per connector are opt-in. TTN uplink decoding and explicit device mappings are local and sample-payload testable; live TTN broker validation, downlinks, and entity auto-provisioning are future work.

Dynamic connector workers are disabled unless explicitly enabled:

```powershell
$env:AIONCORE_CONNECTOR_WORKERS_ENABLED = "false"
cargo run -p aion-api
```

Enable connector-based MQTT workers only when you want AionCore to start one worker for each valid enabled `generic-aion-mqtt` or `generic-mqtt` connector:

```powershell
$env:AIONCORE_CONNECTOR_WORKERS_ENABLED = "true"
cargo run -p aion-api
```

If both `AIONCORE_MQTT_ENABLED=true` and `AIONCORE_CONNECTOR_WORKERS_ENABLED=true` are set, the env-var MQTT worker and connector-based MQTT workers may run at the same time.

Connector MQTT workers can use local connector secret references for broker username/password authentication. Secret values are accepted on write, stored outside `IngestionConnector`, and never returned by the API:

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

$brokerSecret.secret_value
```

The last expression returns nothing because secret values are not included in responses. Create a connector that references the secret by ID:

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

$genericMqttConnector.secret_ref_id
$genericMqttConnector.secret_value
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

This is a local-development credential foundation only. It does not implement encryption, KMS, Vault, TLS/mTLS, per-device MQTT authorization, or AionCore user/device authentication.

Inspect intended and runtime connector workers:

```powershell
Invoke-RestMethod -Method Get -Uri "http://localhost:8080/ingestion/workers/plan"
Invoke-RestMethod -Method Get -Uri "http://localhost:8080/ingestion/workers/status"
Invoke-RestMethod -Method Get -Uri "http://localhost:8080/ready"
```

Runtime reconciliation starts, stops, or restarts connector workers when connectors are created, enabled, or disabled. Manual reconciliation is also available:

```powershell
$reconcile = Invoke-RestMethod `
  -Method Post `
  -Uri "http://localhost:8080/ingestion/workers/reconcile"

$reconcile.actions
```

Startup reconciliation loads persisted enabled connectors. Runtime reconciliation handles connector lifecycle changes after startup. Connector configuration can be updated with `PATCH /ingestion/connectors/{connector_id}`; immutable identity fields such as `id`, `tenant_id`, `connector_key`, `connector_type`, and `connector_profile` are not accepted by the update body.

Update a dynamic MQTT connector and inspect the runtime restart/status fields:

```powershell
$updated = Invoke-RestMethod `
  -Method Patch `
  -Uri "http://localhost:8080/ingestion/connectors/$($genericMqttConnector.id)" `
  -ContentType "application/json" `
  -Body (@{
    topic_filter = "farm/+/telemetry"
    broker_url = "mqtt://127.0.0.1:1883"
    payload_format = "senml-json"
  } | ConvertTo-Json -Depth 8)

$status = Invoke-RestMethod `
  -Method Get `
  -Uri "http://localhost:8080/ingestion/workers/status"

$status.workers | Select-Object connector_key,status,restart_count,reconnect_attempts,last_disconnect_at,last_reconnect_at,next_reconnect_at,last_error
```

If a manual refresh is needed, call reconciliation explicitly:

```powershell
Invoke-RestMethod `
  -Method Post `
  -Uri "http://localhost:8080/ingestion/workers/reconcile"
```

`/ingestion/workers/status` reports `planned`, `starting`, `running`, `reconnecting`, `stopped`, `degraded`, `skipped`, `invalid`, `error`, or `unsupported` per connector. Status entries include `started_at`, `stopped_at`, `restart_count`, `reconnect_attempts`, `last_disconnect_at`, `last_reconnect_at`, `next_reconnect_at`, `last_error`, and `last_reconciled_at`. Connector MQTT workers retry broker disconnects and event-loop failures with bounded exponential backoff from 1 second up to 60 seconds. `/ready` includes `connector_workers.total`, `running`, `stopped`, `degraded`, `skipped`, `invalid`, and `errors`; connector worker issues do not replace the existing storage/env-var MQTT readiness rules.

Example test publish for a connector using the generic AionCore MQTT topic convention:

```powershell
mosquitto_pub.exe `
  -h 127.0.0.1 `
  -p 1883 `
  -t "aioncore/11111111-1111-1111-1111-111111111111/22222222-2222-2222-2222-222222222222/data" `
  -m '{ "e": [ { "n": "soil_moisture", "u": "%", "v": 18.5 } ] }' `
  -V mqttv5
```

TTN v3 connectors with `payload_format = "ttn-uplink-json"` can be registered, planned, decoded from sample uplink JSON, and resolved through explicit TTN device mappings. Live TTN account validation, downlinks, and entity auto-provisioning are not implemented yet.

With PostgreSQL selected as the storage backend, connector records are stored durably. Example connector payloads:

```json
{
  "connector_key": "farm-mqtt",
  "connector_type": "mqtt",
  "connector_profile": "generic-mqtt",
  "enabled": false,
  "broker_url": "mqtt://127.0.0.1:1883",
  "client_id": "aioncore-farm-mqtt",
  "topic_filter": "farm/+/telemetry",
  "payload_format": "canonical-json"
}
```

```json
{
  "connector_key": "ttn-demo",
  "connector_type": "mqtt",
  "connector_profile": "ttn-v3",
  "enabled": false,
  "broker_url": "mqtt://eu1.cloud.thethings.network:1883",
  "topic_filter": "v3/demo-application/devices/+/up",
  "payload_format": "ttn-uplink-json"
}
```

Runtime validation scripts:

```powershell
.\scripts\validate-memory-runtime.ps1
.\scripts\validate-memory-runtime.ps1 -BaseUrl "http://127.0.0.1:8081"
```

```powershell
$env:AIONCORE_DATABASE_URL = "postgres://user:password@localhost:5432/aioncore"
.\scripts\validate-postgres-runtime.ps1
```

The scripts are PowerShell-first and work with Windows PowerShell and PowerShell 7.

## Optional PostgreSQL Adapter Tests

Set `AIONCORE_TEST_DATABASE_URL` to a PostgreSQL database that has the required extensions available, then run the storage crate tests that target the adapter:

```powershell
$env:AIONCORE_TEST_DATABASE_URL = "postgres://user:password@localhost:5432/aioncore"
cargo test -p aion-storage postgres_
```

If the environment variable is unset, the PostgreSQL adapter tests skip cleanly and the normal in-memory test suite still passes.

Connector persistence is covered by the PostgreSQL parity suite:

```powershell
$env:AIONCORE_TEST_DATABASE_URL = "postgres://user:password@localhost:5432/aioncore"
cargo test -p aion-storage postgres_parity_ingestion_connectors
```

If a postgres runtime URL is available, you can also validate the API startup path:

```powershell
$env:AIONCORE_STORAGE_BACKEND = "postgres"
$env:AIONCORE_DATABASE_URL = $env:AIONCORE_TEST_DATABASE_URL
cargo run -p aion-api
```

Troubleshooting:

- If startup exits with `AIONCORE_DATABASE_URL is required when AIONCORE_STORAGE_BACKEND=postgres`, set the database URL before starting the API.
- If `/ready` returns not ready in postgres mode, check database connectivity and confirm the migrations can run against the target database.
- If an unknown backend value is set, the API fails fast instead of silently changing modes.

For convenience, the startup wrappers are:

```powershell
.\scripts\start-memory-runtime.ps1
.\scripts\start-postgres-runtime.ps1 -DatabaseUrl "postgres://user:password@localhost:5432/aioncore"
```

Telemetry parity tests cover raw message filtering, canonical observation storage, and event storage/query behavior:

```powershell
$env:AIONCORE_TEST_DATABASE_URL = "postgres://user:password@localhost:5432/aioncore"
cargo test -p aion-storage postgres_parity_raw_messages
cargo test -p aion-storage postgres_parity_observations
cargo test -p aion-storage postgres_parity_events
```

Lifecycle parity tests cover commands, actions, action results, leases, and rules:

```powershell
$env:AIONCORE_TEST_DATABASE_URL = "postgres://user:password@localhost:5432/aioncore"
cargo test -p aion-storage postgres_parity_commands_actions_and_results
cargo test -p aion-storage postgres_parity_command_leases
cargo test -p aion-storage postgres_parity_rules
```

Start the API:

```text
cargo run -p aion-api
```

The service listens on:

```text
http://localhost:8080
```

Check health:

```text
Invoke-RestMethod -Method Get -Uri "http://localhost:8080/health"
```

Create an entity with the envelope format:

```powershell
$entity = @{
  entity_key = "sensor-01"
  entity_type = "aion:Sensor"
  jsonld = @{
    "@context" = @{
      aion = "https://aioncore.org/ns#"
    }
    "@id" = "urn:aion:sensor:sensor-01"
    "@type" = "aion:Sensor"
    name = "Sensor 01"
  }
} | ConvertTo-Json -Depth 10

Invoke-RestMethod `
  -Method Post `
  -Uri "http://localhost:8080/entities" `
  -ContentType "application/json" `
  -Body $entity
```

Create an entity with native JSON-LD:

```powershell
$jsonldEntity = @{
  "@context" = @{
    aion = "https://aioncore.org/ns#"
  }
  "@id" = "urn:aion:sensor:sensor-ld-01"
  "@type" = "aion:Sensor"
  "aion:entityKey" = "sensor-ld-01"
  name = "Sensor LD 01"
} | ConvertTo-Json -Depth 10

Invoke-RestMethod `
  -Method Post `
  -Uri "http://localhost:8080/entities" `
  -ContentType "application/ld+json" `
  -Body $jsonldEntity
```

For native JSON-LD, AionCore uses `entity_key` first, then `aion:entityKey`, then derives a key from `@id`. Numeric suffixes are combined with the preceding semantic segment, so these IDs derive distinct keys:

```powershell
$zone = @{
  "@context" = @{
    aion = "https://aioncore.org/ns#"
  }
  "@id" = "urn:aion:farm:01:zone:01"
  "@type" = "aion:IrrigationZone"
  name = "Zone 01"
} | ConvertTo-Json -Depth 10

$sensor = @{
  "@context" = @{
    aion = "https://aioncore.org/ns#"
  }
  "@id" = "urn:aion:farm:01:soil-sensor:01"
  "@type" = "aion:SoilSensor"
  name = "Soil Sensor 01"
} | ConvertTo-Json -Depth 10

Invoke-RestMethod `
  -Method Post `
  -Uri "http://localhost:8080/entities" `
  -ContentType "application/ld+json" `
  -Body $zone

Invoke-RestMethod `
  -Method Post `
  -Uri "http://localhost:8080/entities" `
  -ContentType "application/ld+json" `
  -Body $sensor
```

Create a relationship after creating two entities:

```powershell
$relationship = @{
  source_entity_id = "<sensor-id>"
  relationship_type = "aion:locatedIn"
  target_entity_id = "<room-id>"
  jsonld = @{
    "@type" = "aion:Relationship"
  }
} | ConvertTo-Json -Depth 10

Invoke-RestMethod `
  -Method Post `
  -Uri "http://localhost:8080/relationships" `
  -ContentType "application/json" `
  -Body $relationship
```

Create an observation:

```powershell
$observation = @{
  producer_entity_id = "<sensor-id>"
  feature_of_interest_id = "<room-id>"
  observed_property = "temperature"
  value = @{
    type = "number"
    value = 21.4
  }
  unit = "Cel"
  observed_at = "2026-04-27T13:00:00Z"
  received_at = "2026-04-27T13:00:01Z"
  protocol = "http"
  payload_format = "json_mapping"
  quality = @{}
  metadata = @{}
} | ConvertTo-Json -Depth 10

Invoke-RestMethod `
  -Method Post `
  -Uri "http://localhost:8080/observations" `
  -ContentType "application/json" `
  -Body $observation
```

Query entity context:

```text
Invoke-RestMethod -Method Get -Uri "http://localhost:8080/entities/<entity-id>/context"
```

Attach an UltraLight payload profile to a sensor:

```powershell
$profile = @{
  payload_format = "ultralight"
  protocol = "http"
  content_type = "text/plain"
  attribute_mapping = @{
    m = @{
      observed_property = "aion:SoilMoisture"
      unit = "%"
    }
    t = @{
      observed_property = "aion:SoilTemperature"
      unit = "Cel"
    }
  }
  metadata = @{
    profile_version = 1
  }
} | ConvertTo-Json -Depth 10

Invoke-RestMethod `
  -Method Put `
  -Uri "http://localhost:8080/entities/<sensor-id>/payload-profile" `
  -ContentType "application/json" `
  -Body $profile
```

Retrieve a sensor payload profile:

```text
Invoke-RestMethod -Method Get -Uri "http://localhost:8080/entities/<sensor-id>/payload-profile"
```

Ingest UltraLight telemetry using the stored payload profile:

```powershell
$ingest = @{
  producer_entity_id = "<sensor-id>"
  feature_of_interest_id = "<plot-id>"
  payload_format = "ultralight"
  protocol = "http"
  content_type = "text/plain"
  observed_at = "2026-04-27T13:00:00Z"
  payload = "m|18.5|t|24.1"
} | ConvertTo-Json -Depth 10

Invoke-RestMethod `
  -Method Post `
  -Uri "http://localhost:8080/ingest/http" `
  -ContentType "application/json" `
  -Body $ingest
```

Ingest SenML telemetry and keep the raw message ID for inspection:

```powershell
$sensor = Invoke-RestMethod `
  -Method Post `
  -Uri "http://localhost:8080/entities" `
  -ContentType "application/json" `
  -Body (@{
    entity_key = "soil-sensor-01"
    entity_type = "aion:Sensor"
    jsonld = @{
      "@context" = @{ aion = "https://aioncore.org/ns#" }
      "@id" = "urn:aion:farm:01:soil-sensor:01"
      "@type" = "aion:Sensor"
      name = "Soil Sensor 01"
    }
  } | ConvertTo-Json -Depth 10)

$plot = Invoke-RestMethod `
  -Method Post `
  -Uri "http://localhost:8080/entities" `
  -ContentType "application/json" `
  -Body (@{
    entity_key = "plot-01"
    entity_type = "aion:Plot"
    jsonld = @{
      "@context" = @{ aion = "https://aioncore.org/ns#" }
      "@id" = "urn:aion:farm:01:plot:01"
      "@type" = "aion:Plot"
      name = "Plot 01"
    }
  } | ConvertTo-Json -Depth 10)

$senmlIngest = Invoke-RestMethod `
  -Method Post `
  -Uri "http://localhost:8080/ingest/http" `
  -ContentType "application/json" `
  -Body (@{
    producer_entity_id = $sensor.id
    feature_of_interest_id = $plot.id
    payload_format = "senml-json"
    protocol = "http"
    content_type = "application/senml+json"
    payload = @(
      @{
        bn = "urn:aion:farm:01:soil-sensor:01:"
        bt = 1777294800
        n = "soil_moisture"
        u = "%"
        v = 18.5
      },
      @{
        n = "soil_temperature"
        u = "Cel"
        v = 24.1
      }
    )
  } | ConvertTo-Json -Depth 10)

$rawMessageId = $senmlIngest.raw_message_id
```

Inspect a raw message after ingestion:

```powershell
Invoke-RestMethod -Method Get -Uri "http://localhost:8080/raw-messages/$rawMessageId"
```

Query raw messages:

```powershell
Invoke-RestMethod -Method Get -Uri "http://localhost:8080/raw-messages?producer_entity_id=$($sensor.id)"
Invoke-RestMethod -Method Get -Uri "http://localhost:8080/raw-messages?feature_of_interest_id=$($plot.id)"
Invoke-RestMethod -Method Get -Uri "http://localhost:8080/raw-messages?payload_format=senml-json"
```

Query observations generated from a raw message:

```powershell
Invoke-RestMethod -Method Get -Uri "http://localhost:8080/observations?raw_message_id=$rawMessageId"
```

Query observations:

```text
Invoke-RestMethod -Method Get -Uri "http://localhost:8080/observations?feature_of_interest_id=<room-id>"
```

Create a smart-building command, action, and action result:

```powershell
$tank = Invoke-RestMethod `
  -Method Post `
  -Uri "http://localhost:8080/entities" `
  -ContentType "application/json" `
  -Body (@{
    entity_key = "water-tank-01"
    entity_type = "aion:WaterTank"
    jsonld = @{
      "@context" = @{ aion = "https://aioncore.org/ns#" }
      "@id" = "urn:aion:building:water-tank:01"
      "@type" = "aion:WaterTank"
      name = "Water Tank 01"
    }
  } | ConvertTo-Json -Depth 10)

$pump = Invoke-RestMethod `
  -Method Post `
  -Uri "http://localhost:8080/entities" `
  -ContentType "application/json" `
  -Body (@{
    entity_key = "pump-01"
    entity_type = "aion:Pump"
    jsonld = @{
      "@context" = @{ aion = "https://aioncore.org/ns#" }
      "@id" = "urn:aion:building:pump:01"
      "@type" = "aion:Pump"
      name = "Pump 01"
      serves = $tank.id
    }
  } | ConvertTo-Json -Depth 10)

$capabilities = @(
  @{
    capability_name = "StartPump"
    command_type = "StartPump"
    protocol = "http"
    metadata = @{ description = "Start the pump motor" }
  },
  @{
    capability_name = "StopPump"
    command_type = "StopPump"
    protocol = "http"
    metadata = @{ description = "Stop the pump motor" }
  }
) | ConvertTo-Json -Depth 10

Invoke-RestMethod `
  -Method Put `
  -Uri "http://localhost:8080/entities/$($pump.id)/capabilities" `
  -ContentType "application/json" `
  -Body $capabilities

Invoke-RestMethod -Method Get -Uri "http://localhost:8080/entities/$($pump.id)/capabilities"

$command = Invoke-RestMethod `
  -Method Post `
  -Uri "http://localhost:8080/commands" `
  -ContentType "application/json" `
  -Body (@{
    target_entity_id = $pump.id
    command_type = "StartPump"
    payload = @{ target_state = "running" }
    requested_by = "operator@example.com"
    reason = "Water tank level is below minimum"
  } | ConvertTo-Json -Depth 10)

Invoke-RestMethod -Method Get -Uri "http://localhost:8080/commands?target_entity_id=$($pump.id)&status=pending"

$action = Invoke-RestMethod `
  -Method Post `
  -Uri "http://localhost:8080/actions" `
  -ContentType "application/json" `
  -Body (@{
    command_id = $command.id
    executor_entity_id = $pump.id
    action_type = "StartPump"
    status = "started"
    started_at = "2026-04-27T13:00:00Z"
    metadata = @{ external_correlation_id = "building-sim-001" }
  } | ConvertTo-Json -Depth 10)

$result = Invoke-RestMethod `
  -Method Post `
  -Uri "http://localhost:8080/action-results" `
  -ContentType "application/json" `
  -Body (@{
    command_id = $command.id
    action_id = $action.id
    status = "succeeded"
    verified = $true
    result_payload = @{ pump_state = "running" }
    observed_at = "2026-04-27T13:00:05Z"
    metadata = @{ verification_source = "simulated_executor" }
  } | ConvertTo-Json -Depth 10)

Invoke-RestMethod -Method Get -Uri "http://localhost:8080/action-results?command_id=$($command.id)"
```

Create an approval-gated StartPump command:

```powershell
$pump = Invoke-RestMethod `
  -Method Post `
  -Uri "http://localhost:8080/entities" `
  -ContentType "application/json" `
  -Body (@{
    entity_key = "approval-pump-01"
    entity_type = "aion:Pump"
    jsonld = @{
      "@context" = @{ aion = "https://aioncore.org/ns#" }
      "@id" = "urn:aion:building:approval-pump:01"
      "@type" = "aion:Pump"
      name = "Approval Pump 01"
    }
  } | ConvertTo-Json -Depth 10)

$capabilities = @(
  @{
    capability_name = "StartPump"
    command_type = "StartPump"
    protocol = "http"
    metadata = @{ description = "Start the pump motor" }
  }
) | ConvertTo-Json -Depth 10

Invoke-RestMethod `
  -Method Put `
  -Uri "http://localhost:8080/entities/$($pump.id)/capabilities" `
  -ContentType "application/json" `
  -Body $capabilities

$policies = @(
  @{
    target_entity_id = $pump.id
    command_type = "StartPump"
    requires_approval = $true
    auto_execute_allowed = $false
    metadata = @{ reason = "Physical actuation requires approval" }
  }
) | ConvertTo-Json -Depth 10

Invoke-RestMethod `
  -Method Put `
  -Uri "http://localhost:8080/policies" `
  -ContentType "application/json" `
  -Body $policies

$command = Invoke-RestMethod `
  -Method Post `
  -Uri "http://localhost:8080/commands" `
  -ContentType "application/json" `
  -Body (@{
    target_entity_id = $pump.id
    command_type = "StartPump"
    payload = @{ target_state = "running" }
    requested_by = "operator@example.com"
    reason = "Water tank level is below minimum"
  } | ConvertTo-Json -Depth 10)

try {
  Invoke-RestMethod `
    -Method Post `
    -Uri "http://localhost:8080/commands/$($command.id)/claim" `
    -ContentType "application/json" `
    -Body (@{ claimed_by = "edge-agent-01" } | ConvertTo-Json)
} catch {
  $_.ErrorDetails.Message
}

Invoke-RestMethod -Method Post -Uri "http://localhost:8080/commands/$($command.id)/approve"

$claimed = Invoke-RestMethod `
  -Method Post `
  -Uri "http://localhost:8080/commands/$($command.id)/claim" `
  -ContentType "application/json" `
  -Body (@{ claimed_by = "edge-agent-01" } | ConvertTo-Json)

$action = Invoke-RestMethod `
  -Method Post `
  -Uri "http://localhost:8080/actions" `
  -ContentType "application/json" `
  -Body (@{
    command_id = $claimed.id
    executor_entity_id = $pump.id
    action_type = "StartPump"
    status = "started"
    started_at = "2026-04-27T13:00:00Z"
    metadata = @{ external_correlation_id = "edge-agent-001" }
  } | ConvertTo-Json -Depth 10)

Invoke-RestMethod -Method Post -Uri "http://localhost:8080/commands/$($claimed.id)/mark-executed"

Invoke-RestMethod `
  -Method Post `
  -Uri "http://localhost:8080/action-results" `
  -ContentType "application/json" `
  -Body (@{
    command_id = $claimed.id
    action_id = $action.id
    status = "succeeded"
    verified = $true
    result_payload = @{ pump_state = "running" }
    observed_at = "2026-04-27T13:00:05Z"
    metadata = @{ verification_source = "edge-agent-01" }
  } | ConvertTo-Json -Depth 10)
```

Create and query audit events:

```powershell
$manualEvent = Invoke-RestMethod `
  -Method Post `
  -Uri "http://localhost:8080/events" `
  -ContentType "application/json" `
  -Body (@{
    event_type = "aion:ManualAuditEvent"
    severity = "info"
    target_entity_id = $pump.id
    message = "Manual event for audit testing"
    occurred_at = "2026-04-27T13:01:00Z"
    correlation_id = "manual-audit-001"
    command_id = $claimed.id
    metadata = @{ source = "operator" }
  } | ConvertTo-Json -Depth 10)

Invoke-RestMethod -Method Get -Uri "http://localhost:8080/events/$($manualEvent.id)"
Invoke-RestMethod -Method Get -Uri "http://localhost:8080/events?target_entity_id=$($pump.id)"
Invoke-RestMethod -Method Get -Uri "http://localhost:8080/events?event_type=aion:ManualAuditEvent"
Invoke-RestMethod -Method Get -Uri "http://localhost:8080/events?severity=info"
Invoke-RestMethod -Method Get -Uri "http://localhost:8080/events?correlation_id=manual-audit-001"
```

Query events created by ingestion:

```powershell
$sensor = Invoke-RestMethod `
  -Method Post `
  -Uri "http://localhost:8080/entities" `
  -ContentType "application/json" `
  -Body (@{
    entity_key = "event-soil-sensor-01"
    entity_type = "aion:Sensor"
    jsonld = @{
      "@context" = @{ aion = "https://aioncore.org/ns#" }
      "@id" = "urn:aion:farm:event-soil-sensor:01"
      "@type" = "aion:Sensor"
      name = "Event Soil Sensor 01"
    }
  } | ConvertTo-Json -Depth 10)

$plot = Invoke-RestMethod `
  -Method Post `
  -Uri "http://localhost:8080/entities" `
  -ContentType "application/json" `
  -Body (@{
    entity_key = "event-plot-01"
    entity_type = "aion:Plot"
    jsonld = @{
      "@context" = @{ aion = "https://aioncore.org/ns#" }
      "@id" = "urn:aion:farm:event-plot:01"
      "@type" = "aion:Plot"
      name = "Event Plot 01"
    }
  } | ConvertTo-Json -Depth 10)

$ingest = Invoke-RestMethod `
  -Method Post `
  -Uri "http://localhost:8080/ingest/http" `
  -ContentType "application/json" `
  -Body (@{
    producer_entity_id = $sensor.id
    feature_of_interest_id = $plot.id
    payload_format = "senml-json"
    protocol = "http"
    content_type = "application/senml+json"
    payload = @(
      @{
        bn = "urn:aion:farm:event-soil-sensor:01:"
        bt = 1777294800
        n = "soil_moisture"
        u = "%"
        v = 18.5
      }
    )
  } | ConvertTo-Json -Depth 10)

Invoke-RestMethod -Method Get -Uri "http://localhost:8080/events?raw_message_id=$($ingest.raw_message_id)"
```

Query command lifecycle events:

```powershell
Invoke-RestMethod -Method Get -Uri "http://localhost:8080/events?command_id=$($claimed.id)"
```

Create a local in-memory rule for a smart-building closed-loop scenario:

```powershell
$ruleTank = Invoke-RestMethod `
  -Method Post `
  -Uri "http://localhost:8080/entities" `
  -ContentType "application/json" `
  -Body (@{
    entity_key = "rule-water-tank-01"
    entity_type = "aion:WaterTank"
    jsonld = @{
      "@context" = @{ aion = "https://aioncore.org/ns#" }
      "@id" = "urn:aion:building:rule-water-tank:01"
      "@type" = "aion:WaterTank"
      name = "Rule Water Tank 01"
    }
  } | ConvertTo-Json -Depth 10)

$rulePump = Invoke-RestMethod `
  -Method Post `
  -Uri "http://localhost:8080/entities" `
  -ContentType "application/json" `
  -Body (@{
    entity_key = "rule-pump-01"
    entity_type = "aion:Pump"
    jsonld = @{
      "@context" = @{ aion = "https://aioncore.org/ns#" }
      "@id" = "urn:aion:building:rule-pump:01"
      "@type" = "aion:Pump"
      name = "Rule Pump 01"
    }
  } | ConvertTo-Json -Depth 10)

$rulePumpCapability = @{
  capability_name = "Start pump"
  command_type = "StartPump"
  protocol = "local"
  metadata = @{ safety = "requires_approval" }
}

Invoke-RestMethod `
  -Method Put `
  -Uri "http://localhost:8080/entities/$($rulePump.id)/capabilities" `
  -ContentType "application/json" `
  -Body (ConvertTo-Json -InputObject (, $rulePumpCapability) -Depth 10)

$rulePumpPolicy = @{
  target_entity_id = $rulePump.id
  command_type = "StartPump"
  requires_approval = $true
  auto_execute_allowed = $false
  metadata = @{ reason = "physical actuation requires approval" }
}

Invoke-RestMethod `
  -Method Put `
  -Uri "http://localhost:8080/policies" `
  -ContentType "application/json" `
  -Body (ConvertTo-Json -InputObject (, $rulePumpPolicy) -Depth 10)

$lowWaterRule = Invoke-RestMethod `
  -Method Post `
  -Uri "http://localhost:8080/rules" `
  -ContentType "application/json" `
  -Body (@{
    name = "Start pump when water level is low"
    description = "If WaterTankLevel is below 20, create a StartPump command."
    enabled = $true
    trigger_type = "observation_created"
    target_entity_id = $ruleTank.id
    observed_property = "WaterTankLevel"
    condition = @{
      comparison = "less_than"
      value = 20
    }
    action = @{
      type = "create_command"
      target_entity_id = $rulePump.id
      command_type = "StartPump"
      payload = @{ target_state = "running" }
      requested_by = "aion-rule-engine"
      reason = "Water tank level is below threshold"
    }
    metadata = @{ scenario = "smart_building" }
  } | ConvertTo-Json -Depth 10)

Invoke-RestMethod `
  -Method Post `
  -Uri "http://localhost:8080/observations" `
  -ContentType "application/json" `
  -Body (@{
    producer_entity_id = $ruleTank.id
    feature_of_interest_id = $ruleTank.id
    observed_property = "WaterTankLevel"
    value = @{ type = "number"; value = 12 }
    unit = "%"
    observed_at = "2026-04-28T12:00:00Z"
    received_at = "2026-04-28T12:00:01Z"
    protocol = "http"
    payload_format = "json_mapping"
    quality = @{}
    metadata = @{}
  } | ConvertTo-Json -Depth 10)

$ruleCommands = Invoke-RestMethod `
  -Method Get `
  -Uri "http://localhost:8080/commands?target_entity_id=$($rulePump.id)&status=pending"

$ruleCommand = $ruleCommands[0]
$ruleCommand.approval_status
$ruleCommand.policy_decision

Invoke-RestMethod -Method Post -Uri "http://localhost:8080/commands/$($ruleCommand.id)/approve"

$ruleClaimed = Invoke-RestMethod `
  -Method Post `
  -Uri "http://localhost:8080/commands/$($ruleCommand.id)/claim" `
  -ContentType "application/json" `
  -Body (@{ claimed_by = "edge-agent-01" } | ConvertTo-Json)

$ruleAction = Invoke-RestMethod `
  -Method Post `
  -Uri "http://localhost:8080/actions" `
  -ContentType "application/json" `
  -Body (@{
    command_id = $ruleClaimed.id
    executor_entity_id = $rulePump.id
    action_type = "StartPump"
    status = "started"
    started_at = "2026-04-28T12:01:00Z"
    metadata = @{ source = "manual-local-test" }
  } | ConvertTo-Json -Depth 10)

Invoke-RestMethod -Method Post -Uri "http://localhost:8080/commands/$($ruleClaimed.id)/mark-executed"

Invoke-RestMethod `
  -Method Post `
  -Uri "http://localhost:8080/action-results" `
  -ContentType "application/json" `
  -Body (@{
    command_id = $ruleClaimed.id
    action_id = $ruleAction.id
    status = "succeeded"
    verified = $true
    result_payload = @{ pump_state = "running" }
    observed_at = "2026-04-28T12:01:30Z"
    metadata = @{ verification_source = "manual-local-test" }
  } | ConvertTo-Json -Depth 10)
```

Register an external executor and let it poll, claim, and complete the generated command:

```powershell
Invoke-RestMethod `
  -Method Post `
  -Uri "http://localhost:8080/observations" `
  -ContentType "application/json" `
  -Body (@{
    producer_entity_id = $ruleTank.id
    feature_of_interest_id = $ruleTank.id
    observed_property = "WaterTankLevel"
    value = @{ type = "number"; value = 11 }
    unit = "%"
    observed_at = "2026-04-28T12:05:00Z"
    received_at = "2026-04-28T12:05:01Z"
    protocol = "http"
    payload_format = "json_mapping"
    quality = @{}
    metadata = @{}
  } | ConvertTo-Json -Depth 10)

$executorRuleCommands = Invoke-RestMethod `
  -Method Get `
  -Uri "http://localhost:8080/commands?target_entity_id=$($rulePump.id)&status=pending"

$executorRuleCommand = $executorRuleCommands[0]

$executor = Invoke-RestMethod `
  -Method Post `
  -Uri "http://localhost:8080/executors" `
  -ContentType "application/json" `
  -Body (@{
    agent_key = "edge-agent-01"
    agent_type = "edge"
    display_name = "Edge Agent 01"
    status = "online"
    metadata = @{ site = "building-01" }
  } | ConvertTo-Json -Depth 10)

$executorCapability = @{
  command_type = "StartPump"
  protocol = "local"
  metadata = @{ source = "manual-test" }
}

Invoke-RestMethod `
  -Method Put `
  -Uri "http://localhost:8080/executors/$($executor.id)/capabilities" `
  -ContentType "application/json" `
  -Body (ConvertTo-Json -InputObject (, $executorCapability) -Depth 10)

$executorScope = @{
  target_entity_id = $rulePump.id
  metadata = @{ scope = "pump-only" }
}

Invoke-RestMethod `
  -Method Put `
  -Uri "http://localhost:8080/executors/$($executor.id)/scopes" `
  -ContentType "application/json" `
  -Body (ConvertTo-Json -InputObject (, $executorScope) -Depth 10)

try {
  Invoke-RestMethod -Method Post -Uri "http://localhost:8080/executors/$($executor.id)/commands/$($executorRuleCommand.id)/claim"
} catch {
  $_.ErrorDetails.Message
}

Invoke-RestMethod -Method Post -Uri "http://localhost:8080/commands/$($executorRuleCommand.id)/approve"

Invoke-RestMethod `
  -Method Put `
  -Uri "http://localhost:8080/executors/$($executor.id)/heartbeat" `
  -ContentType "application/json" `
  -Body (@{
    status = "online"
    metadata = @{ last_poll = "manual-test" }
  } | ConvertTo-Json -Depth 10)

$executorCommands = Invoke-RestMethod `
  -Method Get `
  -Uri "http://localhost:8080/executors/$($executor.id)/commands/pending"

$executorCommand = $executorCommands[0]

Invoke-RestMethod `
  -Method Post `
  -Uri "http://localhost:8080/executors/$($executor.id)/commands/$($executorCommand.id)/claim"

Invoke-RestMethod `
  -Method Post `
  -Uri "http://localhost:8080/executors/$($executor.id)/commands/$($executorCommand.id)/complete" `
  -ContentType "application/json" `
  -Body (@{
    result_payload = @{ pump_state = "running" }
    verified = $true
    status = "succeeded"
    metadata = @{ source = "edge-agent-01" }
  } | ConvertTo-Json -Depth 10)

Invoke-RestMethod -Method Get -Uri "http://localhost:8080/events?command_id=$($executorCommand.id)"
Invoke-RestMethod -Method Get -Uri "http://localhost:8080/ai/context/entity/$($rulePump.id)"
```

Exercise in-memory command leases, refresh, release, and expiry recovery:

```powershell
$leasePump = Invoke-RestMethod `
  -Method Post `
  -Uri "http://localhost:8080/entities" `
  -ContentType "application/json" `
  -Body (@{
    entity_key = "lease-pump-01"
    entity_type = "aion:Pump"
    jsonld = @{
      "@context" = @{ aion = "https://aioncore.org/ns#" }
      "@id" = "urn:aion:building:lease-pump:01"
      "@type" = "aion:Pump"
      name = "Lease Pump 01"
    }
  } | ConvertTo-Json -Depth 10)

$leasePumpCapability = @{
  capability_name = "StartPump"
  command_type = "StartPump"
  input_schema = @{ type = "object"; additionalProperties = $true }
  metadata = @{ source = "manual-test" }
}

Invoke-RestMethod `
  -Method Put `
  -Uri "http://localhost:8080/entities/$($leasePump.id)/capabilities" `
  -ContentType "application/json" `
  -Body (ConvertTo-Json -InputObject (, $leasePumpCapability) -Depth 10)

Invoke-RestMethod `
  -Method Put `
  -Uri "http://localhost:8080/policies" `
  -ContentType "application/json" `
  -Body (ConvertTo-Json -InputObject (, @{
    target_entity_id = $leasePump.id
    command_type = "StartPump"
    requires_approval = $true
    auto_execute_allowed = $false
    metadata = @{ reason = "lease test requires approval" }
  }) -Depth 10)

$leaseCommand = Invoke-RestMethod `
  -Method Post `
  -Uri "http://localhost:8080/commands" `
  -ContentType "application/json" `
  -Body (@{
    target_entity_id = $leasePump.id
    command_type = "StartPump"
    payload = @{ target_state = "running" }
    requested_by = "operator@example.com"
    reason = "Test command lease recovery"
  } | ConvertTo-Json -Depth 10)

$leaseExecutor = Invoke-RestMethod `
  -Method Post `
  -Uri "http://localhost:8080/executors" `
  -ContentType "application/json" `
  -Body (@{
    agent_key = "edge-agent-01"
    agent_type = "edge"
    display_name = "Edge Agent 01"
    status = "online"
    metadata = @{ site = "building-01" }
  } | ConvertTo-Json -Depth 10)

Invoke-RestMethod `
  -Method Put `
  -Uri "http://localhost:8080/executors/$($leaseExecutor.id)/capabilities" `
  -ContentType "application/json" `
  -Body (ConvertTo-Json -InputObject (, @{
    command_type = "StartPump"
    protocol = "local"
    metadata = @{ source = "manual-test" }
  }) -Depth 10)

Invoke-RestMethod `
  -Method Put `
  -Uri "http://localhost:8080/executors/$($leaseExecutor.id)/scopes" `
  -ContentType "application/json" `
  -Body (ConvertTo-Json -InputObject (, @{
    target_entity_id = $leasePump.id
    metadata = @{ scope = "pump-only" }
  }) -Depth 10)

try {
  Invoke-RestMethod `
    -Method Post `
    -Uri "http://localhost:8080/executors/$($leaseExecutor.id)/commands/$($leaseCommand.id)/claim" `
    -ContentType "application/json" `
    -Body (@{ lease_duration_seconds = 5 } | ConvertTo-Json)
} catch {
  $_.ErrorDetails.Message
}

Invoke-RestMethod -Method Post -Uri "http://localhost:8080/commands/$($leaseCommand.id)/approve"

Invoke-RestMethod `
  -Method Post `
  -Uri "http://localhost:8080/executors/$($leaseExecutor.id)/commands/$($leaseCommand.id)/claim" `
  -ContentType "application/json" `
  -Body (@{
    lease_duration_seconds = 5
    max_retries = 2
    metadata = @{ source = "manual-lease-test" }
  } | ConvertTo-Json -Depth 10)

Invoke-RestMethod -Method Get -Uri "http://localhost:8080/commands/$($leaseCommand.id)/lease"

Invoke-RestMethod `
  -Method Post `
  -Uri "http://localhost:8080/commands/$($leaseCommand.id)/lease/refresh" `
  -ContentType "application/json" `
  -Body (@{
    executor_id = $leaseExecutor.id
    lease_duration_seconds = 10
  } | ConvertTo-Json -Depth 10)

Invoke-RestMethod `
  -Method Post `
  -Uri "http://localhost:8080/commands/$($leaseCommand.id)/lease/release" `
  -ContentType "application/json" `
  -Body (@{ executor_id = $leaseExecutor.id } | ConvertTo-Json)

Invoke-RestMethod `
  -Method Post `
  -Uri "http://localhost:8080/executors/$($leaseExecutor.id)/commands/$($leaseCommand.id)/claim" `
  -ContentType "application/json" `
  -Body (@{
    lease_duration_seconds = 1
    max_retries = 2
    metadata = @{ source = "expiry-test" }
  } | ConvertTo-Json -Depth 10)

Start-Sleep -Seconds 2

Invoke-RestMethod -Method Post -Uri "http://localhost:8080/commands/recover-expired-leases"
Invoke-RestMethod -Method Get -Uri "http://localhost:8080/events?command_id=$($leaseCommand.id)"
```

Build AI context for a smart-building water tank and pump:

```powershell
$contextTank = Invoke-RestMethod `
  -Method Post `
  -Uri "http://localhost:8080/entities" `
  -ContentType "application/json" `
  -Body (@{
    entity_key = "context-water-tank-01"
    entity_type = "aion:WaterTank"
    jsonld = @{
      "@context" = @{ aion = "https://aioncore.org/ns#" }
      "@id" = "urn:aion:building:context-water-tank:01"
      "@type" = "aion:WaterTank"
      name = "Context Water Tank 01"
    }
  } | ConvertTo-Json -Depth 10)

$contextPump = Invoke-RestMethod `
  -Method Post `
  -Uri "http://localhost:8080/entities" `
  -ContentType "application/json" `
  -Body (@{
    entity_key = "context-pump-01"
    entity_type = "aion:Pump"
    jsonld = @{
      "@context" = @{ aion = "https://aioncore.org/ns#" }
      "@id" = "urn:aion:building:context-pump:01"
      "@type" = "aion:Pump"
      name = "Context Pump 01"
    }
  } | ConvertTo-Json -Depth 10)

Invoke-RestMethod `
  -Method Post `
  -Uri "http://localhost:8080/relationships" `
  -ContentType "application/json" `
  -Body (@{
    source_entity_id = $contextPump.id
    relationship_type = "aion:fills"
    target_entity_id = $contextTank.id
    jsonld = @{ "@type" = "aion:Relationship" }
  } | ConvertTo-Json -Depth 10)

Invoke-RestMethod `
  -Method Post `
  -Uri "http://localhost:8080/observations" `
  -ContentType "application/json" `
  -Body (@{
    producer_entity_id = $contextPump.id
    feature_of_interest_id = $contextTank.id
    observed_property = "water_level"
    value = @{ type = "number"; value = 24.5 }
    unit = "%"
    observed_at = "2026-04-27T13:00:00Z"
    received_at = "2026-04-27T13:00:01Z"
    protocol = "http"
    payload_format = "json_mapping"
    quality = @{}
    metadata = @{}
  } | ConvertTo-Json -Depth 10)

Invoke-RestMethod `
  -Method Post `
  -Uri "http://localhost:8080/events" `
  -ContentType "application/json" `
  -Body (@{
    event_type = "aion:LowWaterLevel"
    severity = "warning"
    target_entity_id = $contextTank.id
    message = "Water level is below target"
    occurred_at = "2026-04-27T13:00:05Z"
    metadata = @{ threshold = 30 }
  } | ConvertTo-Json -Depth 10)

$contextCommand = Invoke-RestMethod `
  -Method Post `
  -Uri "http://localhost:8080/commands" `
  -ContentType "application/json" `
  -Body (@{
    target_entity_id = $contextPump.id
    command_type = "StartPump"
    payload = @{ target_state = "running" }
    requested_by = "operator@example.com"
    reason = "Water tank level is low"
  } | ConvertTo-Json -Depth 10)

Invoke-RestMethod -Method Post -Uri "http://localhost:8080/commands/$($contextCommand.id)/approve"

$contextClaimed = Invoke-RestMethod `
  -Method Post `
  -Uri "http://localhost:8080/commands/$($contextCommand.id)/claim" `
  -ContentType "application/json" `
  -Body (@{ claimed_by = "edge-agent-01" } | ConvertTo-Json)

$contextAction = Invoke-RestMethod `
  -Method Post `
  -Uri "http://localhost:8080/actions" `
  -ContentType "application/json" `
  -Body (@{
    command_id = $contextClaimed.id
    executor_entity_id = $contextPump.id
    action_type = "StartPump"
    status = "started"
    started_at = "2026-04-27T13:01:00Z"
    metadata = @{ executor = "edge-agent-01" }
  } | ConvertTo-Json -Depth 10)

Invoke-RestMethod -Method Post -Uri "http://localhost:8080/commands/$($contextClaimed.id)/mark-executed"

Invoke-RestMethod `
  -Method Post `
  -Uri "http://localhost:8080/action-results" `
  -ContentType "application/json" `
  -Body (@{
    command_id = $contextClaimed.id
    action_id = $contextAction.id
    status = "succeeded"
    verified = $true
    result_payload = @{ pump_state = "running" }
    observed_at = "2026-04-27T13:01:30Z"
    metadata = @{ verification_source = "simulated_executor" }
  } | ConvertTo-Json -Depth 10)

Invoke-RestMethod -Method Get -Uri "http://localhost:8080/ai/context/entity/$($contextTank.id)"
Invoke-RestMethod -Method Get -Uri "http://localhost:8080/ai/context/entity/$($contextPump.id)?limit=10"
```

List and invoke local MCP-ready tools:

```powershell
Invoke-RestMethod -Method Get -Uri "http://localhost:8080/mcp/tools"

$mcpTank = Invoke-RestMethod `
  -Method Post `
  -Uri "http://localhost:8080/entities" `
  -ContentType "application/json" `
  -Body (@{
    entity_key = "mcp-water-tank-01"
    entity_type = "aion:WaterTank"
    jsonld = @{
      "@context" = @{ aion = "https://aioncore.org/ns#" }
      "@id" = "urn:aion:building:mcp-water-tank:01"
      "@type" = "aion:WaterTank"
      name = "MCP Water Tank 01"
    }
  } | ConvertTo-Json -Depth 10)

$mcpPump = Invoke-RestMethod `
  -Method Post `
  -Uri "http://localhost:8080/entities" `
  -ContentType "application/json" `
  -Body (@{
    entity_key = "mcp-pump-01"
    entity_type = "aion:Pump"
    jsonld = @{
      "@context" = @{ aion = "https://aioncore.org/ns#" }
      "@id" = "urn:aion:building:mcp-pump:01"
      "@type" = "aion:Pump"
      name = "MCP Pump 01"
    }
  } | ConvertTo-Json -Depth 10)

Invoke-RestMethod `
  -Method Post `
  -Uri "http://localhost:8080/observations" `
  -ContentType "application/json" `
  -Body (@{
    producer_entity_id = $mcpPump.id
    feature_of_interest_id = $mcpTank.id
    observed_property = "water_level"
    value = @{ type = "number"; value = 22.0 }
    unit = "%"
    observed_at = "2026-04-27T13:05:00Z"
    received_at = "2026-04-27T13:05:01Z"
    protocol = "http"
    payload_format = "json_mapping"
    quality = @{}
    metadata = @{}
  } | ConvertTo-Json -Depth 10)

Invoke-RestMethod `
  -Method Post `
  -Uri "http://localhost:8080/events" `
  -ContentType "application/json" `
  -Body (@{
    event_type = "aion:LowWaterLevel"
    severity = "warning"
    target_entity_id = $mcpTank.id
    message = "Water level is low"
    occurred_at = "2026-04-27T13:05:05Z"
    metadata = @{ threshold = 30 }
  } | ConvertTo-Json -Depth 10)

$mcpCommand = Invoke-RestMethod `
  -Method Post `
  -Uri "http://localhost:8080/commands" `
  -ContentType "application/json" `
  -Body (@{
    target_entity_id = $mcpPump.id
    command_type = "StartPump"
    payload = @{ target_state = "running" }
    requested_by = "operator@example.com"
    reason = "Water tank level is low"
  } | ConvertTo-Json -Depth 10)

Invoke-RestMethod `
  -Method Post `
  -Uri "http://localhost:8080/mcp/tools/build_ai_context" `
  -ContentType "application/json" `
  -Body (@{
    arguments = @{
      entity_id = $mcpTank.id
      include_observations = $true
      include_events = $true
      include_commands = $true
      limit = 10
    }
  } | ConvertTo-Json -Depth 10)

Invoke-RestMethod `
  -Method Post `
  -Uri "http://localhost:8080/mcp/tools/get_pending_commands" `
  -ContentType "application/json" `
  -Body (@{
    arguments = @{
      target_entity_id = $mcpPump.id
    }
  } | ConvertTo-Json -Depth 10)
```

Use the minimal JSON-RPC MCP compatibility endpoint for local client testing:

```powershell
Invoke-RestMethod `
  -Method Post `
  -Uri "http://localhost:8080/mcp" `
  -ContentType "application/json" `
  -Body (@{
    jsonrpc = "2.0"
    id = 1
    method = "tools/list"
    params = @{}
  } | ConvertTo-Json -Depth 10)

Invoke-RestMethod `
  -Method Post `
  -Uri "http://localhost:8080/mcp" `
  -ContentType "application/json" `
  -Body (@{
    jsonrpc = "2.0"
    id = 2
    method = "tools/call"
    params = @{
      name = "list_entities"
      arguments = @{}
    }
  } | ConvertTo-Json -Depth 10)

Invoke-RestMethod `
  -Method Post `
  -Uri "http://localhost:8080/mcp" `
  -ContentType "application/json" `
  -Body (@{
    jsonrpc = "2.0"
    id = 3
    method = "tools/call"
    params = @{
      name = "build_ai_context"
      arguments = @{
        entity_id = $mcpTank.id
        include_observations = $true
        include_events = $true
        include_commands = $true
        limit = 10
      }
    }
  } | ConvertTo-Json -Depth 10)
```

The `/mcp` endpoint is a minimal localhost-development compatibility layer for the MCP `tools/list` and `tools/call` JSON-RPC flow. Do not expose it publicly without authentication and Origin validation.

In-memory data is lost when the process exits.
