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
- MQTT ingestion design, with implementation allowed after HTTP ingestion.
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
- [AI and MCP Model](docs/AI_MCP_MODEL.md)
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

## Run Locally Without Docker

The early local runtime uses in-memory storage. It does not require Docker, PostgreSQL, TimescaleDB, NATS, or Mosquitto.

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

In-memory data is lost when the process exits.
