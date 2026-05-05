# TTN Usage

This guide collects the TTN operational examples that were previously embedded in the root `README.md`.

For the ingestion architecture behind this flow, see [Ingestion Model](INGESTION_MODEL.md).

## Create TTN Demo Entities

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
```

## Create A TTN Connector

```powershell
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
```

## TTN Device Mappings

Fallback mapping:

```powershell
$ttnFallbackMapping = Invoke-RestMethod `
  -Method Post `
  -Uri "http://localhost:8080/ingestion/connectors/$($ttnConnector.id)/ttn-device-mappings" `
  -Headers @{ Authorization = "Bearer $($connectorAdminToken.raw_token)" } `
  -ContentType "application/json" `
  -Body (@{
    ttn_device_id = "soil-node-01"
    producer_entity_id = $ttnProducer.id
    feature_of_interest_id = $ttnFeature.id
    metadata = @{
      source = "fallback-local-demo"
    }
  } | ConvertTo-Json -Depth 8)
```

Application-specific mapping:

```powershell
$ttnMapping = Invoke-RestMethod `
  -Method Post `
  -Uri "http://localhost:8080/ingestion/connectors/$($ttnConnector.id)/ttn-device-mappings" `
  -Headers @{ Authorization = "Bearer $($connectorAdminToken.raw_token)" } `
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

The fallback mapping applies when no application-specific mapping exists. The `farm-app` mapping is preferred when the uplink `application_id` is `farm-app`.

List mappings with a read-only connector token:

```powershell
Invoke-RestMethod `
  -Method Get `
  -Uri "http://localhost:8080/ingestion/connectors/$($ttnConnector.id)/ttn-device-mappings" `
  -Headers @{ Authorization = "Bearer $($connectorReadToken.raw_token)" }
```

## Local TTN Validation Without A Live Broker

Ingest a sample TTN uplink through connector-aware HTTP ingestion:

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
```

Inspect normalized records:

```powershell
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

## TTN Connector Validation

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

Validation readiness:

- `ready`: no blocking issues, connector enabled, and at least one enabled TTN device mapping exists
- `degraded`: no blocking issues, but the connector is disabled or has warnings
- `invalid`: deterministic configuration checks found blocking issues

Missing mappings and likely missing public-broker authentication are warnings:

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

## TTN Secret And Credential Validation

Create a connector secret for TTN MQTT basic auth:

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
```

Attach it to the connector and validate again:

```powershell
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

`$ttnSecret.secret_value` is empty in API responses because secret values are write-only.

## TTN Live-Readiness Dry Run

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

The dry-run plan always includes `no_network_call_performed`.

Run the preflight endpoint in dry-run-only mode:

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

## Optional TTN Live Preflight

Only run this when you intentionally want AionCore to connect to the configured broker and subscribe to the uplink topic:

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

When `expect_message = $false`, success means the MQTT connection and subscription completed. When `expect_message = $true`, success also requires at least one matching message before the timeout. Preflight messages are not ingested and secret values remain write-only.

## Mapping Updates And Duplicate Protection

Update and delete mappings:

```powershell
$updatedMapping = Invoke-RestMethod `
  -Method Patch `
  -Uri "http://localhost:8080/ingestion/connectors/$($ttnConnector.id)/ttn-device-mappings/$($ttnMapping.id)" `
  -Headers @{ Authorization = "Bearer $($connectorAdminToken.raw_token)" } `
  -ContentType "application/json" `
  -Body (@{
    enabled = $true
    metadata = @{
      source = "updated-local-demo"
    }
  } | ConvertTo-Json -Depth 8)

Invoke-RestMethod `
  -Method Delete `
  -Uri "http://localhost:8080/ingestion/connectors/$($ttnConnector.id)/ttn-device-mappings/$($ttnMapping.id)" `
  -Headers @{ Authorization = "Bearer $($connectorAdminToken.raw_token)" }
```

Duplicate enabled mappings for the same connector, device, and application are rejected with a conflict. Duplicate enabled fallback mappings for the same connector/device are also rejected.

If no enabled mapping matches and the request does not provide a producer entity, AionCore preserves the raw message, marks it failed, emits `aion:TtnDeviceMappingMissing`, and creates no observations.
