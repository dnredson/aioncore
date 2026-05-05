# SmartSentinel Usage

This guide collects the SmartSentinel operational examples that were previously embedded in the root `README.md`.

For the design model, see [SmartSentinel Integration Model](SMARTSENTINEL_INTEGRATION.md).

## Snapshot Ingestion Example

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

## Query Materialized Records

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

## Validation Failure Example

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

## Provenance And Evidence Example

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

Query evidence and provenance metadata:

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

Operational provenance queries:

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

## Executor Bridge Example

Register a SmartSentinel-like executor bridge and report a dry-run command result. These endpoints do not execute recovery actions inside AionCore.

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
```

Poll, claim, and report:

```powershell
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
