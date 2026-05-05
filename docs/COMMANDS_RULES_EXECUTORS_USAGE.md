# Commands, Rules, And Executors Usage

This guide collects command, action, policy, rule, executor, and lease-oriented operational examples that were previously embedded in the root `README.md`.

For the domain background, see [Action Model](ACTION_MODEL.md).

## Commands

Create a command:

```powershell
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
```

Read commands:

```powershell
Invoke-RestMethod -Method Get -Uri "http://localhost:8080/commands"
Invoke-RestMethod -Method Get -Uri "http://localhost:8080/commands/$($command.id)"
```

## Policies

Policy example used to constrain command execution:

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
```

Read policies:

```powershell
Invoke-RestMethod -Method Get -Uri "http://localhost:8080/policies"
```

## Rules

Rules are part of the broader closed-loop model. Read examples:

```powershell
Invoke-RestMethod -Method Get -Uri "http://localhost:8080/rules"
Invoke-RestMethod -Method Get -Uri "http://localhost:8080/rules/$($ruleId)"
```

In `token` mode these require `rules:read`.

## Executors

Register a SmartSentinel-like executor:

```powershell
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

General executor reads:

```powershell
Invoke-RestMethod -Method Get -Uri "http://localhost:8080/executors"
Invoke-RestMethod -Method Get -Uri "http://localhost:8080/executors/$($sentinelExecutor.executor.id)"
Invoke-RestMethod -Method Get -Uri "http://localhost:8080/executors/$($sentinelExecutor.executor.id)/capabilities"
Invoke-RestMethod -Method Get -Uri "http://localhost:8080/executors/$($sentinelExecutor.executor.id)/scopes"
```

## Executor Polling

SmartSentinel bridge polling:

```powershell
$sentinelCommands = Invoke-RestMethod `
  -Method Get `
  -Uri "http://localhost:8080/integrations/smartsentinel/executors/$($sentinelExecutor.executor.id)/commands"
```

Machine-oriented polling in token mode requires the executor or SmartSentinel executor polling scopes documented in [Authentication Usage](AUTH_USAGE.md).

## Command Leases

Claim a command with a lease:

```powershell
$claimed = Invoke-RestMethod `
  -Method Post `
  -Uri "http://localhost:8080/integrations/smartsentinel/executors/$($sentinelExecutor.executor.id)/commands/$($command.id)/claim" `
  -ContentType "application/json" `
  -Body (@{
    lease_duration_seconds = 60
    max_retries = 1
    metadata = @{ source = "readme-smoke" }
  } | ConvertTo-Json -Depth 8)
```

This captures the command-lease flow without executing the action inside AionCore itself.

## Actions And Action Results

Report execution results back through the bridge:

```powershell
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
```

Read surfaces:

```powershell
Invoke-RestMethod -Method Get -Uri "http://localhost:8080/actions"
Invoke-RestMethod -Method Get -Uri "http://localhost:8080/actions/$($reported.action.id)"
Invoke-RestMethod -Method Get -Uri "http://localhost:8080/action-results"
```

## Approval Example

If the command policy requires approval, approve the command before the executor claim:

```powershell
Invoke-RestMethod -Method Post -Uri "http://localhost:8080/commands/$($command.id)/approve"
```
