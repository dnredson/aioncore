[CmdletBinding()]
param(
    [string]$BaseUrl = "http://127.0.0.1:8080"
)

$ErrorActionPreference = "Stop"

function Write-Step {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Message
    )

    Write-Host ("==> {0}" -f $Message)
}

function Invoke-ApiJson {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Method,
        [Parameter(Mandatory = $true)]
        [string]$Path,
        [object]$Body
    )

    $uri = "$BaseUrl$Path"
    $params = @{
        Method = $Method
        Uri    = $uri
    }

    if ($null -ne $Body) {
        $params.ContentType = "application/json"
        if ($Body -is [string]) {
            $params.Body = $Body
        } else {
            $params.Body = ($Body | ConvertTo-Json -Depth 30)
        }
    }

    Invoke-RestMethod @params
}

function Assert-True {
    param(
        [Parameter(Mandatory = $true)]
        [object]$Condition,
        [Parameter(Mandatory = $true)]
        [string]$Message
    )

    if (-not [bool]$Condition) {
        throw $Message
    }
}

function New-JsonLdEntityBody {
    param(
        [Parameter(Mandatory = $true)]
        [string]$EntityKey,
        [Parameter(Mandatory = $true)]
        [string]$EntityType,
        [Parameter(Mandatory = $true)]
        [string]$Name
    )

    @{
        entity_key = $EntityKey
        entity_type = $EntityType
        jsonld = @{
            "@context" = @{ aion = "https://aioncore.org/ns#" }
            "@id" = "urn:aion:demo:$EntityKey"
            "@type" = $EntityType
            name = $Name
        }
    }
}

$suffix = Get-Date -Format "yyyyMMddHHmmssfff"
$syncSessionId = "demo-sync-$suffix"

Write-Host "AionCore MVP demo starting against $BaseUrl"

Write-Step "Checking health and readiness"
$health = Invoke-ApiJson -Method Get -Path "/health"
$ready = Invoke-ApiJson -Method Get -Path "/ready"
Assert-True ($ready.ready -eq $true) "Expected /ready to report ready=true."

Write-Step "Creating demo entities and relationship"
$field = Invoke-ApiJson -Method Post -Path "/entities" -Body (New-JsonLdEntityBody -EntityKey "demo-field-$suffix" -EntityType "aion:FieldSector" -Name "Demo Field Sector $suffix")
$sensor = Invoke-ApiJson -Method Post -Path "/entities" -Body (New-JsonLdEntityBody -EntityKey "demo-sensor-$suffix" -EntityType "aion:Sensor" -Name "Demo Soil Sensor $suffix")

$relationship = Invoke-ApiJson -Method Post -Path "/relationships" -Body @{
    source_entity_id = $sensor.id
    relationship_type = "observes"
    target_entity_id = $field.id
    jsonld = @{}
}
Assert-True ($relationship.relationship_type -eq "observes") "Expected observes relationship."

Write-Step "Submitting reliable ingestion and duplicate check"
$reliableBody = @{
    producer_entity_id = $sensor.id
    feature_of_interest_id = $field.id
    protocol = "http"
    payload_format = "senml-json"
    source_system = "smartsentinel"
    source_id = "farm-demo-gateway"
    sync_session_id = $syncSessionId
    idempotency_key = "demo:$suffix:soil-moisture:001"
    edge_sequence = 1
    connectivity_state = "online"
    observed_at = "2026-05-07T12:00:00Z"
    stored_at_edge = "2026-05-07T12:00:02Z"
    sent_at = "2026-05-07T12:00:05Z"
    metadata = @{ demo = "mvp" }
    payload = @(
        @{ bn = "demo:soil:"; n = "soil_moisture"; u = "%"; v = 18.5 }
    )
}

$reliable = Invoke-ApiJson -Method Post -Path "/ingest/reliable" -Body $reliableBody
Assert-True ($reliable.raw_message_id) "Expected reliable raw_message_id."
Assert-True ($reliable.duplicate -eq $false) "Expected first reliable ingest to be non-duplicate."

$duplicate = Invoke-ApiJson -Method Post -Path "/ingest/reliable" -Body $reliableBody
Assert-True ($duplicate.duplicate -eq $true) "Expected duplicate reliable ingest on second submit."

Write-Step "Submitting batch/backfill ingestion"
$batch = Invoke-ApiJson -Method Post -Path "/ingest/batch" -Body @{
    batch_id = "demo-batch-$suffix"
    sync_session_id = $syncSessionId
    source_system = "smartsentinel"
    source_id = "farm-demo-gateway"
    connectivity_state = "reconnected_backfill"
    continue_on_error = $true
    metadata = @{ demo = "mvp-backfill" }
    items = @(
        @{
            producer_entity_id = $sensor.id
            feature_of_interest_id = $field.id
            payload_format = "senml-json"
            idempotency_key = "demo:$suffix:soil-moisture:002"
            edge_sequence = 2
            observed_at = "2026-05-07T12:05:00Z"
            payload = @(@{ bn = "demo:soil:"; n = "soil_moisture"; u = "%"; v = 19.0 })
        },
        @{
            producer_entity_id = $sensor.id
            feature_of_interest_id = $field.id
            payload_format = "senml-json"
            idempotency_key = "demo:$suffix:soil-moisture:003"
            edge_sequence = 3
            observed_at = "2026-05-07T12:10:00Z"
            payload = @(@{ bn = "demo:soil:"; n = "soil_moisture"; u = "%"; v = 20.2 })
        }
    )
}
Assert-True ($batch.accepted_count -ge 1) "Expected batch accepted_count >= 1."

$syncSessions = Invoke-ApiJson -Method Get -Path ("/sync-sessions?sync_session_id={0}" -f [uri]::EscapeDataString($syncSessionId))
Assert-True (($syncSessions | Measure-Object).Count -ge 1) "Expected sync session to exist after batch."

Write-Step "Reading time-series metadata and values"
$properties = Invoke-ApiJson -Method Get -Path ("/timeseries/entities/{0}/properties" -f $field.id)
Assert-True (($properties.properties | Measure-Object).Count -ge 1) "Expected time-series properties for field."

$series = Invoke-ApiJson -Method Get -Path ("/timeseries/query?entity_id={0}&observed_property=soil_moisture&limit=10" -f $field.id)
Assert-True ($series.count -ge 1) "Expected at least one time-series point."

Write-Step "Creating, validating, and preview-executing a flow"
$flow = Invoke-ApiJson -Method Post -Path "/flows" -Body @{
    flow_key = "demo-flow-$suffix"
    name = "Demo Flow $suffix"
    description = "MVP demo flow: HTTP input -> filter -> event preview"
    enabled = $false
    nodes = @(
        @{ node_id = "source-1"; node_type = "source"; name = "HTTP Input"; config = @{ kind = "http_input" } },
        @{ node_id = "filter-1"; node_type = "filter"; name = "Moisture threshold"; config = @{ kind = "filter_condition"; field = "soil_moisture"; operator = "lt"; value = 25 } },
        @{ node_id = "sink-1"; node_type = "sink"; name = "Event Preview"; config = @{ kind = "event_create"; event_type = "aion:LowMoistureDemo"; severity = "warning"; message = "Demo low moisture event" } }
    )
    edges = @(
        @{ edge_id = "edge-1"; source_node_id = "source-1"; target_node_id = "filter-1" },
        @{ edge_id = "edge-2"; source_node_id = "filter-1"; target_node_id = "sink-1" }
    )
    metadata = @{ demo = "mvp" }
}
Assert-True ($flow.id) "Expected flow id."

$validation = Invoke-ApiJson -Method Get -Path ("/flows/{0}/validation" -f $flow.id)
Assert-True ($validation.valid -eq $true) "Expected demo flow validation to be valid."

$dryRun = Invoke-ApiJson -Method Post -Path ("/flows/{0}/dry-run" -f $flow.id) -Body @{
    sample_payload = @{ soil_moisture = 18.5 }
    payload_format = "application/json"
}
Assert-True ($dryRun.side_effects_performed -eq $false) "Expected dry-run side effects false."

$execute = Invoke-ApiJson -Method Post -Path ("/flows/{0}/execute" -f $flow.id) -Body @{
    sample_payload = @{ soil_moisture = 18.5 }
    payload_format = "application/json"
}
Assert-True ($execute.side_effects_performed -eq $false) "Expected preview execution side effects false."

Write-Step "Creating a DLQ record and replay plan"
$dlq = Invoke-ApiJson -Method Post -Path "/dlq/records" -Body @{
    dlq_key = "demo-dlq-$suffix"
    source_system = "smartsentinel"
    source_id = "farm-demo-gateway"
    sync_session_id = $syncSessionId
    idempotency_key = "demo:$suffix:bad:001"
    payload_format = "senml-json"
    payload = @(@{ n = "soil_moisture"; v = "bad"; u = "%" })
    failure_stage = "decoding"
    failure_reason = "demo invalid numeric value"
    retry_count = 1
    replay_count = 0
    status = "pending"
    metadata = @{ demo = "mvp" }
}
Assert-True ($dlq.id) "Expected DLQ record id."

$replayPlan = Invoke-ApiJson -Method Post -Path ("/dlq/records/{0}/replay-plan" -f $dlq.id) -Body @{ target = "reliable_ingestion" }
Assert-True ($replayPlan.side_effects_performed -eq $false) "Expected replay plan side effects false."

Write-Step "Reading dashboard overview"
$dashboard = Invoke-ApiJson -Method Get -Path "/dashboard/overview"
Assert-True ($dashboard.generated_at) "Expected dashboard overview generated_at."

Write-Host "AionCore MVP demo completed successfully."
Write-Host ("Base URL: {0}" -f $BaseUrl)
Write-Host ("Health storage: {0}" -f $health.storage)
Write-Host ("Field entity: {0}" -f $field.id)
Write-Host ("Sensor entity: {0}" -f $sensor.id)
Write-Host ("Reliable raw message: {0}" -f $reliable.raw_message_id)
Write-Host ("Sync session: {0}" -f $syncSessionId)
Write-Host ("Flow: {0}" -f $flow.id)
Write-Host ("DLQ record: {0}" -f $dlq.id)
Write-Host ("Time-series points returned: {0}" -f $series.count)
Write-Host ("Dashboard overview generated at: {0}" -f $dashboard.generated_at)
Write-Host "Open the dashboard at /ui/ if AIONCORE_DASHBOARD_STATIC_DIR is enabled."
