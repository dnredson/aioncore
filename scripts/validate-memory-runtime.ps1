[CmdletBinding()]
param(
    [string]$BaseUrl = "http://127.0.0.1:8080"
)

$ErrorActionPreference = "Stop"

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
        $params.Body = ($Body | ConvertTo-Json -Depth 20)
    }

    Invoke-RestMethod @params
}

function Invoke-JsonRpc {
    param(
        [Parameter(Mandatory = $true)]
        [string]$MethodName,
        [object]$Params,
        [int]$Id
    )

    Invoke-ApiJson -Method Post -Path "/mcp" -Body @{
        jsonrpc = "2.0"
        id = $Id
        method = $MethodName
        params = $Params
    }
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
        [string]$IdSuffix,
        [Parameter(Mandatory = $true)]
        [string]$Name
    )

    @{
        entity_key = $EntityKey
        entity_type = $EntityType
        jsonld = @{
            "@context" = @{
                aion = "https://aioncore.org/ns#"
            }
            "@id" = "urn:aion:runtime:$IdSuffix"
            "@type" = $EntityType
            name = $Name
        }
    }
}

$health = Invoke-ApiJson -Method Get -Path "/health"
Assert-True ($health.storage -eq "memory") "Expected memory storage in /health response."

$ready = Invoke-ApiJson -Method Get -Path "/ready"
Assert-True ($ready.ready -eq $true) "Expected /ready to report ready=true."
Assert-True ($ready.storage -eq "memory") "Expected /ready to report memory storage."

$tank = Invoke-ApiJson -Method Post -Path "/entities" -Body (New-JsonLdEntityBody -EntityKey "water-tank-runtime" -EntityType "aion:WaterTank" -IdSuffix "water-tank-runtime" -Name "Water Tank Runtime")
$sensor = Invoke-ApiJson -Method Post -Path "/entities" -Body (New-JsonLdEntityBody -EntityKey "water-level-sensor-runtime" -EntityType "aion:Sensor" -IdSuffix "water-level-sensor-runtime" -Name "Water Level Sensor Runtime")

$relationship = Invoke-ApiJson -Method Post -Path "/relationships" -Body @{
    source_entity_id = $sensor.id
    relationship_type = "observes"
    target_entity_id = $tank.id
    jsonld = @{}
}
Assert-True ($relationship.relationship_type -eq "observes") "Expected relationship to be created."

$ingest = Invoke-ApiJson -Method Post -Path "/ingest/http" -Body @{
    producer_entity_id = $sensor.id
    feature_of_interest_id = $tank.id
    payload_format = "senml-json"
    protocol = "http"
    content_type = "application/senml+json"
    observed_at = "2026-04-27T13:00:00Z"
    payload = @(
        @{
            bn = "urn:aion:runtime:water-level-sensor:"
            bt = 1777294800
            n = "water_level"
            u = "%"
            v = 18.5
        }
    )
}

Assert-True ($ingest.raw_message_id) "Expected raw_message_id from ingest response."
Assert-True (($ingest.observations | Measure-Object).Count -ge 1) "Expected at least one observation from ingest response."

$observationQuery = Invoke-ApiJson -Method Get -Path ("/observations?feature_of_interest_id={0}" -f $tank.id)
Assert-True (($observationQuery | Measure-Object).Count -ge 1) "Expected observations for the water tank."

$rawMessage = Invoke-ApiJson -Method Get -Path ("/raw-messages/{0}" -f $ingest.raw_message_id)
Assert-True ($rawMessage.raw_message_id -eq $ingest.raw_message_id) "Expected raw message lookup by ID to succeed."

$events = Invoke-ApiJson -Method Get -Path ("/events?raw_message_id={0}" -f $ingest.raw_message_id)
Assert-True (($events | Measure-Object).Count -ge 1) "Expected at least one event for the raw message."

$toolsList = Invoke-JsonRpc -MethodName "tools/list" -Params @{} -Id 1
Assert-True ($toolsList.result.tools.Count -ge 1) "Expected MCP tools/list to return tool definitions."

$toolNames = $toolsList.result.tools | ForEach-Object { $_.name }
Assert-True ($toolNames -contains "build_ai_context") "Expected build_ai_context in MCP tool list."

$toolCall = Invoke-JsonRpc -MethodName "tools/call" -Params @{
    name = "build_ai_context"
    arguments = @{
        entity_id = $tank.id
    }
} -Id 2
Assert-True ($toolCall.result.structuredContent.context.target_entity.id -eq $tank.id) "Expected build_ai_context to target the water tank."

Write-Host "Memory runtime validation succeeded."
Write-Host ("Base URL: {0}" -f $BaseUrl)
Write-Host ("Health storage: {0}" -f $health.storage)
Write-Host ("Ready status: {0}" -f $ready.ready)
Write-Host ("Created entity IDs: tank={0}, sensor={1}" -f $tank.id, $sensor.id)
Write-Host ("Raw message ID: {0}" -f $ingest.raw_message_id)
Write-Host "MCP tools/list and build_ai_context completed successfully."
