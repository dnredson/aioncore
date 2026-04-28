[CmdletBinding()]
param(
    [string]$BaseUrl = "http://127.0.0.1:8080"
)

$ErrorActionPreference = "Stop"

if ([string]::IsNullOrWhiteSpace($env:AIONCORE_DATABASE_URL)) {
    throw "AIONCORE_DATABASE_URL must be set before running postgres runtime validation."
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
        $params.Body = ($Body | ConvertTo-Json -Depth 20)
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

$health = Invoke-ApiJson -Method Get -Path "/health"
Assert-True ($health.storage -eq "postgres") "Expected postgres storage in /health response."

$ready = Invoke-ApiJson -Method Get -Path "/ready"
Assert-True ($ready.ready -eq $true) "Expected /ready to report ready=true for postgres."
Assert-True ($ready.storage -eq "postgres") "Expected /ready to report postgres storage."

$suffix = Get-Date -Format "yyyyMMddHHmmssfff"
$entity = Invoke-ApiJson -Method Post -Path "/entities" -Body @{
    entity_key = "runtime-postgres-$suffix"
    entity_type = "aion:Sensor"
    jsonld = @{
        "@context" = @{
            aion = "https://aioncore.org/ns#"
        }
        "@id" = "urn:aion:runtime:postgres:$suffix"
        "@type" = "aion:Sensor"
        name = "Runtime PostgreSQL Sensor $suffix"
    }
}

$fetched = Invoke-ApiJson -Method Get -Path ("/entities/{0}" -f $entity.id)
Assert-True ($fetched.id -eq $entity.id) "Expected entity lookup to return the created entity."

Write-Host "PostgreSQL runtime validation succeeded."
Write-Host ("Base URL: {0}" -f $BaseUrl)
Write-Host ("Health storage: {0}" -f $health.storage)
Write-Host ("Ready status: {0}" -f $ready.ready)
Write-Host ("Created entity ID: {0}" -f $entity.id)
Write-Host "Entity lookup completed successfully."
