[CmdletBinding()]
param(
    [string]$DatabaseUrl = $env:AIONCORE_DATABASE_URL
)

$ErrorActionPreference = "Stop"

if ([string]::IsNullOrWhiteSpace($DatabaseUrl)) {
    throw "Provide -DatabaseUrl or set AIONCORE_DATABASE_URL before starting postgres runtime."
}

$env:AIONCORE_STORAGE_BACKEND = "postgres"
$env:AIONCORE_DATABASE_URL = $DatabaseUrl
cargo run -p aion-api
