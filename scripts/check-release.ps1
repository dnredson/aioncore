[CmdletBinding()]
param(
    [string]$CargoTargetDir
)

$ErrorActionPreference = "Stop"

function Invoke-CheckStep {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Label,
        [Parameter(Mandatory = $true)]
        [scriptblock]$Action
    )

    Write-Host ("==> {0}" -f $Label)
    & $Action
}

if (-not [string]::IsNullOrWhiteSpace($CargoTargetDir)) {
    $env:CARGO_TARGET_DIR = $CargoTargetDir
    Write-Host ("Using CARGO_TARGET_DIR={0}" -f $env:CARGO_TARGET_DIR)
}

Invoke-CheckStep -Label "cargo fmt --all" -Action { cargo fmt --all }
Invoke-CheckStep -Label "cargo build -p aion-api" -Action { cargo build -p aion-api }
Invoke-CheckStep -Label "cargo test -p aion-storage" -Action { cargo test -p aion-storage }
Invoke-CheckStep -Label "cargo test -p aion-api" -Action { cargo test -p aion-api }
Invoke-CheckStep -Label "git diff --check" -Action { git diff --check }
Invoke-CheckStep -Label "git ls-files target" -Action { git ls-files target }
Invoke-CheckStep -Label "git ls-files target_smoke" -Action { git ls-files target_smoke }
Invoke-CheckStep -Label "git ls-files node_modules" -Action { git ls-files node_modules }
Invoke-CheckStep -Label "git ls-files smoke-*.log" -Action { git ls-files smoke-*.log }

Write-Host "Release checks completed successfully."
