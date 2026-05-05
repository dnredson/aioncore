# Authentication Usage

This guide collects the operational authentication examples that were previously in the root `README.md`.

For the architecture and roadmap context, also see [Security Model](SECURITY_MODEL.md).

## Current Status

- `AIONCORE_AUTH_MODE=dev` is still the default when unset.
- `token` mode enforces only selected endpoint groups.
- Enforcement level in `token` mode is still partial.
- Tenant/resource ownership checks currently apply only to selected protected read surfaces.
- Writes and full ownership enforcement remain future work.
- The current auth model is not production-ready.

## Auth Modes

The auth-mode environment variable is:

- `AIONCORE_AUTH_MODE=dev|disabled|token`

Current behavior:

- `dev`: default when unset; auth middleware is installed, requests are allowed through, readiness reports `enforcement_level = none`, and development bypass is active.
- `disabled`: auth is explicitly disabled; requests are still allowed through, and readiness reports `enforcement_level = none`.
- `token`: bearer-token parsing and principal resolution are active, readiness reports `enforcement_level = partial`, and only selected endpoint groups require valid scoped tokens.

## Protected Endpoint Summary In Token Mode

Selected route groups currently protected in `token` mode:

- adapter registration and heartbeat
- executor registration, heartbeat, polling, claim, and report flows
- SmartSentinel executor bridge routes
- connector secret administration
- connector administration and selected operational reads
- selected machine ingestion routes
- selected adapter reads
- event and raw-message reads
- selected entity, observation, command, action, rule, policy, capability, and executor reads
- MCP and AI/provenance read surfaces
- API token administration

The detailed rationale and rollout are documented in [Security Model](SECURITY_MODEL.md) and the ADR series from `0044` through `0054`.

## Local Token Bootstrap

Run token mode locally with a bootstrap admin token:

```powershell
$env:AIONCORE_AUTH_MODE = "token"
$env:AIONCORE_BOOTSTRAP_ADMIN_TOKEN = "bootstrap-admin-local-token-123456"
cargo run -p aion-api
```

The bootstrap token is intended only for local bootstrap and development. Remove it after creating real admin tokens, and do not expose it publicly.

`AIONCORE_BOOTSTRAP_ADMIN_TOKEN` must be at least 24 characters long. When the presented bearer token exactly matches this environment variable, AionCore resolves an admin principal with `auth:tokens:admin` and `admin:all` without storing that bootstrap token.

Check auth state:

```powershell
Invoke-RestMethod -Method Get -Uri "http://localhost:8080/auth/whoami"
```

## Bootstrap Admin Example

```powershell
$bootstrapHeaders = @{ Authorization = "Bearer $env:AIONCORE_BOOTSTRAP_ADMIN_TOKEN" }

$tokenResponse = Invoke-RestMethod `
  -Method Post `
  -Uri "http://localhost:8080/auth/tokens" `
  -Headers $bootstrapHeaders `
  -ContentType "application/json" `
  -Body (@{
    token_name = "local-service"
    principal_type = "service"
    principal_id = "local-service"
    scopes = @("entities:read", "observations:read")
    metadata = @{ purpose = "local bootstrap" }
  } | ConvertTo-Json -Depth 8)

$tokenResponse.raw_token
```

The `raw_token` value is shown only once. AionCore stores only the token hash and token prefix.

## Token Creation Examples

Adapter registration token:

```powershell
$adapterToken = Invoke-RestMethod `
  -Method Post `
  -Uri "http://localhost:8080/auth/tokens" `
  -Headers $bootstrapHeaders `
  -ContentType "application/json" `
  -Body (@{
    token_name = "edge-adapter-01"
    principal_type = "adapter"
    principal_id = "fog-01-mqtt"
    scopes = @("adapters:register", "adapters:heartbeat")
    metadata = @{ purpose = "adapter runtime" }
  } | ConvertTo-Json -Depth 8)
```

Connector admin, connector read, and ingestion write tokens:

```powershell
$connectorAdminToken = Invoke-RestMethod `
  -Method Post `
  -Uri "http://localhost:8080/auth/tokens" `
  -Headers $bootstrapHeaders `
  -ContentType "application/json" `
  -Body (@{
    token_name = "connector-admin"
    principal_type = "service"
    principal_id = "connector-admin"
    scopes = @("connectors:admin")
    metadata = @{ purpose = "connector administration" }
  } | ConvertTo-Json -Depth 8)

$connectorReadToken = Invoke-RestMethod `
  -Method Post `
  -Uri "http://localhost:8080/auth/tokens" `
  -Headers $bootstrapHeaders `
  -ContentType "application/json" `
  -Body (@{
    token_name = "connector-read"
    principal_type = "service"
    principal_id = "connector-read"
    scopes = @("connectors:read")
    metadata = @{ purpose = "connector inspection" }
  } | ConvertTo-Json -Depth 8)

$ingestionWriteToken = Invoke-RestMethod `
  -Method Post `
  -Uri "http://localhost:8080/auth/tokens" `
  -Headers $bootstrapHeaders `
  -ContentType "application/json" `
  -Body (@{
    token_name = "connector-ingest"
    principal_type = "connector"
    principal_id = "field-http-01"
    scopes = @("ingestion:write")
    metadata = @{ purpose = "connector-aware ingestion" }
  } | ConvertTo-Json -Depth 8)
```

Read tokens for selected surfaces:

```powershell
$adapterReadToken = Invoke-RestMethod `
  -Method Post `
  -Uri "http://localhost:8080/auth/tokens" `
  -Headers $bootstrapHeaders `
  -ContentType "application/json" `
  -Body (@{
    token_name = "adapter-read"
    principal_type = "service"
    principal_id = "adapter-read"
    scopes = @("adapters:read")
    metadata = @{ purpose = "adapter inspection" }
  } | ConvertTo-Json -Depth 8)

$mcpToolsToken = Invoke-RestMethod `
  -Method Post `
  -Uri "http://localhost:8080/auth/tokens" `
  -Headers $bootstrapHeaders `
  -ContentType "application/json" `
  -Body (@{
    token_name = "local-mcp-tools"
    principal_type = "service"
    principal_id = "local-mcp-client"
    scopes = @("mcp:tools")
    metadata = @{ purpose = "local MCP access" }
  } | ConvertTo-Json -Depth 8)

$aiContextToken = Invoke-RestMethod `
  -Method Post `
  -Uri "http://localhost:8080/auth/tokens" `
  -Headers $bootstrapHeaders `
  -ContentType "application/json" `
  -Body (@{
    token_name = "local-ai-context"
    principal_type = "service"
    principal_id = "local-ai-client"
    scopes = @("ai:context:read")
    metadata = @{ purpose = "AI context reads" }
  } | ConvertTo-Json -Depth 8)

$provenanceToken = Invoke-RestMethod `
  -Method Post `
  -Uri "http://localhost:8080/auth/tokens" `
  -Headers $bootstrapHeaders `
  -ContentType "application/json" `
  -Body (@{
    token_name = "local-provenance"
    principal_type = "service"
    principal_id = "local-provenance-client"
    scopes = @("provenance:read")
    metadata = @{ purpose = "provenance search" }
  } | ConvertTo-Json -Depth 8)
```

Additional read-scope examples:

```powershell
$eventsReadToken = Invoke-RestMethod `
  -Method Post `
  -Uri "http://localhost:8080/auth/tokens" `
  -Headers $bootstrapHeaders `
  -ContentType "application/json" `
  -Body (@{
    token_name = "events-read"
    principal_type = "service"
    principal_id = "events-reader"
    scopes = @("events:read")
    metadata = @{ purpose = "event inspection" }
  } | ConvertTo-Json -Depth 8)

$rawMessagesReadToken = Invoke-RestMethod `
  -Method Post `
  -Uri "http://localhost:8080/auth/tokens" `
  -Headers $bootstrapHeaders `
  -ContentType "application/json" `
  -Body (@{
    token_name = "raw-messages-read"
    principal_type = "service"
    principal_id = "raw-messages-reader"
    scopes = @("raw-messages:read")
    metadata = @{ purpose = "raw payload inspection" }
  } | ConvertTo-Json -Depth 8)

$entitiesReadToken = Invoke-RestMethod `
  -Method Post `
  -Uri "http://localhost:8080/auth/tokens" `
  -Headers $bootstrapHeaders `
  -ContentType "application/json" `
  -Body (@{
    token_name = "entities-read"
    principal_type = "service"
    principal_id = "entities-reader"
    scopes = @("entities:read")
    metadata = @{ purpose = "entity inspection" }
  } | ConvertTo-Json -Depth 8)

$commandsReadToken = Invoke-RestMethod `
  -Method Post `
  -Uri "http://localhost:8080/auth/tokens" `
  -Headers $bootstrapHeaders `
  -ContentType "application/json" `
  -Body (@{
    token_name = "commands-read"
    principal_type = "service"
    principal_id = "commands-reader"
    scopes = @("commands:read")
    metadata = @{ purpose = "command inspection" }
  } | ConvertTo-Json -Depth 8)

$rulesReadToken = Invoke-RestMethod `
  -Method Post `
  -Uri "http://localhost:8080/auth/tokens" `
  -Headers $bootstrapHeaders `
  -ContentType "application/json" `
  -Body (@{
    token_name = "rules-read"
    principal_type = "service"
    principal_id = "rules-reader"
    scopes = @("rules:read")
    metadata = @{ purpose = "rule inspection" }
  } | ConvertTo-Json -Depth 8)
```

## Scope Usage Examples

```powershell
$eventsHeaders = @{ Authorization = "Bearer $($eventsReadToken.raw_token)" }
$rawMessagesHeaders = @{ Authorization = "Bearer $($rawMessagesReadToken.raw_token)" }
$entitiesHeaders = @{ Authorization = "Bearer $($entitiesReadToken.raw_token)" }
$observationsHeaders = @{ Authorization = "Bearer $($tokenResponse.raw_token)" }
$commandsHeaders = @{ Authorization = "Bearer $($commandsReadToken.raw_token)" }
$rulesHeaders = @{ Authorization = "Bearer $($rulesReadToken.raw_token)" }

Invoke-RestMethod `
  -Method Get `
  -Uri "http://localhost:8080/events" `
  -Headers $eventsHeaders

Invoke-RestMethod `
  -Method Get `
  -Uri "http://localhost:8080/raw-messages" `
  -Headers $rawMessagesHeaders

Invoke-RestMethod `
  -Method Get `
  -Uri "http://localhost:8080/entities" `
  -Headers $entitiesHeaders

Invoke-RestMethod `
  -Method Get `
  -Uri "http://localhost:8080/commands" `
  -Headers $commandsHeaders

Invoke-RestMethod `
  -Method Get `
  -Uri "http://localhost:8080/rules" `
  -Headers $rulesHeaders
```

Token-mode behavior for these scoped surfaces:

- no bearer token or an invalid bearer token returns `401`
- a valid bearer token without the required scope returns `403`
- `admin:all` satisfies all route scope checks
- on selected protected read routes, `admin:all` also bypasses tenant/resource ownership checks
- on those selected protected read routes, non-admin tokens can read only resources whose `tenant_id` matches the principal tenant
- selected list/query endpoints return only same-tenant resources
- selected detail endpoints return `403` for known cross-tenant access

## 401 And 403 Examples

```powershell
try {
  Invoke-RestMethod -Method Get -Uri "http://localhost:8080/events"
} catch {
  $_.Exception.Response.StatusCode.value__
}

try {
  Invoke-RestMethod `
    -Method Get `
    -Uri "http://localhost:8080/raw-messages" `
    -Headers $eventsHeaders
} catch {
  $_.Exception.Response.StatusCode.value__
}
```

The first request returns `401` in `token` mode because no bearer token was provided. The second returns `403` because `events:read` does not satisfy `raw-messages:read`.

Missing-token and wrong-scope examples:

```powershell
try {
  Invoke-RestMethod `
    -Method Post `
    -Uri "http://localhost:8080/adapters" `
    -ContentType "application/json" `
    -Body (@{
      adapter_key = "missing-token"
      display_name = "Missing Token"
      adapter_type = "edge"
      status = "online"
    } | ConvertTo-Json -Depth 8)
} catch {
  ($_.ErrorDetails.Message | ConvertFrom-Json).error
}
```

```powershell
$wrongScopeToken = Invoke-RestMethod `
  -Method Post `
  -Uri "http://localhost:8080/auth/tokens" `
  -Headers $bootstrapHeaders `
  -ContentType "application/json" `
  -Body (@{
    token_name = "wrong-scope"
    principal_type = "service"
    scopes = @("entities:read")
  } | ConvertTo-Json -Depth 8)

try {
  Invoke-RestMethod `
    -Method Post `
    -Uri "http://localhost:8080/adapters" `
    -Headers @{ Authorization = "Bearer $($wrongScopeToken.raw_token)" } `
    -ContentType "application/json" `
    -Body (@{
      adapter_key = "wrong-scope-adapter"
      display_name = "Wrong Scope"
      adapter_type = "edge"
      status = "online"
    } | ConvertTo-Json -Depth 8)
} catch {
  ($_.ErrorDetails.Message | ConvertFrom-Json).error
}
```

## Tenant And Resource Ownership Examples

PowerShell examples for local tenant-ownership validation against a seeded multi-tenant test setup:

```powershell
$tenantAHeaders = @{ Authorization = "Bearer $($tenantAReadToken.raw_token)" }
$tenantBHeaders = @{ Authorization = "Bearer $($tenantBReadToken.raw_token)" }
$adminHeaders = @{ Authorization = "Bearer $($adminAllToken.raw_token)" }

# tenant A token reads tenant A resources
Invoke-RestMethod -Method Get -Uri "http://localhost:8080/entities/$tenantAEntityId" -Headers $tenantAHeaders
Invoke-RestMethod -Method Get -Uri "http://localhost:8080/entities" -Headers $tenantAHeaders

# tenant B token is denied on tenant A detail reads
Invoke-RestMethod -Method Get -Uri "http://localhost:8080/entities/$tenantAEntityId" -Headers $tenantBHeaders

# admin:all can read across tenants
Invoke-RestMethod -Method Get -Uri "http://localhost:8080/entities/$tenantAEntityId" -Headers $adminHeaders
Invoke-RestMethod -Method Get -Uri "http://localhost:8080/entities" -Headers $adminHeaders
```

Current limitations:

- ownership checks cover selected protected read surfaces only
- writes are not broadly tenant-authorized yet
- full cross-route coverage remains future work

## Token Revocation Example

```powershell
Invoke-RestMethod `
  -Method Post `
  -Uri "http://localhost:8080/auth/tokens/$($tokenResponse.token.id)/revoke" `
  -Headers $bootstrapHeaders `
  -ContentType "application/json" `
  -Body "{}"
```
