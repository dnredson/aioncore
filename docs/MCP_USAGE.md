# MCP Usage

This guide collects the operational MCP examples that were previously embedded in the root `README.md`.

For the design background, see [AI and MCP Model](AI_MCP_MODEL.md) and [Security Model](SECURITY_MODEL.md).

## Current Local MCP Surface

The local in-memory runtime exposes:

```text
GET  /mcp/tools
POST /mcp/tools/{tool_name}
POST /mcp
```

This is a development MCP-style tools HTTP surface, not a production MCP server.

Auth behavior:

- `AIONCORE_AUTH_MODE=dev` keeps the local development bypass
- `AIONCORE_AUTH_MODE=disabled` keeps auth explicitly off
- `AIONCORE_AUTH_MODE=token` requires `mcp:tools` for `GET /mcp/tools`, `POST /mcp/tools/{tool_name}`, and `POST /mcp`

## `/mcp/tools`

List available tools:

```powershell
Invoke-RestMethod `
  -Method Get `
  -Uri "http://localhost:8080/mcp/tools"
```

Token-mode example:

```powershell
$mcpHeaders = @{ Authorization = "Bearer $($mcpToolsToken.raw_token)" }

Invoke-RestMethod `
  -Method Get `
  -Uri "http://localhost:8080/mcp/tools" `
  -Headers $mcpHeaders
```

Call a tool through the direct HTTP tool route:

```powershell
Invoke-RestMethod `
  -Method Post `
  -Uri "http://localhost:8080/mcp/tools/build_ai_context" `
  -Headers $mcpHeaders `
  -ContentType "application/json" `
  -Body (@{
    entity_id = "11111111-1111-1111-1111-111111111111"
  } | ConvertTo-Json -Depth 8)
```

## `/mcp` JSON-RPC Examples

List tools through the MCP-style tools JSON-RPC endpoint:

```powershell
Invoke-RestMethod `
  -Method Post `
  -Uri "http://localhost:8080/mcp" `
  -ContentType "application/json" `
  -Body (@{
    jsonrpc = "2.0"
    id = "tools-list-1"
    method = "tools/list"
    params = @{}
  } | ConvertTo-Json -Depth 8)
```

Call a tool through JSON-RPC:

```powershell
Invoke-RestMethod `
  -Method Post `
  -Uri "http://localhost:8080/mcp" `
  -ContentType "application/json" `
  -Body (@{
    jsonrpc = "2.0"
    id = "tools-call-1"
    method = "tools/call"
    params = @{
      name = "build_ai_context"
      arguments = @{
        entity_id = "11111111-1111-1111-1111-111111111111"
      }
    }
  } | ConvertTo-Json -Depth 12)
```

## Token-Mode `mcp:tools` Example

Create a token with `mcp:tools`:

```powershell
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
```

Use it against both MCP surfaces:

```powershell
$mcpHeaders = @{ Authorization = "Bearer $($mcpToolsToken.raw_token)" }

Invoke-RestMethod `
  -Method Get `
  -Uri "http://localhost:8080/mcp/tools" `
  -Headers $mcpHeaders

Invoke-RestMethod `
  -Method Post `
  -Uri "http://localhost:8080/mcp" `
  -Headers $mcpHeaders `
  -ContentType "application/json" `
  -Body (@{
    jsonrpc = "2.0"
    id = "tools-list-token"
    method = "tools/list"
    params = @{}
  } | ConvertTo-Json -Depth 8)
```

Related protected surfaces:

- `GET /ai/context/entity/{entity_id}` requires `ai:context:read`
- `GET /provenance/search` requires `provenance:read`

Those are adjacent AI-facing surfaces but separate from the `mcp:tools` scope.

## See Also

- [AI and MCP Model](AI_MCP_MODEL.md)
- [Security Model](SECURITY_MODEL.md)
- [Authentication Usage](AUTH_USAGE.md)
- [ADR 0005: MCP-Ready AI Integration](ADR/0005-mcp-ready-ai-integration.md)
- [ADR 0044: Security Model and Auth Roadmap](ADR/0044-security-model-and-auth-roadmap.md)
- [ADR 0051: MCP, AI, and Provenance Auth Hardening](ADR/0051-mcp-ai-provenance-auth-hardening.md)
