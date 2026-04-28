# Runtime Validation

These scripts are for local developer validation of the running API. They are
intended to be simple and Windows-friendly, with PowerShell as the primary
entrypoint.

## Memory Validation

`scripts/validate-memory-runtime.ps1` assumes `aion-api` is already running on
`http://127.0.0.1:8080` by default.

It validates:

- `/health` returns `storage = memory`
- `/ready` returns `ready = true`
- entity creation
- relationship creation
- SenML ingestion
- observation lookup
- raw message lookup
- event lookup
- MCP `tools/list`
- MCP `tools/call` for `build_ai_context`

Run it with:

```powershell
.\scripts\validate-memory-runtime.ps1
```

Override the base URL if needed:

```powershell
.\scripts\validate-memory-runtime.ps1 -BaseUrl "http://127.0.0.1:8081"
```

## PostgreSQL Validation

`scripts/validate-postgres-runtime.ps1` assumes the API is already running in
PostgreSQL mode and that `AIONCORE_DATABASE_URL` is set.

It validates:

- `/health` returns `storage = postgres`
- `/ready` returns `ready = true`
- entity creation and readback

Run it with:

```powershell
$env:AIONCORE_DATABASE_URL = "postgres://user:password@localhost:5432/aioncore"
.\scripts\validate-postgres-runtime.ps1
```

## Startup Helpers

Optional helpers wrap `cargo run -p aion-api`:

- `scripts/start-memory-runtime.ps1`
- `scripts/start-postgres-runtime.ps1`

## CI Usage Idea

The scripts can be used in a developer workflow or CI job after the API starts:

1. Start `aion-api` in memory or postgres mode.
2. Run the matching validation script.
3. Fail the job if any assertion throws.

## Expected Output

Successful runs print a short summary including:

- storage backend
- readiness result
- created entity IDs
- MCP validation completion

The scripts stop at the first failed assertion and print a PowerShell error with
the failing condition.
