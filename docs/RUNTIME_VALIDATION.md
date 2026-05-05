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

## Optional PostgreSQL Adapter Tests

Set `AIONCORE_TEST_DATABASE_URL` to a PostgreSQL database with the required extensions available, then run the storage crate tests that target the adapter:

```powershell
$env:AIONCORE_TEST_DATABASE_URL = "postgres://user:password@localhost:5432/aioncore"
cargo test -p aion-storage postgres_
```

If the environment variable is unset, the PostgreSQL adapter tests skip cleanly and the normal in-memory test suite still passes.

Connector persistence parity:

```powershell
$env:AIONCORE_TEST_DATABASE_URL = "postgres://user:password@localhost:5432/aioncore"
cargo test -p aion-storage postgres_parity_ingestion_connectors
```

Telemetry parity:

```powershell
$env:AIONCORE_TEST_DATABASE_URL = "postgres://user:password@localhost:5432/aioncore"
cargo test -p aion-storage postgres_parity_raw_messages
cargo test -p aion-storage postgres_parity_observations
cargo test -p aion-storage postgres_parity_events
```

Lifecycle parity:

```powershell
$env:AIONCORE_TEST_DATABASE_URL = "postgres://user:password@localhost:5432/aioncore"
cargo test -p aion-storage postgres_parity_commands_actions_and_results
cargo test -p aion-storage postgres_parity_command_leases
cargo test -p aion-storage postgres_parity_rules
```

If a postgres runtime URL is available, you can also validate API startup explicitly:

```powershell
$env:AIONCORE_STORAGE_BACKEND = "postgres"
$env:AIONCORE_DATABASE_URL = $env:AIONCORE_TEST_DATABASE_URL
cargo run -p aion-api
```

## Startup Helpers

Optional helpers wrap `cargo run -p aion-api`:

- `scripts/start-memory-runtime.ps1`
- `scripts/start-postgres-runtime.ps1`

Example startup commands:

```powershell
.\scripts\start-memory-runtime.ps1
.\scripts\start-postgres-runtime.ps1 -DatabaseUrl "postgres://user:password@localhost:5432/aioncore"
```

## Troubleshooting

- If startup exits with `AIONCORE_DATABASE_URL is required when AIONCORE_STORAGE_BACKEND=postgres`, set the database URL before starting the API.
- If `/ready` returns not ready in postgres mode, verify database connectivity and confirm the migrations can run against the target database.
- If an unknown backend value is set, the API fails fast instead of silently changing modes.

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
