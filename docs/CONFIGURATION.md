# AionCore Configuration

This guide documents the environment variables used by the current local MVP runtime and validation workflow.

The current validated MVP is local and development focused. Most deployments should start with in-memory storage, optional static dashboard hosting, and no background workers unless they are explicitly being exercised.

See also:

- [README Quick Local Start](../README.md#quick-local-start)
- [MVP Runbook](MVP_RUNBOOK.md)
- [Runtime Validation](RUNTIME_VALIDATION.md)
- [Authentication Usage](AUTH_USAGE.md)

## Default MVP Profile

The simplest local profile is:

```powershell
$env:AIONCORE_AUTH_MODE = "dev"
$env:AIONCORE_DASHBOARD_STATIC_DIR = "apps/aion-dashboard"
cargo run -p aion-api
```

Behavior in this profile:

- storage backend is in-memory
- auth stays in local development bypass mode
- `/ui/` is served from disk if the path exists
- connector workers are disabled unless explicitly enabled
- standalone MQTT ingestion is disabled unless explicitly enabled

## Environment Variables

### Authentication

`AIONCORE_AUTH_MODE`

- Values: `dev`, `disabled`, `token`
- Default when unset: `dev`
- Purpose: selects the current auth behavior
- Local MVP recommendation: `dev`
- Notes: `token` mode is still partial and not production-ready

`AIONCORE_BOOTSTRAP_ADMIN_TOKEN`

- Required only for local bootstrap in `AIONCORE_AUTH_MODE=token`
- Purpose: local bootstrap bearer token that resolves to admin privileges
- Constraints: must be at least 24 characters long
- Notes: intended for local bootstrap/development only; remove after creating real admin tokens

### Dashboard Static Hosting

`AIONCORE_DASHBOARD_STATIC_DIR`

- Example: `apps/aion-dashboard`
- Purpose: enables optional static dashboard hosting from disk under `/ui/`
- Default when unset: disabled
- Notes:
  - this does not embed dashboard assets into the binary
  - when unset or empty, API behavior is otherwise unchanged and `/ui` is not served
  - `/dashboard/*` API routes remain separate and are not shadowed

### Storage

`AIONCORE_STORAGE_BACKEND`

- Values: `memory`, `postgres`
- Default when unset: `memory`
- Purpose: chooses the runtime storage backend
- Local MVP recommendation: leave unset or set `memory`

`AIONCORE_DATABASE_URL`

- Example: `postgres://aioncore:change-me@localhost:5432/aioncore`
- Required only when `AIONCORE_STORAGE_BACKEND=postgres`
- Purpose: PostgreSQL/TimescaleDB connection string for the runtime
- Notes: the API fails fast if `postgres` is selected without this value

`AIONCORE_TEST_DATABASE_URL`

- Example: `postgres://aioncore:change-me@localhost:5432/aioncore_test`
- Purpose: opt-in PostgreSQL adapter test database for storage and API tests
- Default when unset: PostgreSQL-specific tests skip where supported
- Notes: keep this separate from any long-lived development database when possible

### Connector Worker Runtime

`AIONCORE_CONNECTOR_WORKERS_ENABLED`

- Values: `true`, `false`, `1`, `0`
- Default when unset: `false`
- Purpose: enables dynamic connector worker startup and reconciliation
- Local MVP recommendation: keep disabled unless you are intentionally validating connector worker behavior
- Notes: this is the current worker-enable flag in code; there is no generic `AIONCORE_WORKERS_ENABLED` variable

### Standalone MQTT Ingestion Runtime

These variables apply to the standalone MQTT ingestion worker started by `aion-api` when explicitly enabled.

`AIONCORE_MQTT_ENABLED`

- Values: `true`, `false`, `1`, `0`
- Default when unset: `false`
- Purpose: enables the standalone local MQTT ingestion subscriber

`AIONCORE_MQTT_BROKER_URL`

- Default when unset: `mqtt://127.0.0.1:1883`
- Purpose: broker URL for standalone MQTT ingestion

`AIONCORE_MQTT_CLIENT_ID`

- Default when unset: `aioncore-ingest`
- Purpose: client identifier for the standalone MQTT subscriber

`AIONCORE_MQTT_TOPIC_FILTER`

- Default when unset: `aioncore/+/+/data`
- Purpose: topic filter for standalone MQTT ingestion

`AIONCORE_MQTT_PAYLOAD_FORMAT`

- Example: `senml-json`
- Purpose: payload decoder selection for standalone MQTT ingestion

`AIONCORE_MQTT_USERNAME`

- Optional
- Purpose: username for standalone MQTT authentication when needed

`AIONCORE_MQTT_PASSWORD`

- Optional
- Purpose: password for standalone MQTT authentication when needed
- Notes: do not commit real credentials

### TTN Live Validation Test Variables

These are test-only variables used by live TTN validation test paths. They are not required for the local MVP demo.

`AIONCORE_TEST_TTN_LIVE`

- Value: set to `1` to opt in
- Default when unset: live TTN tests stay disabled

`AIONCORE_TEST_TTN_BROKER_URL`

- Purpose: test broker URL for TTN live validation

`AIONCORE_TEST_TTN_TOPIC_FILTER`

- Example: `v3/example-app/devices/+/up`
- Purpose: test topic filter for TTN live validation

`AIONCORE_TEST_TTN_USERNAME`

- Purpose: TTN MQTT username for live validation tests

`AIONCORE_TEST_TTN_PASSWORD`

- Purpose: TTN MQTT password or API token for live validation tests

`AIONCORE_TEST_TTN_APPLICATION_ID`

- Default when unset in tests: `test-app`
- Purpose: test application identifier

`AIONCORE_TEST_TTN_DEVICE_ID`

- Default when unset in tests: `test-device`
- Purpose: test device identifier

## Common Local Profiles

### Memory MVP Demo

```powershell
$env:AIONCORE_AUTH_MODE = "dev"
$env:AIONCORE_DASHBOARD_STATIC_DIR = "apps/aion-dashboard"
cargo run -p aion-api
```

### Token Mode Bootstrap

```powershell
$env:AIONCORE_AUTH_MODE = "token"
$env:AIONCORE_BOOTSTRAP_ADMIN_TOKEN = "replace-with-local-bootstrap-token-min-24-chars"
$env:AIONCORE_DASHBOARD_STATIC_DIR = "apps/aion-dashboard"
cargo run -p aion-api
```

### PostgreSQL Runtime

```powershell
$env:AIONCORE_STORAGE_BACKEND = "postgres"
$env:AIONCORE_DATABASE_URL = "postgres://aioncore:change-me@localhost:5432/aioncore"
cargo run -p aion-api
```

### Standalone MQTT Ingestion

```powershell
$env:AIONCORE_MQTT_ENABLED = "true"
$env:AIONCORE_MQTT_BROKER_URL = "mqtt://127.0.0.1:1883"
$env:AIONCORE_MQTT_TOPIC_FILTER = "aioncore/+/+/data"
$env:AIONCORE_MQTT_PAYLOAD_FORMAT = "senml-json"
cargo run -p aion-api
```

### Connector Worker Validation

```powershell
$env:AIONCORE_CONNECTOR_WORKERS_ENABLED = "true"
cargo run -p aion-api
```

## Local Deployment Notes

The current MVP is not a full production deployment package.

Current state:

- local and developer-operated runtime is the validated path
- static dashboard assets are served from disk when configured, not embedded
- PostgreSQL is the first durable backend, but memory remains the reference MVP path

Production-hardening items still required later:

- stronger auth coverage and operator procedures
- secret management outside the repository
- TLS and reverse-proxy deployment decisions
- database sizing, backups, and migration procedures
- supervised worker processes and operational monitoring
- deployment-specific MQTT/TTN credential handling
