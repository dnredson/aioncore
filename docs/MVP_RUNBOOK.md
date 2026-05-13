# AionCore MVP Runbook

This runbook describes the validated local MVP operating path after Milestone 109.

It is intentionally local/dev focused. It does not define a production deployment.

See also:

- [Configuration](CONFIGURATION.md)
- [MVP Demo Scenario](MVP_DEMO_SCENARIO.md)
- [MVP Scope Freeze](MVP_SCOPE_FREEZE.md)
- [Release Checklist](RELEASE_CHECKLIST.md)

## 1. Start `aion-api` In Default Memory/Dev Mode

From the repository root:

```powershell
cargo run -p aion-api
```

Expected behavior:

- `aion-api` starts on `http://127.0.0.1:8080`
- storage backend is `memory`
- auth mode defaults to `dev` when unset
- no PostgreSQL, external broker, or external service is required

## 2. Enable Static Dashboard Hosting Under `/ui`

Set the dashboard directory before starting the API:

```powershell
$env:AIONCORE_DASHBOARD_STATIC_DIR = "apps/aion-dashboard"
cargo run -p aion-api
```

Expected behavior:

- `GET /ui`
- `GET /ui/`
- dashboard static assets under `GET /ui/*`

Notes:

- dashboard assets are served from disk, not embedded
- if `AIONCORE_DASHBOARD_STATIC_DIR` is unset or empty, `/ui` is not served
- `/dashboard/*` API routes remain available and are not shadowed

## 3. Run The Frozen MVP Demo Script

With the API running:

```powershell
.\scripts\demo-mvp-memory.ps1
```

Optional alternate base URL:

```powershell
.\scripts\demo-mvp-memory.ps1 -BaseUrl "http://127.0.0.1:8080"
```

The script exercises:

- `/health`
- `/ready`
- entities and relationships
- reliable ingestion and duplicate detection
- batch/backfill ingestion with sync-session tracking
- time-series discovery and query
- stored flow creation, validation, dry-run, and preview execution
- DLQ replay planning
- dashboard overview read

## 4. Open The Dashboard

If static hosting is enabled, open:

```text
http://127.0.0.1:8080/ui/
```

Expected sections for the MVP demo:

- overview
- time series
- connectors
- flows

## 5. Validate `/health` And `/ready`

Run:

```powershell
Invoke-RestMethod -Method Get -Uri "http://127.0.0.1:8080/health"
Invoke-RestMethod -Method Get -Uri "http://127.0.0.1:8080/ready"
```

Expected successful result:

- `/health` responds successfully
- `/ready.ready` is `true`
- memory mode reports `storage = memory`
- when `/ui` is enabled, dashboard static diagnostics show the path as configured and available

## 6. Expected Successful MVP Result

A successful local MVP run should demonstrate:

- local startup without external infrastructure
- raw-message-first ingestion and observation creation
- duplicate protection on reliable ingestion
- sync-session accumulation during batch/backfill
- time-series reads for created demo data
- flow validation, dry-run, and preview execution without broadening side effects
- DLQ replay planning without replay worker execution
- optional dashboard access under `/ui/`

For the demo script specifically, success ends with:

```text
AionCore MVP demo completed successfully.
```

## 7. Intentionally Out Of Scope For The MVP

The MVP runbook does not cover:

- production auth hardening
- public internet exposure
- secret-management systems
- TLS and reverse proxy deployment
- automatic enabled-flow runtime execution
- replay worker execution
- mandatory PostgreSQL deployment
- required live MQTT or TTN integrations
- dashboard build pipeline or frontend dependency installation

## 8. Local Troubleshooting

- If startup fails with missing PostgreSQL configuration, unset `AIONCORE_STORAGE_BACKEND` or provide `AIONCORE_DATABASE_URL`.
- If `/ui/` is missing, confirm `AIONCORE_DASHBOARD_STATIC_DIR=apps/aion-dashboard` points to an existing directory.
- If token mode startup fails, confirm `AIONCORE_BOOTSTRAP_ADMIN_TOKEN` is at least 24 characters long.
- If the demo script fails on the first request, confirm `aion-api` is already running and reachable at the selected base URL.
