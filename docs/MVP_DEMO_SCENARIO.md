# AionCore MVP Demo Scenario

This document freezes a practical MVP demonstration path for AionCore after Milestone 106.

The scenario is intentionally local-first and operator-friendly. It shows how AionCore can ingest telemetry, preserve raw/provenance data, handle reconnect/backfill windows, expose time-series data, inspect flows visually, simulate flow execution, and prepare DLQ replay without requiring production infrastructure.

## Demo Goals

The MVP demo should prove the following capabilities:

1. AionCore starts locally with in-memory storage.
2. The optional static dashboard can be served by `aion-api` under `/ui/`.
3. JSON-LD entities and relationships can model a small IoT domain.
4. Reliable ingestion stores raw messages, creates observations, and deduplicates by tenant-scoped idempotency key.
5. Batch/backfill ingestion can represent a reconnect window from SmartSentinel, Aion Edge Adapter, NiFi, MiNiFi, or a custom gateway.
6. Sync sessions accumulate reconnect/backfill counters.
7. Time-series APIs and the dashboard can discover entity/property data.
8. Flow definitions can be created, validated, dry-run, visually inspected, and executed in controlled preview mode.
9. DLQ records can preserve failed payloads and provenance and can be replay-planned without running a replay worker.
10. Security boundaries remain explicit: dev mode is for local demos, token mode is required before exposing machine-facing endpoints.

## Recommended Local Startup

For the simplest local demo:

```powershell
$env:AIONCORE_DASHBOARD_STATIC_DIR = "apps/aion-dashboard"
cargo run -p aion-api
```

Then open:

```text
http://127.0.0.1:8080/ui/
```

The demo script assumes the API listens at:

```text
http://127.0.0.1:8080
```

## Demo Script

Use the PowerShell script:

```powershell
scripts/demo-mvp-memory.ps1
```

Optional parameters:

```powershell
scripts/demo-mvp-memory.ps1 -BaseUrl "http://127.0.0.1:8080"
```

The script creates a unique demo suffix on each run and exercises:

- `/health`
- `/ready`
- `/entities`
- `/relationships`
- `/ingest/reliable`
- `/ingest/batch`
- `/sync-sessions`
- `/timeseries/entities/{entity_id}/properties`
- `/timeseries/query`
- `/dashboard/overview`
- `/flows`
- `/flows/{flow_id}/validation`
- `/flows/{flow_id}/dry-run`
- `/flows/{flow_id}/execute`
- `/dlq/records`
- `/dlq/records/{record_id}/replay-plan`

## Demo Storyboard

### 1. Domain model

Create two entities:

- a field sector, modeled as `aion:FieldSector`
- a soil-moisture sensor, modeled as `aion:Sensor`

Then create a relationship:

```text
sensor --observes--> field sector
```

### 2. Reliable ingestion

Submit one reliable envelope with:

- `source_system = "smartsentinel"`
- `source_id = "farm-demo-gateway"`
- stable `idempotency_key`
- `sync_session_id`
- SenML payload

Submit the same envelope again. The second response should report:

```text
duplicate = true
observations_created = 0
```

### 3. Backfill/batch ingestion

Submit two batch items using the same `sync_session_id`. AionCore should update the matching sync session counters.

### 4. Time-series exploration

Query available properties for the field entity, then query the `soil_moisture` series.

The dashboard should show the same data in the Time Series section.

### 5. Flow configuration and preview execution

Create a stored flow:

```text
http_input -> filter_condition -> event_create
```

Then call:

- validation
- dry-run
- simulated execution

The execution response must keep:

```text
simulated = true
side_effects_performed = false
```

The static dashboard can inspect the flow visually and run the same simulated execution path.

### 6. DLQ replay planning

Create a DLQ record representing a failed payload. Then call replay planning:

```text
POST /dlq/records/{record_id}/replay-plan
```

The replay plan must not execute replay. It should return blockers/warnings and a redacted payload preview.

## Expected Dashboard Views

During the demo, the dashboard should be able to show:

- overview counters
- time-series entities and properties
- connector/worker overview, even if no live broker is used
- stored flow inventory and detail
- flow validation/dry-run/execution preview results
- visual flow graph and node detail

## Demo Non-Goals

The MVP demo intentionally does not require:

- production auth hardening
- public internet exposure
- live TTN credentials
- live NiFi/MiNiFi deployment
- Kafka
- Grafana
- real MQTT/HTTP external side effects
- replay worker execution
- automatic late-data rule policy
- Cassandra

These remain post-MVP or optional integration work.

## Safety Notes

The local demo should normally run in dev auth mode. Do not expose it publicly.

When demonstrating side-effecting flow execution later, prefer local mock brokers/endpoints first. This MVP demo uses preview/simulated flow execution and avoids external sink delivery.
