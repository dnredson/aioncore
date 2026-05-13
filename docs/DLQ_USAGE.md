# DLQ Usage

This guide covers the Milestone 84 DLQ model and API foundation.

Milestone 85 note:

- `POST /ingest/reliable` now preserves upstream provenance and idempotency metadata in raw messages and failure events
- future DLQ routing can reuse that preserved metadata
- reliable-ingestion failures still do not create `DlqRecord` automatically in this milestone

Milestone 86 note:

- `POST /ingest/batch` now preserves the same upstream provenance and idempotency metadata per item, including reconnect/backfill metadata such as `batch_id` and inherited `sync_session_id`
- batch item failures still do not create `DlqRecord` automatically
- later DLQ routing can reuse preserved batch and item provenance from raw messages and events

## Scope

- `POST /dlq/records`
- `GET /dlq/records`
- `GET /dlq/records/{record_id}`
- `PATCH /dlq/records/{record_id}/status`

This milestone does not execute replay and does not route ingestion failures into the DLQ automatically.

## Create A DLQ Record For A Decoding Failure

```powershell
Invoke-RestMethod -Method Post -Uri "http://localhost:8080/dlq/records" -ContentType "application/json" -Body @'
{
  "dlq_key": "decode-failure-01",
  "source_system": "custom",
  "source_id": "field-gateway-01",
  "payload_format": "senml-json",
  "payload": [{"n":"temperature","v":"bad"}],
  "failure_stage": "decoding",
  "failure_reason": "decoder rejected payload",
  "failure_detail": "temperature must be numeric",
  "retry_count": 2,
  "replay_count": 0,
  "status": "pending",
  "metadata": {
    "operator_note": "captured by trusted upstream tool"
  }
}
'@
```

## Create A DLQ Record With NiFi Provenance

```powershell
Invoke-RestMethod -Method Post -Uri "http://localhost:8080/dlq/records" -ContentType "application/json" -Body @'
{
  "dlq_key": "nifi-parse-01",
  "source_system": "nifi",
  "source_id": "plant-01-edge-01",
  "idempotency_key": "tenant-a:plant-01:pump-07:2026-05-06T12:00:00Z:41",
  "external_flow_id": "flow-edge-sync",
  "external_flow_name": "Edge Telemetry Sync",
  "external_flowfile_uuid": "2f8d0c30-7cf6-4d58-b9ac-4e53c9ad3a72",
  "external_process_group_id": "pg-plant-01",
  "external_processor_id": "proc-http-post",
  "external_provenance_uri": "nifi://provenance/events/123456",
  "sync_session_id": "sync-2026-05-06-outage-recovery-01",
  "payload_format": "senml-json",
  "payload_hash": "sha256:5ce3b778...",
  "payload": [
    {
      "bn": "pump-07",
      "n": "temperature",
      "u": "Cel",
      "v": "bad"
    }
  ],
  "failure_stage": "decoding",
  "failure_reason": "invalid numeric value",
  "retry_count": 3,
  "replay_count": 1,
  "status": "pending",
  "metadata": {
    "external.source_system": "nifi",
    "external.route": "edge-http->cloud-sync->aioncore"
  }
}
'@
```

## List Pending DLQ Records

```powershell
Invoke-RestMethod -Method Get -Uri "http://localhost:8080/dlq/records?status=pending"
```

Other supported filters:

- `failure_stage`
- `source_system`
- `connector_id`
- `flow_id`
- `raw_message_id`
- `idempotency_key`
- `external_flowfile_uuid`
- `sync_session_id`

Example:

```powershell
Invoke-RestMethod -Method Get -Uri "http://localhost:8080/dlq/records?source_system=minifi&failure_stage=validation&limit=25"
```

## Get One DLQ Record

```powershell
Invoke-RestMethod -Method Get -Uri "http://localhost:8080/dlq/records/<record-id>"
```

## Update Status

Inspecting:

```powershell
Invoke-RestMethod -Method Patch -Uri "http://localhost:8080/dlq/records/<record-id>/status" -ContentType "application/json" -Body '{"status":"inspecting"}'
```

Resolved:

```powershell
Invoke-RestMethod -Method Patch -Uri "http://localhost:8080/dlq/records/<record-id>/status" -ContentType "application/json" -Body '{"status":"resolved"}'
```

Ignored:

```powershell
Invoke-RestMethod -Method Patch -Uri "http://localhost:8080/dlq/records/<record-id>/status" -ContentType "application/json" -Body '{"status":"ignored"}'
```

Replay requested marker:

```powershell
Invoke-RestMethod -Method Patch -Uri "http://localhost:8080/dlq/records/<record-id>/status" -ContentType "application/json" -Body '{"status":"replay_requested"}'
```

`replay_requested` only records operator intent in this milestone. No replay worker runs yet.


## Replay Planning

Plan replay without changing the record:

```powershell
Invoke-RestMethod -Method Post -Uri "http://localhost:8080/dlq/records/<record-id>/replay-plan" -ContentType "application/json" -Body '{"target":"reliable_ingestion"}'
```

Request replay intent for an eligible record:

```powershell
Invoke-RestMethod -Method Post -Uri "http://localhost:8080/dlq/records/<record-id>/replay" -ContentType "application/json" -Body '{"target":"flow_execution","operator_reason":"mapping corrected","approval_reference":"ticket-123"}'
```

`/replay` marks the record as `replay_requested` when eligible. It does not execute a replay worker yet. Use `simulate_only=true` to return the replay response shape without changing status.

See [DLQ Replay Usage](DLQ_REPLAY_USAGE.md) for more examples.

## Token Mode

In `AIONCORE_AUTH_MODE=token`:

- `GET /dlq/records` and `GET /dlq/records/{record_id}` require `dlq:read`
- `POST /dlq/records` and `PATCH /dlq/records/{record_id}/status` require `dlq:write`
- `POST /dlq/records/{record_id}/replay-plan` requires `dlq:read`
- `POST /dlq/records/{record_id}/replay` requires `dlq:write`
- `admin:all` satisfies both

Read example:

```powershell
$headers = @{ Authorization = "Bearer <token-with-dlq-read>" }
Invoke-RestMethod -Method Get -Headers $headers -Uri "http://localhost:8080/dlq/records?status=pending"
```

Write example:

```powershell
$headers = @{ Authorization = "Bearer <token-with-dlq-write>" }
Invoke-RestMethod -Method Post -Headers $headers -Uri "http://localhost:8080/dlq/records" -ContentType "application/json" -Body @'
{
  "dlq_key": "trusted-tool-01",
  "failure_stage": "validation",
  "failure_reason": "schema mismatch"
}
'@
```

Tenant behavior in token mode:

- non-admin principals create DLQ records under their authenticated tenant
- non-admin principals list, get, and update only their tenant’s DLQ records
- known cross-tenant detail and update attempts return `403`
- `admin:all` can read and manage across tenants


## Sync Sessions

AionCore sync sessions provide an optional tenant-scoped tracking record for reconnect/backfill windows. NiFi, MiNiFi, SmartSentinel, or an edge adapter should reuse a stable `sync_session_id` across all batches belonging to the same outage/reconnect episode.
