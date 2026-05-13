# Sync Session Usage

Sync sessions help operators track reconnect/backfill windows from SmartSentinel, Aion Edge Adapter, MiNiFi, NiFi, or custom gateways.

## Create a sync session

```powershell
$body = @{
  sync_session_id = "farm-01-gw-01-2026-05-07-reconnect-01"
  source_system = "smartsentinel"
  source_id = "farm-01:gateway-01"
  connectivity_state = "reconnected_backfill"
  expected_items = 250
  metadata = @{ note = "4G outage recovery" }
} | ConvertTo-Json -Depth 10

Invoke-RestMethod -Method Post `
  -Uri "http://127.0.0.1:8080/sync-sessions" `
  -ContentType "application/json" `
  -Body $body
```

In token mode, `sync-sessions:write` is required.

## Link a batch/backfill ingestion request

Use the same `sync_session_id` in `POST /ingest/batch`:

```json
{
  "batch_id": "batch-001",
  "sync_session_id": "farm-01-gw-01-2026-05-07-reconnect-01",
  "source_system": "smartsentinel",
  "source_id": "farm-01:gateway-01",
  "connectivity_state": "reconnected_backfill",
  "continue_on_error": true,
  "items": []
}
```

When the batch is accepted, AionCore updates cumulative counters on the matching session, or creates it if it does not exist yet.

## List sessions

```powershell
Invoke-RestMethod "http://127.0.0.1:8080/sync-sessions?status=receiving&source_system=smartsentinel"
```

In token mode, `sync-sessions:read` is required.

## Complete a session

```powershell
Invoke-RestMethod -Method Post `
  -Uri "http://127.0.0.1:8080/sync-sessions/$SESSION_ID/complete"
```

## Mark a session as failed

```powershell
$body = @{ status = "failed"; message = "gateway stopped sending before expected count" } | ConvertTo-Json
Invoke-RestMethod -Method Patch `
  -Uri "http://127.0.0.1:8080/sync-sessions/$SESSION_ID/status" `
  -ContentType "application/json" `
  -Body $body
```

## Notes

- Sync sessions are tenant-scoped.
- Batch idempotency remains per raw message through `tenant_id + idempotency_key`.
- Sync sessions do not execute replay or DLQ processing by themselves.
- Sync sessions are suitable for dashboard views and operational audit trails.
