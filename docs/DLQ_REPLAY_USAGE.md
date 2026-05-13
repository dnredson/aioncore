# DLQ Replay Usage

AionCore DLQ replay is currently a planning and operator-intent workflow. It helps operators decide what should happen to a DLQ record without executing automatic replay yet.

## Plan A Replay

```powershell
Invoke-RestMethod `
  -Method Post `
  -Uri "http://localhost:8080/dlq/records/<record-id>/replay-plan" `
  -ContentType "application/json" `
  -Body @'
{
  "target": "reliable_ingestion"
}
'@
```

The response includes:

- `eligible`
- `blockers`
- `warnings`
- `suggested_action`
- `payload_preview`
- `provenance`
- `simulated = true`
- `side_effects_performed = false`

The plan endpoint never changes the DLQ record.

## Request Replay

```powershell
Invoke-RestMethod `
  -Method Post `
  -Uri "http://localhost:8080/dlq/records/<record-id>/replay" `
  -ContentType "application/json" `
  -Body @'
{
  "target": "flow_execution",
  "operator_reason": "decoder mapping was corrected",
  "approval_reference": "ticket-123"
}
'@
```

If the record is eligible, AionCore marks it as `replay_requested` and emits the existing `aion:DlqReplayRequested` audit event. It still does not run a replay worker.

Use `simulate_only=true` to reuse the replay endpoint shape without changing status:

```powershell
Invoke-RestMethod `
  -Method Post `
  -Uri "http://localhost:8080/dlq/records/<record-id>/replay" `
  -ContentType "application/json" `
  -Body '{"simulate_only":true}'
```

## Replay Targets

`reliable_ingestion` is intended for future replay through the reliable ingestion envelope. It requires a DLQ payload and `payload_format`.

`flow_execution` is intended for future replay through a selected flow. It requires `flow_id` on the request or DLQ record, plus either a payload or `raw_message_id`.

`manual_review` is for operator inspection and does not require runtime replay material.

## Token Mode

- `POST /dlq/records/{record_id}/replay-plan` requires `dlq:read`.
- `POST /dlq/records/{record_id}/replay` requires `dlq:write`.
- `admin:all` satisfies both.

## Current Limitations

- No replay worker exists yet.
- No payload is submitted to `/ingest/reliable`.
- No flow is executed.
- No MQTT or HTTP side effects happen.
- No observations, events, commands, or DLQ records are created by replay.
