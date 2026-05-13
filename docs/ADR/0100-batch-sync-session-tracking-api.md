# ADR 0100: Batch Sync-Session Tracking API

## Status

Accepted.

## Context

AionCore supports reliable single-message ingestion, batch/backfill ingestion, DLQ records, and NiFi/MiNiFi provenance conventions. Field deployments often lose 4G or WAN connectivity and later send many buffered records after reconnection. Operators need a way to track the reconnection window as a coherent unit instead of inspecting isolated raw messages or DLQ entries.

## Decision

AionCore adds a first-class tenant-scoped `SyncSession` model and API:

- `POST /sync-sessions`
- `GET /sync-sessions`
- `GET /sync-sessions/{session_id}`
- `PATCH /sync-sessions/{session_id}`
- `PATCH /sync-sessions/{session_id}/status`
- `POST /sync-sessions/{session_id}/complete`
- `POST /sync-sessions/{session_id}/fail`

`POST /ingest/batch` updates or creates a sync session automatically when `sync_session_id` is present. The model tracks cumulative accepted, duplicate, failed, and observation counts.

## Consequences

Sync sessions make reconnect/backfill operations visible to the API and dashboard without introducing a batch-session worker or replay runtime. They provide an explicit bridge between reliable ingestion, NiFi/MiNiFi provenance, DLQ records, and future late-data policies.

## Non-goals

- No replay execution.
- No automatic DLQ routing.
- No automatic timeout/abandonment worker.
- No per-batch ledger table.
- No changes to idempotency semantics.
