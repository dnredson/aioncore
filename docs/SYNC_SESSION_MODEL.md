# AionCore Sync Session Model

A sync session represents a bounded reconnection/backfill window from an edge or fog producer. It is designed for field IoT deployments where SmartSentinel, Aion Edge Adapter, MiNiFi, NiFi, or a custom gateway buffers data locally while disconnected and later sends many records after connectivity returns.

A sync session is not a transport runtime. It is an operational tracking record that correlates batches, raw messages, DLQ records, provenance, and dashboard status around a logical synchronization episode.

## Concepts

A `SyncSession` has two identifiers:

- `id`: the AionCore UUID used by API routes.
- `sync_session_id`: the external/session key supplied by an edge producer, NiFi/MiNiFi flow, SmartSentinel snapshot pipeline, or operator.

The pair `tenant_id + sync_session_id` is unique. The same external session key may appear in different tenants without collision.

## Status values

- `open`: declared but no batch result has been recorded yet.
- `receiving`: one or more batches have been associated with the session.
- `completed`: operator or upstream system marked the session as complete.
- `failed`: operator or upstream system marked the session as failed.
- `abandoned`: operator marked the session as abandoned.

## Counters

Sync sessions track cumulative batch/backfill counters:

- `received_items`
- `accepted_count`
- `duplicate_count`
- `failed_count`
- `observations_created`

`POST /ingest/batch` automatically updates a session when `sync_session_id` is present in the batch request.

## Relationship to reliable ingestion

Reliable ingestion provides per-message idempotency. Batch ingestion groups many reliable envelopes. Sync sessions correlate those batches into an operational window such as:

```text
4G outage starts -> edge buffers locally -> connectivity returns -> batch/backfill sends -> sync session receives counts -> operator marks completed
```

## Relationship to NiFi/MiNiFi

NiFi/MiNiFi deployments should pass a stable `sync_session_id` in batch/backfill requests. AionCore preserves it in raw-message metadata, DLQ records, events, and sync-session records. External provenance remains evidence/correlation metadata and is not trusted as proof by itself.

## Current limitations

- There is no background sync-session worker.
- There is no automatic session timeout/abandonment.
- There is no persisted per-batch table yet.
- `POST /ingest/batch` updates cumulative counters but does not store a complete batch ledger.
- Late-data policies for rules and commands remain future work.
