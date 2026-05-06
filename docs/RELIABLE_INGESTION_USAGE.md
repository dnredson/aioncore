# Reliable Ingestion Usage

This guide covers Milestone 85: the reliable ingestion envelope and tenant-scoped idempotency-key foundation.

For the model background, also see [Ingestion Model](INGESTION_MODEL.md), [NiFi Integration Model](NIFI_INTEGRATION_MODEL.md), [DLQ Model](DLQ_MODEL.md), and [Security Model](SECURITY_MODEL.md).

## Endpoint

```text
POST /ingest/reliable
```

This endpoint is additive. Existing `POST /ingest/http` behavior is unchanged.

## Current Request Shape

Milestone 85 adds the reliable-envelope fields plus the current generic HTTP semantic targeting fields that AionCore still needs today:

- `producer_entity_id`
- `feature_of_interest_id`
- optional `protocol`
- optional `content_type`
- optional `mapping`
- reliable-envelope provenance and idempotency fields

Current runtime note:

- the reliable envelope carries upstream reliability and provenance metadata
- the generic HTTP runtime still requires explicit producer and feature IDs
- connector-aware reliable ingestion is deferred

## Example: First Reliable Ingest

```powershell
Invoke-RestMethod `
  -Method Post `
  -Uri "http://localhost:8080/ingest/reliable" `
  -ContentType "application/json" `
  -Body (@{
    producer_entity_id = "11111111-1111-1111-1111-111111111111"
    feature_of_interest_id = "22222222-2222-2222-2222-222222222222"
    protocol = "http"
    payload_format = "senml-json"
    source_system = "minifi"
    source_id = "edge-01"
    idempotency_key = "tenant-a:plot-01:2026-05-06T12:00:00Z:41"
    external_flow_id = "flow-edge-sync"
    external_flow_name = "Edge Telemetry Sync"
    external_flowfile_uuid = "2f8d0c30-7cf6-4d58-b9ac-4e53c9ad3a72"
    external_process_group_id = "pg-plant-01"
    external_processor_id = "proc-http-post"
    external_provenance_uri = "nifi://provenance/events/123456"
    sync_session_id = "sync-2026-05-06-outage-recovery-01"
    edge_sequence = 41
    observed_at = "2026-05-06T12:00:00Z"
    stored_at_edge = "2026-05-06T12:00:05Z"
    sent_at = "2026-05-06T12:10:11Z"
    replay_count = 1
    retry_count = 3
    connectivity_state = "replayed_after_outage"
    payload_hash = "sha256:5ce3b778..."
    metadata = @{
      route = "edge-http->cloud-sync->aioncore"
    }
    payload = @(
      @{
        bn = "pump-07"
        n = "temperature"
        u = "Cel"
        v = 21.4
        t = 1746532800
      }
    )
  } | ConvertTo-Json -Depth 10)
```

Success response shape:

```json
{
  "raw_message_id": "c4c6d8f5-3caa-47f8-9f6c-4fb4d756d522",
  "duplicate": false,
  "idempotency_key": "tenant-a:plot-01:2026-05-06T12:00:00Z:41",
  "observations_created": 1,
  "event_id": "ef2f44cc-31cd-4aa0-b593-9f1eb2a0b1fa",
  "payload_format": "senml-json",
  "source_system": "minifi",
  "sync_session_id": "sync-2026-05-06-outage-recovery-01",
  "received_at": "2026-05-06T12:10:11Z"
}
```

## Example: Replay-Safe Duplicate

Submit the same envelope again with the same tenant and `idempotency_key`:

```powershell
Invoke-RestMethod `
  -Method Post `
  -Uri "http://localhost:8080/ingest/reliable" `
  -ContentType "application/json" `
  -Body $sameEnvelopeJson
```

Duplicate response:

```json
{
  "raw_message_id": "c4c6d8f5-3caa-47f8-9f6c-4fb4d756d522",
  "duplicate": true,
  "idempotency_key": "tenant-a:plot-01:2026-05-06T12:00:00Z:41",
  "observations_created": 0,
  "event_id": null,
  "payload_format": "senml-json",
  "source_system": "minifi",
  "sync_session_id": "sync-2026-05-06-outage-recovery-01",
  "received_at": "2026-05-06T12:10:11Z"
}
```

Behavior:

- no duplicate `RawMessage` is created
- no duplicate `Observation` is created
- the existing `raw_message_id` is returned
- the response uses `200 OK` so replay-safe senders can retry without creating duplicates

## Tenant-Scoped Idempotency

Idempotency lookup is scoped by `tenant_id + idempotency_key`.

That means:

- the same key in the same tenant is treated as a duplicate
- the same key in a different tenant does not collide
- null or absent `idempotency_key` values are not deduplicated

## Provenance Preservation

Reliable ingestion preserves upstream evidence in `RawMessage.headers` and `Event.metadata` using `external.*` keys such as:

- `external.source_system`
- `external.flow_id`
- `external.flow_name`
- `external.flowfile_uuid`
- `external.process_group_id`
- `external.processor_id`
- `external.provenance_uri`
- `external.idempotency_key`
- `external.sync_session_id`
- `external.edge_sequence`
- `external.replay_count`
- `external.retry_count`
- `external.connectivity_state`

These fields are preserved as evidence. They are not treated as trusted proof by themselves.

## NiFi And MiNiFi Example

Recommended NiFi or MiNiFi behavior:

1. keep the original payload intact
2. set a stable tenant-scoped `idempotency_key`
3. preserve FlowFile and processor provenance fields
4. forward to `POST /ingest/reliable`

The current runtime does not execute replay, but it now preserves enough metadata for future replay and DLQ milestones to stay explainable.

## SmartSentinel Store-And-Forward Example

For disconnected field deployments:

- preserve the original sensor `observed_at`
- preserve `stored_at_edge`
- preserve `sent_at`
- set `connectivity_state` such as `offline_buffered` or `replayed_after_outage`
- set `sync_session_id` for outage recovery sessions
- keep the same `idempotency_key` across retries

This lets AionCore distinguish source timing from AionCore arrival time without rewriting `RawMessage.received_at`.

## Token Mode

In `AIONCORE_AUTH_MODE=token`:

- `POST /ingest/reliable` requires `ingestion:write`
- `admin:all` satisfies the scope
- non-admin reliable ingestion belongs to the authenticated principal tenant
- idempotency lookup is performed within that tenant

## Current Limitations

- no batch ingestion API yet
- no backfill session API yet
- no replay execution yet
- no automatic DLQ routing yet
- no connector-aware reliable ingestion endpoint yet
- no flow execution
- existing `POST /ingest/http` behavior remains unchanged
