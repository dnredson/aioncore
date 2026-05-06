# Batch Ingestion Usage

This guide covers Milestone 86: the batch and backfill reliable ingestion API foundation.

For the model background, also see [Ingestion Model](INGESTION_MODEL.md), [Reliable Ingestion Usage](RELIABLE_INGESTION_USAGE.md), [NiFi Integration Model](NIFI_INTEGRATION_MODEL.md), [DLQ Model](DLQ_MODEL.md), and [Security Model](SECURITY_MODEL.md).

## Endpoint

```text
POST /ingest/batch
```

This endpoint is additive. Existing `POST /ingest/http` and `POST /ingest/reliable` behavior is unchanged.

## Request Shape

The batch request wraps multiple reliable-ingestion items.

Batch-level fields:

- `batch_id`
- `sync_session_id`
- `source_system`
- `source_id`
- `connectivity_state`
- `continue_on_error` default `true`
- `external_flow_id`
- `external_flow_name`
- `metadata`
- `items`

Each item currently includes:

- `producer_entity_id`
- `feature_of_interest_id`
- optional `protocol`
- optional `content_type`
- optional `mapping`
- the same reliable-envelope fields used by `POST /ingest/reliable`

Example:

```json
{
  "batch_id": "sync-plant-01-2026-05-06-01",
  "sync_session_id": "sync-plant-01-2026-05-06-01",
  "source_system": "smartsentinel",
  "source_id": "plant-01-edge-01",
  "connectivity_state": "reconnected_backfill",
  "continue_on_error": true,
  "external_flow_id": "edge-backfill-sync",
  "external_flow_name": "Edge Backfill Sync",
  "metadata": {
    "route": "edge-http->cloud-sync->aioncore"
  },
  "items": [
    {
      "producer_entity_id": "11111111-1111-1111-1111-111111111111",
      "feature_of_interest_id": "22222222-2222-2222-2222-222222222222",
      "payload_format": "senml-json",
      "idempotency_key": "tenant-a:plot-01:2026-05-06T12:00:00Z:41",
      "observed_at": "2026-05-06T12:00:00Z",
      "stored_at_edge": "2026-05-06T12:00:05Z",
      "sent_at": "2026-05-06T12:10:11Z",
      "retry_count": 0,
      "replay_count": 0,
      "payload": [
        {
          "n": "soil_moisture",
          "u": "%",
          "v": 18.5
        }
      ]
    },
    {
      "producer_entity_id": "11111111-1111-1111-1111-111111111111",
      "feature_of_interest_id": "22222222-2222-2222-2222-222222222222",
      "payload_format": "senml-json",
      "idempotency_key": "tenant-a:plot-01:2026-05-06T12:05:00Z:42",
      "payload": [
        {
          "n": "soil_moisture",
          "u": "%",
          "v": 19.0
        }
      ]
    }
  ]
}
```

## Response Shape

Success responses use `200 OK` and return per-item outcomes:

```json
{
  "batch_id": "sync-plant-01-2026-05-06-01",
  "sync_session_id": "sync-plant-01-2026-05-06-01",
  "source_system": "smartsentinel",
  "received_at": "2026-05-06T12:10:12Z",
  "total_items": 2,
  "accepted_count": 2,
  "duplicate_count": 0,
  "failed_count": 0,
  "observations_created": 2,
  "stopped_early": false,
  "results": [
    {
      "index": 0,
      "status": "accepted",
      "duplicate": false,
      "idempotency_key": "tenant-a:plot-01:2026-05-06T12:00:00Z:41",
      "raw_message_id": "c4c6d8f5-3caa-47f8-9f6c-4fb4d756d522",
      "observations_created": 1,
      "error": null
    }
  ],
  "event_id": "ef2f44cc-31cd-4aa0-b593-9f1eb2a0b1fa"
}
```

## Idempotency Behavior

- idempotency lookup is scoped by `tenant_id + idempotency_key`
- absent `idempotency_key` means normal ingestion without deduplication
- duplicates return `duplicate = true`, `observations_created = 0`, and the existing `raw_message_id`
- repeated keys inside the same batch are safe because items are processed sequentially
- the same key in different tenants does not collide

## Continue-On-Error Behavior

Default:

- `continue_on_error = true`
- one failed item does not stop later items

Optional strict mode:

- `continue_on_error = false`
- processing stops after the first failed item
- already processed item results remain in the response
- `stopped_early = true`

There is no global transaction for the full batch.

## Batch Limits

- empty `items` is rejected with `400 Bad Request`
- batches above `1000` items are rejected with `400 Bad Request`

## Provenance Inheritance

When item-level fields are absent, AionCore inherits these batch-level values into each item:

- `source_system`
- `source_id`
- `sync_session_id`
- `connectivity_state`
- `external_flow_id`
- `external_flow_name`
- `metadata`

If both batch and item `metadata` are objects, item keys override matching batch keys and batch-only keys are preserved.

Batch identifiers are preserved in `RawMessage.headers` and ingestion `Event.metadata` as audit evidence. External provenance remains evidence, not trusted proof.

## SmartSentinel Store-And-Forward Example

Recommended pattern:

1. buffer envelopes locally while connectivity is down
2. preserve `observed_at`, `stored_at_edge`, `sent_at`, `sync_session_id`, and stable `idempotency_key`
3. submit the backlog through `POST /ingest/batch` after reconnection
4. retry safely when needed and rely on duplicate responses for already-accepted items

## NiFi And MiNiFi Backfill Example

Recommended pattern:

1. keep each original payload intact inside its item envelope
2. preserve NiFi or MiNiFi provenance fields and a stable tenant-scoped `idempotency_key`
3. group buffered envelopes into bounded reconnect batches
4. use `continue_on_error=true` for operational catch-up unless an upstream controller requires stop-on-first-failure semantics

## Token Mode

In `AIONCORE_AUTH_MODE=token`:

- `POST /ingest/batch` requires `batches:write`
- `admin:all` satisfies the scope
- non-admin batch ingestion belongs to the authenticated principal tenant
- idempotency lookup remains tenant-scoped

## Current Limitations

- no persistent batch session table yet
- no replay execution
- no automatic DLQ routing
- no flow execution
- no connector-aware reliable batch endpoint
- no broker subscription or outbound publish behavior in this milestone
