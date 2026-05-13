# NiFi/MiNiFi Usage

This guide shows how to use the Milestone 83 NiFi/MiNiFi integration contract without introducing a NiFi dependency into AionCore.

## What This Milestone Enables

- a documented boundary between AionCore and NiFi/MiNiFi
- a recommended envelope shape for reliable upstream producers
- consistent provenance metadata keys for future replay, DLQ, and backfill work
- a DLQ API foundation for trusted integrations to preserve failed or quarantined records explicitly
- a batch/backfill ingestion runtime at `POST /ingest/batch` for reconnect catch-up submissions
- explicit deployment guidance for edge, fog, and cloud flow topologies

This milestone does not add:

- a NiFi client
- a NiFi runtime dependency
- flow execution
- DLQ runtime behavior
- replay execution

## Recommended Producer Behavior

An external NiFi or MiNiFi flow should:

1. preserve the original payload content
2. generate a tenant-scoped `idempotency_key` when possible
3. preserve external runtime identifiers such as `external_flowfile_uuid`
4. send timing fields that distinguish observation, buffering, and send time
5. forward payloads to `POST /ingest/reliable` when replay-safe deduplication is desired
6. use `POST /ingest/batch` when reconnecting with many buffered envelopes

## Suggested Ingestion Targets

Depending on deployment shape, NiFi or MiNiFi can target:

- `POST /ingest/http`
- `POST /ingest/reliable`
- `POST /ingest/batch`
- `POST /ingestion/connectors/{connector_id}/ingest`
- a broker path that AionCore MQTT ingestion already consumes

The preferred current paths for provenance-rich reliable ingestion are:

- `POST /ingest/reliable` for one envelope
- `POST /ingest/batch` for backfill or reconnect batches

## Recommended Envelope

Suggested envelope fields are defined in [NiFi Integration Model](NIFI_INTEGRATION_MODEL.md).

Minimum practical fields:

```json
{
  "source_system": "nifi",
  "payload_format": "canonical-json",
  "payload": {
    "producer_entity_id": "11111111-1111-1111-1111-111111111111",
    "feature_of_interest_id": "22222222-2222-2222-2222-222222222222",
    "observations": [
      {
        "observed_property": "temperature",
        "value": 21.4,
        "unit": "Cel",
        "observed_at": "2026-05-06T12:00:00Z"
      }
    ]
  }
}
```

More complete reliable-ingestion example:

```json
{
  "source_system": "nifi",
  "external_flow_id": "cloud-route-01",
  "external_flow_name": "Cloud Route 01",
  "external_process_group_id": "pg-cloud-ingest",
  "external_processor_id": "proc-route-http",
  "external_flowfile_uuid": "8fc2778d-66d3-4f0a-89b2-e497cb7e7387",
  "idempotency_key": "tenant-a:well-03:2026-05-06T12:00:00Z:seq-1884",
  "source_id": "edge-site-03",
  "edge_sequence": 1884,
  "sync_session_id": "backfill-2026-05-06-plant-03",
  "observed_at": "2026-05-06T12:00:00Z",
  "stored_at_edge": "2026-05-06T12:00:03Z",
  "sent_at": "2026-05-06T12:08:40Z",
  "payload_format": "senml-json",
  "payload": [
    {
      "bn": "well-03",
      "n": "pressure",
      "u": "bar",
      "v": 3.2,
      "t": 1746532800
    }
  ],
  "payload_hash": "sha256:42d6d3f6...",
  "replay_count": 1,
  "retry_count": 2,
  "connectivity_state": "replayed_after_outage",
  "provenance": {
    "provenance_uri": "nifi://provenance/events/98122"
  }
}
```

## Metadata Preservation Guidance

When implementing an upstream adapter later, preserve these keys consistently:

- `external.source_system`
- `external.flow_id`
- `external.flow_name`
- `external.flowfile_uuid`
- `external.process_group_id`
- `external.processor_id`
- `external.provenance_uri`
- `external.replay_count`
- `external.retry_count`
- `external.idempotency_key`
- `external.sync_session_id`

Recommended storage targets:

- raw ingress context in `RawMessage.headers`
- ingestion and audit outcomes in `Event.metadata`
- delayed or replay-sensitive semantics in `Observation.metadata`
- external graph references in `Flow.metadata`

## Authentication Direction

NiFi or MiNiFi should authenticate to AionCore with API tokens today and future service credentials later.

Useful present or planned scopes:

- `ingestion:write`
- `flows:read`
- `dashboard:read`
- `dlq:write`
- `dlq:read`
- `batches:write`

Do not use broad operator credentials for automated upstream flow engines.

## Replay And Backfill Guidance

When replaying historical data:

- preserve original `observed_at`
- preserve AionCore arrival time as `RawMessage.received_at`
- include `sync_session_id`
- increment `replay_count`
- keep `idempotency_key` stable for the semantic record being retried or replayed

This makes future backfill and replay features easier to explain and audit.

## Current Limitations

- AionCore now supports the generic reliable envelope at `POST /ingest/reliable`.
- AionCore now supports multi-item reliable backfill batches at `POST /ingest/batch`.
- AionCore now enforces tenant-scoped idempotency-key deduplication for reliable HTTP ingestion when `idempotency_key` is present.
- AionCore now supports batch-level provenance inheritance and per-item duplicate reporting.
- AionCore now provides explicit DLQ record APIs, but it does not route records into DLQ automatically and does not execute replay.
- AionCore does not yet implement a persistent batch session table.
- AionCore does not yet distinguish late data in rule execution behavior.
- Connector-aware reliable ingestion is not implemented yet.

Use this guide as a contract for future-compatible producer design rather than a runtime feature checklist.


## Sync Sessions

AionCore sync sessions provide an optional tenant-scoped tracking record for reconnect/backfill windows. NiFi, MiNiFi, SmartSentinel, or an edge adapter should reuse a stable `sync_session_id` across all batches belonging to the same outage/reconnect episode.
