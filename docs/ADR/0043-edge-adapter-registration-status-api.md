# ADR 0043: Edge Adapter Registration And Status API

## Status

Accepted

## Context

Milestone 42 documented the Aion Edge Adapter as an optional edge/fog architecture. Milestone 46 adds a lightweight AionCore-facing contract so future adapters can register themselves, publish heartbeat/status, and report DLQ/offline-buffer state without becoming part of the AionCore runtime.

AionCore already has JSON-LD entities, events, raw message preservation, pluggable storage, and server-side connector workflows. It needs an optional contract for future adapters that is explicit, low-coupling, and safe to ignore when not deployed.

## Decision

Add the following optional endpoints:

- `POST /adapters`
- `GET /adapters`
- `GET /adapters/{adapter_id}`
- `PUT /adapters/{adapter_id}/heartbeat`
- `GET /adapters/{adapter_id}/status`

The adapter model includes:

- `adapter_key`
- `adapter_type`
- optional display name, version, host, site, environment, and metadata
- current status and `last_seen_at`
- status reports that can include uptime, active connectors, active plugins, DLQ depth, DLQ oldest record age, last successful and failed publish timestamps, and an error string

Registration reuses an existing adapter when the same tenant and `adapter_key` already exist. Heartbeat updates `last_seen_at`, status, and the latest stored status record.

When an adapter registers, AionCore may materialize or update an Entity with `entity_type = aion:EdgeAdapter` and a key derived from `adapter_key`. AionCore also emits lifecycle events for registration, heartbeat, and status changes.

The contract is informational and operationally useful, but it does not implement any runtime collection, buffering, or credential handling inside AionCore.

## Consequences

- Future adapters have a stable registration and status contract.
- AionCore can represent adapters as semantic entities without tightly coupling to their runtime.
- Heartbeats and DLQ state can be audited through Events and status records.
- Existing server-side ingestion remains unchanged.
- Authentication is intentionally deferred and must be added before production use.

## Non-Goals

- No edge adapter runtime.
- No Python prototype import.
- No serial, SDI-12, CoAP, CSV, or DLQ runtime.
- No authentication.
- No dashboard.
- No Cassandra adapter.
- No production MCP transport.
- No external AI calls.
- No secret material in event metadata.
