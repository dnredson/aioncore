# ADR 0042: Aion Edge Adapter Architecture

## Status

Accepted

## Context

AionCore already supports payload-agnostic ingestion, RawMessage preservation, HTTP ingestion, MQTT ingestion, connector-aware HTTP ingestion, dynamic MQTT connector workers, payload decoders, canonical Observations, TTN profile mapping, and optional SmartSentinel integration.

Some deployments also need a component that runs close to sensors, local brokers, serial buses, or fog networks. An existing Python adapter prototype explores MQTT reads, UltraLight, TTN and ChirpStack JSON, SDI-12, CSV, serial-like sensor formats, conversion toward SenML or Magistrala-compatible telemetry, local DLQ/offline retry concepts, entity registry handling, and publishing.

AionCore needs an architecture model for this future adapter without importing prototype code, copying secrets, or making the adapter a required runtime dependency.

## Decision

Document the Aion Edge Adapter as an optional edge/fog component. The adapter may collect from MQTT, HTTP, CoAP, serial, SDI-12, CSV, UltraLight, TTN JSON, ChirpStack JSON, and future plugins. It can normalize data near the source, buffer offline messages, manage a local DLQ, and publish to AionCore.

The adapter output modes are:

- `senml-json`
- `canonical-json`
- `aion-observation-batch` as a future format name only

The conceptual plugin contract includes:

- input protocol/source
- parser
- normalizer
- output encoder
- publisher
- DLQ strategy

The adapter can publish to:

- AionCore connector-aware HTTP ingestion
- AionCore MQTT ingestion
- Magistrala/SenML compatibility mode when needed

Adapter metadata should include fields such as `adapter_id`, `connector_id`, `source_protocol`, `parser_name`, and `dlq_replayed`. AionCore should preserve inbound adapter metadata in RawMessages and ingestion Events when published through existing ingestion paths.

Future milestones may register adapters as Entities or ExecutorAgents and may publish adapter heartbeat/status as Observations or Events.

## Consequences

- AionCore remains usable without any edge adapter.
- Existing server-side IngestionConnectors remain valid.
- Edge/fog deployments have a documented path for local protocol adaptation, offline buffering, and DLQ behavior.
- The existing Python prototype can inform future work without becoming part of this milestone.
- AionCore does not take on serial, SDI-12, CoAP, CSV, or ChirpStack runtime responsibilities in this milestone.
- No runtime behavior changes are required.

## Non-Goals

- No adapter runtime implementation.
- No Python adapter code import.
- No copied secrets, tokens, passwords, keys, or real configs.
- No AionCore API behavior changes.
- No AionCore DLQ implementation.
- No serial, SDI-12, CoAP, CSV, or ChirpStack readers in AionCore.
- No dashboard.
- No Cassandra adapter.
- No production MCP transport.
- No external AI calls.
