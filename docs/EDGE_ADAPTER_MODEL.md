# Aion Edge Adapter Model

The Aion Edge Adapter is a future optional edge/fog component for collecting telemetry from local protocols and brokers, normalizing payloads near the source, buffering when disconnected, and publishing to AionCore.

It is not required by the AionCore runtime. Existing AionCore server-side ingestion paths, including HTTP ingestion, MQTT ingestion, connector-aware HTTP ingestion, and dynamic MQTT connector workers, remain valid.

## Responsibilities

### AionCore

AionCore owns the semantic platform boundary:

- JSON-LD entities and relationships.
- IngestionConnector and ConnectorProfile records.
- RawMessage preservation before normalization.
- Payload decoding for supported server-side formats.
- Canonical Observations.
- Events, Commands, Actions, ActionResults, policies, and executor lifecycle.
- AI/MCP-ready context assembly.
- Durable persistence through pluggable storage backends.

AionCore should continue to accept data directly from devices, server-side connectors, gateways, and future edge adapters.

### Aion Edge Adapter

The adapter owns local collection and transport adaptation:

- Subscribe to or read from local MQTT brokers, HTTP sources, CoAP endpoints, serial buses, SDI-12 sensors, files, CSV drops, and protocol-specific gateways.
- Parse source-specific payloads such as UltraLight, TTN JSON, ChirpStack JSON, SDI-12 responses, CSV rows, serial-like sensor frames, SenML, and future plugin formats.
- Normalize data toward AionCore-compatible output formats.
- Buffer messages locally when AionCore is unreachable.
- Move invalid or exhausted messages to a local dead-letter queue.
- Publish to AionCore or compatible upstream systems.
- Preserve local provenance such as adapter ID, parser name, source protocol, source topic/path, connector ID, and replay status.

The adapter may be implemented in Rust, Python, or another deployment-specific language later. This milestone does not import or integrate the existing Python prototype.

## Why Optional

Many deployments do not need an edge adapter. A single AionCore API process can already receive HTTP payloads and run server-side MQTT workers for straightforward deployments.

The adapter becomes useful when a site needs:

- Local protocol support that should not run inside AionCore.
- Collection from serial, SDI-12, CoAP, local files, field gateways, or broker-specific payloads.
- Local buffering during WAN outages.
- Site-specific parsing or calibration logic.
- One process close to sensors and brokers, with AionCore running centrally.
- Multiple isolated collectors, one per facility, fog node, broker, or protocol group.

Keeping the adapter optional prevents AionCore from becoming coupled to one edge runtime, one broker topology, or one field protocol stack.

## Difference From Server-Side IngestionConnectors

Server-side IngestionConnectors are AionCore runtime records. They describe sources that AionCore itself can ingest from or validate. Dynamic MQTT workers can run inside the AionCore process when enabled.

The Aion Edge Adapter is an external component. It may use AionCore connector-aware ingestion endpoints, but it is not the same thing as an IngestionConnector worker.

Key differences:

- Server-side connectors run in or are planned by AionCore.
- Edge adapters run outside AionCore, usually near sensors or local brokers.
- Server-side connectors use AionCore-managed connector configuration and connector secrets.
- Edge adapters use local deployment configuration and local secret handling.
- Server-side connectors preserve raw messages at AionCore ingress.
- Edge adapters may preserve local raw input in a local buffer or DLQ, then publish normalized or semi-normalized payloads to AionCore, where AionCore still stores the inbound RawMessage.

Both models can coexist. A deployment can run no adapters, one adapter, or many adapters.

## Deployment Topology

Multiple adapters may run independently:

- One adapter per farm, building, plant, or fog node.
- One adapter per local MQTT broker.
- One adapter per protocol group, such as serial/SDI-12, CoAP, or CSV file drops.
- One adapter per security zone or network segment.

Each adapter should have a stable `adapter_id`. Future milestones may register adapters as AionCore Entities, ExecutorAgents, or both:

- Entity registration can represent the adapter as a deployed component in the semantic graph.
- ExecutorAgent registration can support future command polling for adapter-managed operational tasks, subject to the existing policy and lease model.

## Supported Source Types

The adapter architecture should allow plugins for:

- MQTT.
- HTTP pull or push.
- CoAP.
- Serial ports.
- SDI-12.
- CSV files or streams.
- UltraLight payloads.
- SenML JSON.
- TTN JSON.
- ChirpStack JSON.
- Vendor-specific JSON, text, binary, or line protocols.
- Future custom parsers.

AionCore should not need native readers for every edge protocol. The adapter can translate local protocols into stable AionCore ingestion formats.

## Output Modes

The adapter should support these output modes.

### senml-json

Use SenML JSON when the adapter can represent measurements as standard SenML records and publish them to AionCore with `payload_format = "senml-json"`.

This is useful for compatibility with Magistrala-style telemetry and other systems that already understand SenML.

### canonical-json

Use canonical JSON when the adapter can directly produce AionCore canonical observation input. This is useful when parsing and normalization are done at the edge and the adapter can include observed property, value type, unit, timestamps, and metadata explicitly.

### aion-observation-batch

`aion-observation-batch` is a future output format for efficient batches of canonical observation candidates. It should be designed later and should preserve the same core requirements:

- AionCore stores the inbound RawMessage first.
- Each observation links back to the raw message where possible.
- Adapter metadata is preserved.
- Invalid records can fail individually when the format supports partial acceptance.

This milestone defines the name only. It does not implement the format.

## Plugin Contract

Adapter plugins should be small, composable units. A conceptual plugin contract includes:

- Input protocol/source: where bytes or messages come from, such as MQTT topic, serial port, file path, CoAP endpoint, or HTTP endpoint.
- Parser: converts protocol payloads into structured measurements or parse errors.
- Normalizer: maps parsed values into AionCore-compatible measurements with entity references, observed properties, units, timestamps, and metadata.
- Output encoder: emits `senml-json`, `canonical-json`, or future `aion-observation-batch`.
- Publisher: sends encoded output to AionCore HTTP ingestion, AionCore MQTT ingestion, or compatibility targets such as Magistrala.
- DLQ strategy: classifies failures and decides whether to retry, dead-letter, drop, or quarantine.

Plugins should not embed secrets in code or example files. They should receive credentials from local secret sources or runtime configuration.

## DLQ And Offline Buffering

The adapter should distinguish between temporary delivery failure and permanent parse or validation failure.

Recommended local queues:

- Pending queue: accepted local messages waiting to be published.
- Retry queue: publish attempts that failed due to transient conditions.
- DLQ: messages that cannot be parsed, normalized, validated, or delivered after retry exhaustion.

DLQ records should include safe metadata:

- `adapter_id`
- local source identifier
- source protocol
- parser name
- payload format hint
- failure class
- failure reason
- attempt count
- first seen timestamp
- last attempted timestamp
- next retry timestamp when applicable
- replay eligibility

DLQ records may include raw payload only if local deployment policy allows it. Raw payloads can contain sensitive operational data and must be protected locally.

AionCore should not implement the adapter DLQ in this milestone. When replayed messages are eventually published to AionCore, adapter metadata should set `dlq_replayed = true`.

## Retries And Backoff

The adapter should use bounded retry policies:

- Immediate retry for short transient failures only when safe.
- Exponential backoff with jitter for network, broker, or upstream availability failures.
- Maximum attempt count or maximum age for each message.
- Circuit-breaker behavior when AionCore or the upstream broker is unavailable.
- Clear transition to DLQ for exhausted messages.

Retry metadata should be local by default. Published messages can include a compact replay marker, such as `dlq_replayed`, `attempt_count`, or `first_seen_at`, when useful for audit.

## Credential Handling

Adapter credentials and keys must be handled locally and safely:

- Do not store secrets in the repository.
- Do not copy secrets from adapter prototypes into AionCore.
- Prefer environment variables, local secret files with restricted permissions, platform secret stores, or deployment secret managers.
- Do not log passwords, API tokens, private keys, or full connection strings containing credentials.
- Keep per-source credentials separate from AionCore publish credentials.
- Rotate credentials without requiring payload parser changes.
- Redact credentials in status, heartbeat, DLQ, and error output.

AionCore connector secrets remain server-side AionCore records. Edge adapter secrets are local adapter deployment concerns unless a future explicit adapter registration feature is added.

## Publishing To AionCore

The adapter can publish through several modes.

### Connector-Aware HTTP Ingestion

Preferred when AionCore has a connector record representing the adapter or source:

```text
POST /ingestion/connectors/{connector_id}/ingest
```

The adapter should include metadata such as:

- `adapter_id`
- `connector_id`
- `source_protocol`
- `source_ref`
- `parser_name`
- `output_mode`
- `dlq_replayed`

### AionCore MQTT Ingestion

Useful when the adapter publishes to a broker and AionCore subscribes:

```text
aioncore/{producer_entity_id}/{feature_of_interest_id}/data
```

The payload format should match the configured AionCore MQTT decoder, such as `senml-json` or `canonical-json`.

### Magistrala/SenML Compatibility Mode

When a deployment also needs Magistrala-compatible telemetry, the adapter can emit SenML JSON and publish it to both AionCore and a Magistrala-compatible broker or HTTP endpoint. This should remain a compatibility mode, not a required AionCore dependency.

## AionCore Metadata Preservation

Adapter-published messages should preserve metadata that helps explain origin and replay behavior:

- `adapter_id`
- `connector_id`
- `source_protocol`
- `source_ref`
- `parser_name`
- `dlq_replayed`
- `output_mode`
- optional local site or node identifiers

AionCore should preserve this metadata in RawMessages and ingestion Events. Observations may also carry relevant adapter metadata when the decoder or request shape includes it.

Future adapter heartbeats may be represented as Observations or Events:

- Observation: adapter queue depth, last successful publish age, local broker connectivity, DLQ count.
- Event: adapter started, adapter stopped, upstream unavailable, DLQ replay started, DLQ replay completed.

## Non-Goals

- No adapter runtime is implemented in this milestone.
- No Python adapter prototype code is imported.
- No adapter secrets, tokens, passwords, keys, or real configs are added.
- No AionCore API behavior changes are required.
- No AionCore DLQ implementation is added.
- No serial, SDI-12, CoAP, CSV, or ChirpStack reader is implemented in AionCore.
- No dashboard, Cassandra adapter, production MCP transport, or external AI integration is added.

## Example Artifacts

Example non-secret payload artifacts are available under:

```text
docs/examples/edge-adapter/
```

They show possible adapter output and DLQ record shapes only. They are not runtime configuration files.
