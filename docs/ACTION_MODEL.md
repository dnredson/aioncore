# Action Model

AionCore supports closed-loop decision support through explicit, auditable concepts. The core flow is:

```text
Observe -> Contextualize -> Decide -> Command -> Act -> Verify
```

The action model is domain-agnostic. It can represent irrigation control, HVAC changes, smart city operations, infrastructure remediation, incident workflows, and other operational domains without changing the core model.

## Observe

Observation starts with raw messages, snapshots, telemetry, events, human reports, or imported records.

AionCore must store raw messages before normalization when data arrives through ingestion. Valid telemetry and relevant snapshot details are then materialized as canonical observations or events.

Examples:

- Soil moisture reading.
- Room CO2 measurement.
- Waste container fill-level report.
- SmartSentinel service-health snapshot.
- Deployment failure event.

## Contextualize

Contextualization links observations and events to JSON-LD entities and relationships.

The context graph answers questions such as:

- What entity produced this signal?
- What feature of interest is affected?
- What system, place, asset, or dependency does it belong to?
- What capabilities exist for this target?
- What policies constrain possible actions?

## Decide

Decision logic determines whether a command should be proposed, requested, or rejected.

Decision sources can include:

- Human operators.
- Deterministic application logic.
- Policy checks.
- External systems.
- AI or LLM-assisted decision support.

MVP policy: AI-facing tools are read-oriented by default. LLMs must not directly execute critical actions without explicit future authorization and approval design.

## Command

A Command records requested intent. It does not prove execution.

Command examples:

- Open valve V1 for 10 minutes.
- Set room 101 temperature setpoint to 22 C.
- Dim lighting circuit C to 70%.
- Restart service S.
- Open incident for a degraded dependency.

Commands should include:

- Target entity.
- Requested capability.
- Parameters.
- Requester.
- Decision reference.
- Policy evaluation result.
- Status.
- Expiration or schedule when applicable.

## Act

An Action records an execution attempt. Actions are performed by executors such as devices, gateways, automation services, operational tools, or optional integrations like SmartSentinel.

Action examples:

- MQTT command sent to a controller.
- Building automation API called.
- Smart city lighting API called.
- SmartSentinel remediation workflow triggered.
- Incident ticket created.

Actions should include:

- Command reference.
- Executor entity or integration.
- Capability used.
- Started timestamp.
- Status.
- External correlation ID.
- Request payload or reference.

## Verify

Verification determines whether the intended outcome occurred.

Verification can use:

- Follow-up observations.
- Action results.
- External system acknowledgements.
- Events.
- Human confirmation.

Examples:

- Valve state observation reports open.
- Room temperature begins trending toward the setpoint.
- Lighting controller acknowledges dimming.
- SmartSentinel reports remediation success.
- Error rate drops after restart.

## Safety Boundaries

Critical actions require explicit policy and authorization design.

Critical actions include:

- Physical actuation.
- Production service remediation.
- Credential or access changes.
- Deletion of data.
- Rule or policy changes.
- Expensive or irreversible operations.

The default MCP/AI-facing surface should remain read-only until write and action paths have approval, audit, and policy controls.

## Persistence Guidance

For MVP sequencing, action records can be introduced after the entity, relationship, raw message, and observation foundations are stable.

Recommended future tables:

- `events`
- `capabilities`
- `policies`
- `commands`
- `actions`
- `action_results`

These tables should reference entities, raw messages, observations, external correlation IDs, and policy evaluations where applicable.
