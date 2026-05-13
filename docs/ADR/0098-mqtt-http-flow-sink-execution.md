# ADR 0098: MQTT/HTTP Flow Sink Execution

## Status

Accepted.

## Context

AionCore has progressively introduced flow execution in safe stages. Earlier milestones added preview-only execution, side-effect authorization, and safe internal side effects for observation and event creation. MQTT publish and HTTP forward are the first external side effects and therefore require stronger constraints than preview, validation, or internal persistence.

## Decision

AionCore now supports explicitly authorized MQTT publish and HTTP forward execution from flow sink nodes, while preserving preview-first behavior.

Real external sink execution is only attempted when:

- the execution request declares side-effect intent;
- token mode authorization satisfies `flows:execute` or `admin:all`;
- the request explicitly lists the action in `requested_sink_actions`;
- the sink references a tenant-owned enabled connector through `config.connector_id`;
- the connector type matches the sink kind.

MQTT publish is limited to MQTT connectors, `mqtt://` broker URLs, optional `mqtt_basic_auth` connector secrets, explicit non-wildcard topics, QoS, retain flag, and bounded publish attempts. TTN v3 connectors are excluded because they are modeled as subscriber/uplink connectors.

HTTP forward is limited to enabled HTTP connectors, `http://` endpoints, and `POST`, `PUT`, or `PATCH`. HTTPS, custom headers, and secret-backed HTTP credentials remain future work.

## Consequences

This adds the first external flow side effects without enabling automatic flow runtime execution. Operators must still call the execution API explicitly and request the relevant sink actions. Flow enablement alone does not execute flows.

Responses continue to include sink previews and redacted endpoint/connector metadata. Secret values are not returned. MQTT/HTTP side-effect events are recorded without payload bodies or credentials.

## Non-goals

- broker subscriptions from flows;
- automatic enabled-flow runtime execution;
- command creation;
- DLQ writes;
- HTTPS or custom HTTP headers;
- arbitrary direct network destinations without connector references;
- executing TTN downlinks.
