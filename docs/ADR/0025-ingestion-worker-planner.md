# ADR 0025: Ingestion Worker Planner

## Status

Accepted.

## Context

AionCore now has configurable ingestion connectors and PostgreSQL persistence for connector records. Before adding dynamic MQTT workers per connector, the runtime needs a safe way to inspect enabled connectors and understand which workers would be started.

The planner must not change existing runtime behavior. The environment-variable MQTT worker remains the only MQTT runtime path, and MQTT remains disabled by default.

## Decision

Add a read-only ingestion worker planner exposed through:

```text
GET /ingestion/workers/plan
```

The planner reads all ingestion connectors and returns intended worker specs. It does not start workers, open network connections, subscribe to brokers, or call external services.

Planner behavior:

- disabled connectors are included as `skipped`
- enabled HTTP connectors produce `http_listener` specs
- enabled MQTT connectors produce `mqtt_subscriber` specs
- missing MQTT `broker_url` or `topic_filter` makes the spec `invalid`
- missing HTTP `http_path` or `endpoint` makes the spec `invalid`
- `ttn-v3` connectors produce MQTT subscriber specs and include a limitation note when the payload format is not implemented by the current decoder path
- `future` connector types produce `unsupported` specs

`GET /ready` includes a cheap worker plan summary with planned, invalid, and unsupported counts. These counts do not affect readiness in this milestone because the planner is not responsible for running workers.

## Consequences

Operators and tests can validate connector configuration before dynamic worker orchestration exists.

The planner creates a stable bridge between persisted connector configuration and future runtime worker startup. Dynamic MQTT workers, TTN uplink decoding, secrets storage, connector authentication, TLS/mTLS, dashboard work, Cassandra, production MCP transport, and SmartSentinel integration remain out of scope.
