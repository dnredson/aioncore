# ADR 0028: Connector Update And MQTT Reconnect Backoff

## Status

Accepted

## Context

Runtime connector worker reconciliation can start, stop, and restart dynamic MQTT workers, but external callers did not have a general connector update API to trigger configuration changes. Connector MQTT workers also became degraded permanently after broker disconnects or event-loop failures.

Connector workers must remain opt-in through `AIONCORE_CONNECTOR_WORKERS_ENABLED=false` by default. The existing environment-variable MQTT worker must keep its current behavior. TTN decoding, secrets storage, TLS/mTLS, and per-device MQTT authorization remain out of scope.

## Decision

Add:

```text
PATCH /ingestion/connectors/{connector_id}
```

The endpoint updates operational connector fields: display name, enabled state, protocol, endpoint, broker URL, client ID, topic filter, HTTP path, payload format, content type, default entity IDs, and metadata.

Immutable connector identity fields are not accepted by the update request: `id`, `tenant_id`, `connector_key`, `connector_type`, and `connector_profile`.

After a connector update, the API runs worker reconciliation. If dynamic workers are enabled, relevant MQTT config changes restart the worker. Disabling a connector stops the worker. Invalid updated config leaves the worker stopped or invalid with validation details.

Connector-based MQTT workers now retry broker disconnects and event-loop failures with bounded exponential backoff from 1 second up to 60 seconds. Worker status includes reconnect attempts, disconnect/reconnect timestamps, next reconnect time, and last error. Successful resubscription returns status to `running`.

New events:

- `aion:IngestionConnectorUpdated`
- `aion:ConnectorWorkerDisconnected`
- `aion:ConnectorWorkerReconnectScheduled`
- `aion:ConnectorWorkerReconnected`

## Consequences

Operators can update connector runtime configuration without restarting `aion-api`.

Dynamic connector workers are more resilient to temporary broker outages while preserving the default safe behavior: no connector workers start unless explicitly enabled.

The environment-variable MQTT worker remains unchanged. TTN v3 connectors remain skipped until TTN uplink decoding is implemented. Secrets storage, TLS/mTLS, and per-device MQTT authorization remain future work.
