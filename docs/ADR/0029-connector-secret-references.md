# ADR 0029: Connector Secret References

## Status

Accepted

## Context

Dynamic MQTT connector workers need broker credentials for TTN v3, public brokers, cloud Mosquitto, EMQX, and similar deployments. Raw passwords must not be stored directly in `IngestionConnector` records or exposed in API responses, logs, readiness, worker status, events, or raw-message metadata.

This milestone needs a local-development-friendly foundation without implementing a production secret manager, encryption, KMS, Vault, TLS/mTLS, AionCore user authentication, or per-device MQTT authorization.

## Decision

Add tenant-scoped connector secrets with:

- `id`
- `tenant_id`
- `secret_key`
- `secret_type`: `mqtt_basic_auth`, `token`, `api_key`, or `custom`
- optional `username`
- write-only `secret_value`
- optional metadata
- timestamps

Add API endpoints:

```text
POST /secrets/connectors
GET /secrets/connectors
GET /secrets/connectors/{secret_id}
DELETE /secrets/connectors/{secret_id}
```

API responses omit `secret_value`. Events for secret create/delete include only non-secret metadata.

Add `secret_ref_id` to `IngestionConnector`. Connector create/update accepts the reference ID and connector responses expose only the ID. Dynamic MQTT connector workers resolve `mqtt_basic_auth` secrets internally and apply username/password credentials to the MQTT client. The existing environment-variable MQTT worker remains unchanged.

Persist connector secrets in memory and PostgreSQL. PostgreSQL migration `0008_create_connector_secrets.sql` creates `connector_secrets` and adds `ingestion_connectors.secret_ref_id`.

## Consequences

Dynamic MQTT workers can authenticate to external brokers without putting raw credentials into connector records.

The model is intentionally not a production secret-management solution. Stored secret values are still present in the selected storage backend. Encryption, KMS, Vault integration, rotation, TLS/mTLS, user/device authentication, per-device MQTT authorization, and TTN uplink decoding remain future work.
