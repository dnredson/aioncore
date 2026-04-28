# ADR 0021: MQTT Ingestion Foundation

## Status

Accepted

## Context

AionCore needs a lightweight MQTT ingestion path for local runtime development without changing the HTTP ingestion flow or requiring a broker for normal tests.

The platform already stores raw messages first and then normalizes valid telemetry into canonical observations. MQTT should reuse that model rather than inventing a separate path.

## Decision

Implement MQTT ingestion as an opt-in local-runtime worker.

- MQTT is disabled by default.
- When enabled, the API starts an MQTT subscriber alongside the HTTP server.
- The worker subscribes to a simple MVP topic convention: `aioncore/{producer_entity_id}/{feature_of_interest_id}/data`.
- The worker stores the raw message first, then decodes supported payloads, then creates canonical observations.
- Supported payload formats in this milestone are `senml-json`, `ultralight`, and `canonical-json`.
- UltraLight MQTT ingestion uses stored payload-profile mappings when available.
- MQTT authentication, broker scaling, and production transport hardening are deferred.

## Consequences

- Local developers can validate MQTT ingestion without changing HTTP behavior.
- The runtime fails clearly if MQTT is enabled but the broker cannot be reached.
- The ingestion model stays aligned with the existing raw-message-first design.
- MQTT remains a local-development foundation, not a production broker integration.

