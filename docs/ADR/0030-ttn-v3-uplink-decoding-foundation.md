# ADR 0030: TTN v3 Uplink Decoding Foundation

## Status

Accepted

## Context

AionCore can register `ttn-v3` ingestion connectors and can run dynamic MQTT connector workers, but TTN v3 uplink payloads were previously modeled only as future work. Operators need a local, testable foundation that can decode common The Things Network / The Things Stack v3 uplink JSON without requiring a live TTN account or broker.

This milestone must remain domain-agnostic. It must not auto-provision entities, implement downlinks, require TLS/mTLS, validate live TTN connectivity, or add TTN-specific account management.

## Decision

Add payload format:

```text
ttn-uplink-json
```

Add a `TtnUplinkJsonDecoder` that parses common The Things Stack v3 uplink JSON. It extracts TTN device/application identifiers, decoded payload values, uplink timestamps, frame metadata, radio metadata, and settings when present.

The decoder maps primitive top-level `uplink_message.decoded_payload` fields to canonical observations:

- numbers become numeric observations;
- strings become text observations;
- booleans become boolean observations;
- nested objects and arrays are skipped for now and listed in metadata.

Observed properties use the `ttn:` prefix, such as `ttn:temperature`. Observation time prefers `uplink_message.received_at`, then root `received_at`, then ingestion time. Units can be supplied through connector metadata `unit_mapping`.

TTN device/application IDs and decoded payload keys are included in observation metadata and `aion:PayloadIngested` event metadata. Raw TTN JSON remains stored before normalization through the existing raw-message path.

`ttn-v3` connectors with `payload_format = "ttn-uplink-json"` are now valid worker plans. They are no longer skipped solely because the profile is `ttn-v3`. Other TTN payload formats remain invalid for dynamic worker planning in this milestone.

## Consequences

TTN uplink samples can be ingested through connector-aware HTTP and decoded into canonical observations without live TTN infrastructure.

Dynamic MQTT worker plumbing can start for valid `ttn-v3` connector configuration, but this is still generic MQTT runtime behavior. Live TTN account integration, downlinks, TLS/mTLS hardening, entity auto-provisioning, and TTN-specific operational validation remain future work.
