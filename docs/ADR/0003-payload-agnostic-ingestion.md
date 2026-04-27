# ADR 0003: Payload-Agnostic Ingestion

## Status

Accepted

## Context

IoT telemetry arrives in many formats, including SenML JSON, UltraLight, custom JSON, and future formats. Ingestion should not be tied to one payload schema.

AionCore also needs auditability. Raw messages must not be lost when decoding fails.

## Decision

AionCore ingestion will be payload-agnostic.

Every raw message will be stored before normalization. After storage, a selected decoder attempts to convert the payload into decoded measurements and then canonical observations.

Initial decoder support:

- SenML JSON.
- UltraLight.
- JSON mapping.

## Consequences

Positive:

- Failed normalization does not lose source data.
- New payload formats can be added through decoder implementations.
- Raw message replay is possible later.

Negative:

- Ingestion responses must represent partial success when raw storage succeeds but normalization fails.
- Decoder selection must be explicit enough to avoid ambiguity.

## MVP Simplification

Prefer explicit decoder hints from headers, route configuration, or mapping names. Postpone automatic payload format detection.
