# ADR 0067: aion-api events and raw-messages route extraction

## Status

Accepted

## Context

Milestones 61 through 71 established the staged `aion-api` modularization pattern by extracting auth and error foundations first, then moving bounded route groups and the provenance search surface out of `apps/aion-api/src/lib.rs`.

After Milestone 71, `lib.rs` still contained two closely related but still cohesive HTTP read/write surfaces:

- `POST /events`
- `GET /events`
- `GET /events/{event_id}`
- `GET /raw-messages`
- `GET /raw-messages/{raw_message_id}`

These routes are tightly related to event and raw-message storage behavior, but they also share some lower-level metadata/header filtering primitives with the already-extracted provenance search route. Extracting events and raw messages after provenance keeps the modularization sequence incremental while avoiding a broader ingestion or entity split.

## Decision

Extract the events and raw-messages HTTP surfaces from `apps/aion-api/src/lib.rs` into dedicated route modules:

- `apps/aion-api/src/routes/events.rs`
- `apps/aion-api/src/routes/raw_messages.rs`

Move into `routes/events.rs`:

- route registration for `/events` and `/events/{event_id}`
- the existing create, detail, and list handlers
- `CreateEventRequest`
- `EventQuery`
- event-route-local metadata filter matching

Move into `routes/raw_messages.rs`:

- route registration for `/raw-messages` and `/raw-messages/{raw_message_id}`
- the existing detail and list handlers
- `RawMessageQuery`
- `RawMessageResponse`
- raw-message response shaping
- raw-message-route-local provenance filter matching
- raw-message header/payload decoding helpers used only by raw-message responses and filters

Create `apps/aion-api/src/query_filters.rs` for the shared metadata/header matching primitives that are used by both:

- events/raw-messages route filtering
- provenance search filtering

Keep in `lib.rs`:

- shared application state and route assembly
- central auth middleware wiring
- storage, entity, action, command, and raw-message existence helpers used across route groups
- centralized tests

## Consequences

Positive:

- `lib.rs` continues shrinking through narrow, behavior-preserving extraction.
- events and raw-messages now live in dedicated route modules with their route-local DTOs and helper logic.
- provenance compatibility is preserved because both route modules and provenance search still use the same shared metadata/header matching primitives.

Neutral / intentional:

- No endpoint paths, auth semantics, tenant/resource ownership behavior, filtering behavior, or JSON shapes changed.
- `POST /events` keeps its pre-existing behavior, including its current auth behavior, because the handler logic was moved without semantic changes.
- Tests remain in `lib.rs` to minimize churn during staged modularization.

Future work:

- continue extracting other cohesive remaining route groups from `lib.rs`
- revisit whether additional shared query/filter helpers should move again only when another extracted route group genuinely needs them
