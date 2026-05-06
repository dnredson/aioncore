# ADR 0069: aion-api entity route extraction

## Status

Accepted

## Context

Milestones 61 through 73 established the staged `aion-api` modularization pattern by extracting auth and error foundations first, then moving bounded route groups such as adapters, auth, executors, commands, SmartSentinel, MCP, AI context, provenance, events/raw-messages, and observations out of `apps/aion-api/src/lib.rs`.

After Milestone 73, `lib.rs` still contained the remaining entity-centered HTTP surface:

- `POST /entities`
- `GET /entities`
- `GET /entities/{entity_id}`
- `GET /entities/{entity_id}/context`
- `POST /relationships`
- `PUT /entities/{entity_id}/capabilities`
- `GET /entities/{entity_id}/capabilities`
- `PUT /entities/{entity_id}/payload-profile`
- `GET /entities/{entity_id}/payload-profile`

These routes are part of the core domain surface and are depended on by ingestion, SmartSentinel, AI context, and later observation-history and dashboard work. Extracting observations first reduced the risk of mixing this milestone with upcoming time-series concerns; extracting the remaining entity surface next keeps the route split incremental while preparing the codebase for historical query additions.

## Decision

Extract the entity-centered HTTP surface from `apps/aion-api/src/lib.rs` into:

- `apps/aion-api/src/routes/entities.rs`

Move into `routes/entities.rs`:

- route registration for the existing `/entities*` and `/relationships` endpoints
- the existing entity, relationship, capability, and payload-profile handlers
- route-local DTOs for entity creation, relationship creation, capability updates, and payload-profile updates
- entity route helper logic for JSON-LD request parsing, `entity_key` extraction/derivation, and entity-context response shaping

Keep in `lib.rs`:

- shared application state and route assembly
- central auth middleware wiring
- shared auth and tenant/resource ownership helpers
- shared existence helpers such as `ensure_entity_exists`
- ingestion, SmartSentinel, AI context, and other non-entity route groups
- centralized tests

## Consequences

Positive:

- `lib.rs` continues shrinking through a narrow, behavior-preserving extraction.
- entity-centered routes now live in one dedicated module with their route-local DTOs and helpers.
- future historical observation/time-series APIs and dashboard-facing read surfaces can be added without first untangling the core entity HTTP surface from the main application module.

Neutral / intentional:

- No endpoint paths, auth semantics, tenant/resource ownership behavior, JSON-LD parsing behavior, `entity_key` derivation behavior, relationship behavior, capability behavior, payload-profile behavior, or JSON shapes changed.
- Dev/disabled-mode auth bypass, token-mode scope enforcement, and `admin:all` behavior remain unchanged.
- Tests intentionally remain in `lib.rs` to minimize churn during staged modularization.
- Shared helpers still used across route groups intentionally remain in `lib.rs` to avoid premature movement and modularization risk.

Future work:

- continue extracting only cohesive remaining route groups or helpers when there is a concrete need
- add historical observation/time-series query APIs in a separate milestone without changing existing `/observations` or entity route behavior
- keep dashboard work separate from this route-extraction phase
