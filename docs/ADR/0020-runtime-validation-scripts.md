# ADR 0020: Runtime Validation Scripts

## Status

Accepted

## Context

AionCore already has Rust unit and integration tests, but developers also need
quick local checks that exercise the running API end to end. Those checks should
verify storage mode, readiness, ingestion, query paths, and MCP tool calls
without adding production dependencies.

## Decision

Add simple PowerShell scripts for runtime validation:

- memory runtime validation
- PostgreSQL runtime validation
- optional startup wrappers for memory and PostgreSQL modes

Keep the scripts developer-friendly and explicit. They should fail fast on the
first assertion and remain Windows-first without changing runtime behavior.

## Consequences

- Developers can verify the running service without touching Rust tests.
- Memory and PostgreSQL runtime paths can be checked from the command line.
- The scripts provide a simple foundation for future CI smoke checks.
- Runtime behavior is unchanged; the scripts only observe and validate it.
