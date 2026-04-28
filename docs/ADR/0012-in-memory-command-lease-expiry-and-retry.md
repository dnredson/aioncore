# ADR 0012: In-Memory Command Lease Expiry and Retry

## Status

Accepted

## Context

Executor agents can claim compatible Commands, but a claimed Command should not remain stuck forever if the executor crashes, disconnects, or never reports a result. The local MVP still keeps all state in memory and must remain domain-agnostic, with AionCore creating Commands but never executing real actions directly.

Approval policies and executor capability/scope checks must continue to protect command execution. Lease expiry and retry semantics are needed before adding production transports, persistence, or executor credentials.

## Decision

Add an in-memory CommandLease model with active, expired, released, completed, and failed states. Executor command claims create an active lease, store the lease expiry on the Command, and block other executors from claiming the Command while the lease is active.

Add local API endpoints to inspect the latest command lease, refresh the lease for the owning executor, release a lease back to pending, and recover expired leases. Expired lease recovery marks leases expired, schedules retries while the retry limit allows it, and marks Commands failed when the retry limit is exceeded.

Completion and failure through executor endpoints mark the active lease completed or failed while preserving the existing Action and ActionResult behavior.

## Consequences

- Local executor testing can recover from abandoned claimed Commands.
- Retry behavior remains explicit and in-memory only.
- Approval, capability, and scope gates remain part of the command lifecycle.
- AionCore still does not execute real commands.
- Future work needs durable lease persistence, background recovery scheduling, executor authentication, Origin validation for browser-facing flows, and distributed concurrency controls.
