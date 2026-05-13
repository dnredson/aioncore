# AionCore Release Checklist

This checklist is for the current local MVP release-hardening pass. It is intentionally non-destructive and focused on validation, documentation readiness, and repository hygiene.

## Required Validation Commands

Run from the repository root:

```powershell
cargo fmt --all
cargo build -p aion-api
cargo test -p aion-storage
cargo test -p aion-api
git diff --check
```

If Windows file locking interferes with tests, rerun the affected command with an isolated `CARGO_TARGET_DIR` and record both outcomes.

## Artifact Hygiene Checks

These should return no tracked files:

```powershell
git ls-files target
git ls-files target_smoke
git ls-files node_modules
git ls-files smoke-*.log
```

## Demo And Local Runtime Checks

1. Start `aion-api` in the default local memory profile.
2. Validate `GET /health` and `GET /ready`.
3. Run `.\scripts\demo-mvp-memory.ps1`.
4. Enable static dashboard hosting with `AIONCORE_DASHBOARD_STATIC_DIR=apps/aion-dashboard`.
5. Open `/ui/`.
6. Confirm `/dashboard/*` API routes are still reachable and not shadowed by `/ui/*`.

## Documentation Sanity Checks

Confirm these documents are current and discoverable:

- [README](../README.md)
- [Documentation Index](INDEX.md)
- [Roadmap](ROADMAP.md)
- [Configuration](CONFIGURATION.md)
- [MVP Runbook](MVP_RUNBOOK.md)
- [Release Checklist](RELEASE_CHECKLIST.md)
- [MVP Demo Scenario](MVP_DEMO_SCENARIO.md)
- [MVP Scope Freeze](MVP_SCOPE_FREEZE.md)

## Security And Secret Hygiene

Run a lightweight secret-oriented search and inspect the hits:

```powershell
rg -n "password|secret|token|api_key|private_key" docs apps crates scripts -S
```

Pass criteria:

- no real secrets committed
- placeholders remain placeholders
- security documentation references are left intact

## Optional Helper Script

If present, run:

```powershell
.\scripts\check-release.ps1
```

This helper should remain non-destructive and should not require external services.
