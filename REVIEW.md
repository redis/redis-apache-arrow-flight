# Review policy

Status: proposed project policy, prepared 2026-10-20. Maintainer acceptance is not yet recorded.
Applies to human and AI-assisted changes. Evidence baseline: [project context](docs/sdlc/context.md).

## Review passes

1. **Behavior:** Compare the diff with the change's intent, spec, and plan. Check routing, gRPC status codes, prepared handle rebinding, write execution timing, and metadata schemas. Report an unplanned behavior change with a concrete failing scenario.
2. **Identity and data access:** Trace Redis credentials through authentication, tenants, sessions, and result tickets. Check session ownership, credential changes, and ACL-denied operations. Health's authentication exception is intentional. Do not introduce credentials in logs, metrics labels, help defaults, or fixtures.
3. **Storage and lifecycle:** Check dataset generation ordering, atomic commit, ambiguous commit replies, cancellation, TTL/drop behavior, persistent sequence keys, same-slot Lua keys, and legacy storage reads. Check Arrow Retain/Release, pool cleanup, cursor cleanup, session settings restoration, and panic paths.
4. **Capacity and operations:** Check stream limits, fallback budget accounting, slow consumers, shutdown drain, health, and metrics. Distinguish per-process limits from Redis index limits shared by all clients. Assess mixed-version deployment and rollback.
5. **Evidence:** Use [testing.md](docs/sdlc/testing.md) to select checks. Confirm integration tests actually ran with `REDIS_URI`; a skipped test is not a pass. Client compatibility claims require evidence for that client.

## Findings and severity

Each finding includes severity, file/line, trigger, user impact, and suggested verification. Distinguish confirmed defects from hypotheses needing reproduction.

- **Critical:** demonstrated cross-identity access, credential disclosure, or widespread irreversible data loss.
- **Important:** reproducible wrong results, repeated writes, resource leaks/exhaustion, broken compatibility, or missing checks for a changed critical contract.
- **Nit:** optional readability or documentation polish without behavioral impact. Report at most three.

Prioritize actionable findings. Avoid repeating formatting output or flagging unchanged code unless the diff activates the defect. Review dependency changes through `go.mod`, `go.sum`, the replacement, and affected call sites; do not manually review every checksum.

## Review record

Record linked intent/spec/plan, findings and dispositions, checks run and skipped, remaining risk, and maintainer decision in the PR. A model's review is evidence, not a recorded human approval. Branch protection, CODEOWNERS requirements, hosted review integrations, and production approval rules cannot be established from the checked-in workflows alone.
