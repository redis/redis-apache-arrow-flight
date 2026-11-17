# Preserve sessions after definite publication refusals

- Requester: [issue #74](https://github.com/redis/redis-apache-arrow-flight/issues/74).
- Date: 2026-11-16 (America/Los_Angeles).
- Baseline: `86bbf1e03a80f0bf3d81722f791da7d9d74d79c3`.
- Scope: core SQL session correctness and regression evidence; no new paid
  monitoring, optimization, automation, backup or security implementation.
- Goal: preserve connection-local SQL settings and temporary objects after a
  proven statement-local publication refusal, while returning the original
  failure without replaying SQL or Arrow input.
- Requester follow-up: use latest driver code. Pin the verified default-branch
  revision instead of the repository's usual stable-release preference.
- Acceptance: deterministic native-driver fault injection, NotFound control,
  connection/cookie identity, timezone/schema/temp rows after fresh safe retries,
  committed-prefix and consumed-input protection, conservative negative cases.
- Design and results: [spec](spec.md), [plan/evidence](plan.md).
- Status: implemented locally; maintainer review and downstream audit pending.

The original four-worker Rust observation remains a reported symptom. A
deterministic reproduction of the server's connection-discard path establishes
that the refusal can cause state loss; it does not prove which error occurred
in the uninstrumented downstream run.
