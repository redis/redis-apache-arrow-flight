# Design evidence

Date: 2026-10-22. Author/reviewer: Codex. Human review pending.
Source: `b1992c67251471efebca07b0512edec0835391fb`.
Refs [intent](intent.md), [spec](spec.md), [plan](plan.md).

Read CLAUDE.md, REVIEW.md and the SDLC workflow. Inspected current server metrics,
health, router/actions, SQL execution/tickets, pools, cookie sessions, error
translation and named dataset generation/publication. The existing metrics
listener/health bypass authentication and must not silently become admin surfaces.
SQL uses the pinned driver; named datasets are an independent Redis/IPC path.

The requestor explicitly assigns the full optional control plane to Flight while
requiring native ordinary/Active-Active driver use without Flight. Documented
that boundary and implementation/failure gates, including independent Cloud
identity/bootstrap proof. Existing source is not evidence those features work.

This patch changes Markdown only. Local links/diff checks are appropriate;
runtime tests/benchmarks, deployment/API calls, dependency updates and current
runtime feature claims are not part of this patch. No credentials, licenses,
operational endpoints or infrastructure resource identifiers are included.

Reviewed official Software proxy/CRDB object and Cloud API documentation. Recorded
their distinct routing and optional management roles, and the proposed native
control-endpoint option without claiming it solves bootstrap/fencing. No live API
requests or infrastructure operations were needed for this documentation review.

Validation passed: 6 Markdown files, 40 local links and git diff --check.

Security-priority follow-up: the requestor makes TLS and security first-priority
scope for this control-plane design. Read the Redis security skill and official
Cloud TLS, Software TLS and certificate guidance; added concrete per-hop trust,
role/tenant separation, endpoint restrictions, secret rotation/revocation, audit
and negative-test gates before enabling management/migration. Current driver
sources expose rediss/custom CA handling; Flight exposes optional gRPC TLS and
currently defaults to AuthNone. These existing mechanisms do not prove the new
security contract. No runtime security implementation or test pass is claimed.

Security follow-up documentation checks passed: 40 local links and git diff --check.
