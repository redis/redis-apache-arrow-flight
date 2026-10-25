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

Commercial policy follow-up: the requestor designates monitoring, optimization,
automation, backup and security as paid-feature categories and requires review
on any match. Added a shared commercial gate, current candidate inventory and
required decision evidence; current security/TLS scope is explicitly pending
baseline-versus-premium review. No packaging approval, license/runtime change,
billing integration or existing-feature removal is claimed.

Commercial follow-up checks passed: 6 Markdown files, 41 local links and git diff --check.

Build-profile update: community builds compile with public provider interfaces
and stubs; enterprise artifacts link complete implemented paid features. No
private dependencies are resolved by a clean community build. Optional telemetry
returns no feature data; SQL execution retains correct basic results. Requested
management/security/backup operations cannot silently succeed when their provider
is absent. Preserve safe failure and explicit capability reporting. Runtime
source extraction and both-profile tests remain pending.

## Managed-profile alignment (2026-10-25)

The maintainer explicitly requested that optional Flight management may change
selected architecture decisions, enabling centrally coordinated capabilities
unavailable under standalone constraints. Updated the public design and change
records to distinguish operating profiles from build profiles and to require
explicit per-capability dependencies, authorization, failure behavior and safe
transitions. Candidate changes remain undecided; this does not implement them.

Corrected the current Cloud target to approved AWS us-west-2/us-east-1 and the
SDLC commercial scope to Flight-owned implementations only. The native driver
retains its full public implementation. This is documentation only: no Go module,
provider, build composition, runtime or infrastructure changed. Local Markdown
links and whitespace were checked; no runtime tests were repeated.

## Automatic Active-Active enterprise placement (2026-10-25)

Albert Wong explicitly directed that Active-Active features automatically belong
in Flight Enterprise. From source `092f0d0`, updated repository guidance, build
profiles, architecture and current change records: every Flight-owned
Active-Active capability is Enterprise, including undecided managed variations,
without a separate MOABS category/edition decision. Public contracts and
unavailable community stubs remain; the full native driver stays public.

Removed stale current-plan wording applying the commercial rule to the driver
and describing the existing source split as pending. Historical evidence remains
dated. Documentation-only validation covers local Markdown links, whitespace and
policy consistency: all 41 local links in the eight changed Markdown files and
`git diff --check` passed. Inspection of `server/edition.go` confirms that
`active_active` remains false; classification does not enable the capability.
No code, provider, module pin, composition fingerprint, entitlement mechanism or
infrastructure changed; no runtime tests were rerun.
