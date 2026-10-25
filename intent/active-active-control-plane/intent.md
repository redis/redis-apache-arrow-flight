# Intent: optional Flight control plane

Date: 2026-10-22. Requestor: Albert Wong. Author: Codex. Status: design proposal;
human review pending. Source: `b1992c67251471efebca07b0512edec0835391fb`.
Refs [spec](spec.md), [plan](plan.md), [evidence](evidence.md),
[architecture](../../docs/active-active-control-plane.md).

The driver should just work in ordinary or Active-Active deployments without
Flight. Use Flight when control, centralized configuration or migration is wanted.
Move the full optional control plane here: product management discovery,
provisioning/data migration/fencing/cutover, desired policy and operator status,
logs/metrics. Preserve independent native correctness and the current driver
capabilities; do not duplicate its execution/allocation/recovery engine.

Cloud Pro qualification comes first (AWS us-west-2/us-east-1, at least three
primary shards per region), Software next with the same minimum and verified
4x QPF. This is planning only; no runtime change, release or deployment is claimed.

The requestor prioritizes TLS and broader security now that the optional control
plane is in scope. Treat secure transport, separated identities/admin privileges,
secret lifecycle, endpoint trust and tenant isolation as prerequisites for enabling
management/migration, not deferred hardening after functionality ships.

Commercial policy: monitoring, optimization, automation, backup and security are
paid-feature categories. A match to any one triggers requestor/maintainer review
before implementation/release. Record category/scope, existing versus new behavior,
paid packaging, standalone/safety impact and the actual decision. Current proposed
security/TLS and operational/control-plane capabilities are pending that review;
no implicit baseline exemption, existing-feature relicensing or billing/enforcement
implementation is authorized by this documentation. Preserve safe TLS and execution.

Build-profile update: community builds compile with public provider interfaces
and stubs; enterprise artifacts link complete implemented paid features. No
private dependencies are resolved by a clean community build. Optional telemetry
returns no feature data; SQL execution retains correct basic results. Requested
management/security/backup operations cannot silently succeed when their provider
is absent. Preserve safe failure and explicit capability reporting. Runtime
source extraction and both-profile tests remain pending.

## Managed-profile decision (2026-10-25)

The maintainer permits selected architectural decisions to change in an explicit
Flight-managed operating profile. Exact variations remain undecided. Record each
changed decision, service dependency, authority boundary, outage behavior, safe
transition and acceptance tests before implementing it. Native requirements remain
the baseline unless individually reclassified; attaching Flight does not silently
change them. This operating profile is distinct from community/enterprise builds.
See the [shared profile contract](https://github.com/alberttwong/redis-adbc-driver/blob/b71bc7c5678b3128abcf4a3029f8214e11943926/docs/flight-managed-profile.md).
