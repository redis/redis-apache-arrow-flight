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

Cloud Pro qualification comes first (AWS us-west-1/us-west-2, at least three
primary shards per region), Software next with the same minimum and verified
4x QPF. This is planning only; no runtime change, release or deployment is claimed.

The requestor prioritizes TLS and broader security now that the optional control
plane is in scope. Treat secure transport, separated identities/admin privileges,
secret lifecycle, endpoint trust and tenant isolation as prerequisites for enabling
management/migration, not deferred hardening after functionality ships.
