# Upgrade Redis ADBC to v0.0.17

Requested by Albert Wong on 2026-10-22 while reviewing latest-major dependency
coverage. Baseline: `5ca8a4b`. Status: implemented and locally validated;
maintainer review pending.

Use the new released Redis ADBC driver rather than maintaining local dependency
patches. Its regexp2 v2 migration replaces this project's older transitive v1.
Preserve endpoint behavior, identity isolation and dataset storage contracts.

See [spec.md](spec.md) and [plan.md](plan.md).
