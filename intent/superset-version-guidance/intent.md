# Superset version guidance

Date: 2026-11-01. Source: `ff12a5a4e6afa91798029be4b4795a6040a113af`.
Requester: repository user. Status: implemented; maintainer review pending.

Document direct Redis ADBC as the recommended Superset 6.1 path and Flight SQL
as a candidate for Superset 7 after its SQLAlchemy 2 migration. Preserve the
distinction between dependency compatibility and an application validation pass.
No runtime, dependency or integration-test changes are requested.

Acceptance: a Markdown guide linked from existing client guidance, with upstream
sources, the recorded 6.1 blocker and explicit Superset 7 validation limits.
Maintainer decision: pending PR review.
