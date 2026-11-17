# Intent: qualify Flight snapshots and enterprise upload merging

Date: 2026-11-17. Requester: Albert Wong. Base public revision: e4dcbd3.
The requester asked to work on public issues #27 and #65, then directed #27 to
the enterprise tracker. It is now enterprise issue #12; the public predecessor
was closed as a tracking migration, without claiming implementation completion.

Complete the remaining real-backend snapshot-contract checks and independent
Airbyte lifecycle validation. Retain the public core TLS/ACL and snapshot
boundary. Keep upload merging and its tests in the private optimization overlay.
Record exact source/client/native versions and actual checks, not inferred passes.

See [specification](spec.md), [plan](plan.md) and
[qualification evidence](../../docs/validation/2026-11-17-snapshots/README.md).
