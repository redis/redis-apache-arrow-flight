# Plan and verification

1. Inspect pinned native protocol-17 resource descriptors and all four failures.
2. Repair shared cursor assertions and SIGTERM fixtures using actual metadata;
   preserve strong cleanup checks and add exact materialized-view discovery.
3. Serialize packages and cooperating suites, add process-level lock tests, and
   exercise the lock while queuing real Go and Python backend commands.
4. Run focused affected cases, full unit/race/static/build checks, complete
   real-Redis Go integration/race, and Python Flight SQL including fixed expiry.
5. Record evidence in the dated shared-snapshot validation record and update PR #67.

Completed so far:
- All four previously failing real-Redis cases passed together with `-p 1 -race`.
- Resource descriptor coverage includes retained segments and malformed metadata.
- Runner tests prove credential-independent database scope, preserved
  nonzero command status, same-scope serialization and different-scope concurrency.
- Vet and the seven release-gate unit tests passed.

Final results:
- Full applicable core Redis Enterprise Go integration/race suite passed:
  `go test -count=1 -p 1 -race ./...` (including the repaired SIGTERM case).
- The queued Python Flight SQL suite acquired the same scope lock only after
  Go finished: **4 passed in 121.70 seconds**, including fixed expiry, no skips.
- Full macOS unit/race suite passed with backend variables unset.
- Vet, formatting, diff checks, static build, resource-descriptor regression,
  seven release-gate tests and runner concurrency/termination tests passed.
- Existing enterprise-only optimization skips remain; no private composition,
  independent Airbyte connector or production release qualification is claimed.
See the [dated evidence](../../docs/validation/2026-11-11-shared-snapshots/README.md).
