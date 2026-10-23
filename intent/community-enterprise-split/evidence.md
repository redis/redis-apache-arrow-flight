# Evidence

Implementation in progress, 2026-10-22. User authorization: execute the chosen
community-stub/complete-enterprise split. Human code review is pending.
Full-module source fingerprints prevent stale composition. Original feature test
files are preserved in the enterprise source map; community keeps shared core
checks and new explicit profile isolation/failure/SQL tests. No private repository
identity or operational credential is published here. Detailed validation results
are appended after final checks; no full integration pass is implied.

Driver dependency check, 2026-10-22: latest upstream release v0.0.18 and main
f22241ac641b0dc0d251695a0a154fa71935d961 were inspected before pinning. This
change intentionally selects split runtime ebfb32ee2a15b5582db061902f3ff9f9e100ac9b,
not an unrelated main upgrade. Preserve the fork replacement at immutable
v0.0.0-20261023004747-ebfb32ee2a15; go.sum records its checksum.

Community and enterprise unit suites pass. Community builds/tests also pass with
GOFLAGS empty, GOWORK off and GOPROXY off against cached public modules. Both
executables build. New profile checks verify the driver and Flight editions agree;
enterprise startup/composition rejects a missing matching enterprise driver.
Community monitoring returns HTTP 204 and health UNKNOWN; requests for missing
TLS/authentication fail explicitly. Basic lifecycle logging remains core, and the
community Dockerfile has no permanently failing paid health probe.

Both profiles pass selected Software/4x integration for complete SQL round trips,
SQL semantics, ingestion and write-once ticket replay. This is focused integration,
not the entire Flight suite or multi-region/dataset qualification. Six composition
safety tests pass in each extension project. No benchmark, Cloud deployment or
new backup/migration/Active-Active implementation is claimed. Human review pending.
