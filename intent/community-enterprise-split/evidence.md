# Flight-only commercial boundary validation

Date: 2026-10-23. The maintainer restricted the commercial feature rule to Flight.
Driver PR #501 was reverted by #504; the full driver remains available in its
normal build. Both Flight profiles pin the restored driver commit
0ec68215d240 (v0.0.0-20261023155825-0ec68215d240), with no driver enterprise
composition or profile dependency. Flight-owned paid implementations remain split.

Before changing the dependency, checked latest driver release v0.0.18 and default
branch 518db9a7bb308994b6af5c7b3eec71be973ad8a2 on 2026-10-23. The selected
revision deliberately restores the pre-split tree rather than retaining stubs.

Driver and both Flight unit suites pass with REDIS_URI unset; backend integration
skips are expected. The full driver shared library and both Flight executables
build. Six Python composition safety tests pass, including building without an
additional driver composition. Whole-module fingerprint validation remains enabled.

Redis Software 8.6.2 was verified by `make -C go redis-up`: 1 shard, active 4x QPF.
Both Flight profiles pass focused integration for TestBuildIntegrationRoundTrip,
TestIntegrationSQL, TestIntegrationWritesRunOnce and TestIntegrationIngest. No
Cloud, multi-region, full integration-suite or performance qualification is claimed.

Prior split evidence is superseded for driver packaging. Flight's own community
capability flags/security rejection behavior remains in place; SQL delegates to
the full driver in both builds. No new migration, backup or Active-Active runtime
was implemented by this correction.

2026-10-23 documentation follow-up: recorded the authoritative newer Active-Active
proposal and clarified Flight ownership. No runtime files, build contracts or
qualification claims changed. Diff/local-link checks pass; no server tests rerun.
