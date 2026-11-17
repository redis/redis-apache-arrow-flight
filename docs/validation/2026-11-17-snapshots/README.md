# Snapshot and upload qualification — 2026-11-17

Source change: [intent, specification and plan](../../../intent/snapshot-qualification/intent.md).
This record covers the gateway snapshot protocol, an independent Airbyte source
change, and the paired enterprise-only dataset upload optimization. A source
qualification is not a published gateway/connector release or production rollout.

## Inputs and environment

- Public base: `5702927b5a8831481fa727f079f0c620d305b7dc` plus this change.
- Private source base: `5fb5ff08da8c679b07559e8367cd5706b307afc1` plus the
  paired source composition; complete Go-module fingerprints are checked by its
  builder. The public repo contains no private implementation source.
- Latest stable native driver checked 2026-11-17: **v0.0.28**, release commit
  `b63a010d88d2b69707594cb9dc063992dac06b32`, Go submodule
  `v0.0.0-20261117191025-b63a010d88d2`. Default branch at check:
  `2b6634e0c7c92d0c8148dff62225671cb7a15c4e`; unreleased main was not selected.
- Go 1.27.4, Arrow Go 18.9.0, ADBC Go 1.13.0, gRPC 1.85.0;
  Linux arm64 containerized integration runners. Local unit checks
  additionally run on macOS arm64.
- Disposable Redis Software `redislabs/redis:8.2.0-78.18`, Redis 8.6.2, Search,
  three active primary shards, active **4x QPF**, 1 GiB capacity, no replicas,
  `noeviction`. Management topology/QPF and Search read/write operations were
  verified, along with rejection of plaintext, absent/wrong credentials and an
  untrusted CA. Flight and Redis connections use verified TLS and username/password
  authentication. Secrets/certificates and raw operational records remain ignored.
- Python 3.12.15, ADBC Flight SQL/driver manager 1.13.0, PyArrow 26.0.1,
  pytest 9.2.0; native-catalog test inspector redis-py 8.1.0. The actual Flight
  Python client does not load the native Redis shared library.
- Independent connector base `3b110b0d4ac3b5134b028d8e9185da3134abba7c`,
  incorporating its concurrent dependency refresh, Airbyte CDK 7.49.0 and
  PyArrow 26.0.1 from its own dependency lock, with the separate
  proposed Flight snapshot lifecycle change. Published connector versions retain
  their existing behavior until that change is merged and released.

## Executed checks

Both complete Go backend profiles passed with the race detector and required
integration mode against the secured Software fixture. The enterprise run
recorded 206 passing named tests/subtests and one expected community-only skip;
the community run recorded 154 passes and one expected enterprise-only skip.
[Per-case results](go-results.json) preserve package and skip distinctions;
skips are not passes. Local macOS unit/race checks passed for both compositions.

All **seven actual Python snapshot tests passed**, including the real fixed
lifetime and natural native lease collection after SIGKILL/restart. The gateway
kept its token secret across restart, so failure demonstrates lost session state
rather than invalidating authentication by rotating the signing key.
[Python results](python-results.json) record each case. Both independent Airbyte
Flight integration tests passed using its own dependency lock (12.60 seconds).
The connector unit suite passed 194 tests; 12 integration cases were deselected
for that unit command, and the two Flight cases were run separately.

[Composition fingerprints](composition-fingerprints.json) hash the full public
Go-module manifest, replaced community files and private overlay source. Each
hash uses SHA-256 over UTF-8 JSON with sorted keys and compact separators; the
private source itself remains private. Final source revisions are recorded in
the accompanying PRs and the private manifest's qualified public commit.
Pre-refresh runs remain historical and do not qualify the newer dependencies.

## Dependency and refactor follow-up

All seven refreshed Python profiles passed with the same secured Redis backend
through a separate loopback-only compatibility gateway. That gateway used
ordinary unauthenticated Flight transport, as required by the existing profile
fixtures; these runs do not qualify Flight authentication. The dedicated snapshot
tests above exercised verified TLS/authentication on both hops. The profile
report covers 24 automated tool routes; RStudio, Tableau and Metabase remain
three explicit manual application gaps. [Profile results](dependency-profiles.json)
and the per-profile `*-versions.json` files record resolved dependencies.

Datasets 5.2.0 still has the exact two zero-shard empty-save expected failures;
the explicit one-shard workaround and nonempty controls passed. The classifier
accepts only that version/signature, and unexpected passes fail. Issue #57 remains
open. Java 21.0.13, JDBC 19.0.0, Quarto 1.11.9, R 4.6.2 and the isolated R
driver-manager package were used where applicable. These are custom workflow
checks, not GUI application certification.

The Go runtime upgrades and stream reference-counting fix from public PR #76
are retained and ported into the private composition. This change additionally
shares the existing read-only protocol-17 catalog resolver between cursor
inspection and integration fixtures, removes stale guessed-index lookup and
reuses session-token validation for cancellation IDs. The private optimization
remains private. Arrow 18.9 still needs the existing failed-send record handoff
workaround; it was retained. Native ingest/refusal handling was also retained
because replacing it with the higher-level helper changes error wrapping and
ownership semantics. `go vet`, script tests, example cleanup tests and strict
MkDocs build passed.

## Upload benchmark

The private enterprise implementation was compared with its unmerged upload
implementation on the same composed build and fixture. Each case used 100,000
rows, ten numeric columns and 100 uploaded batches of 1,000 rows, one client,
10 iterations per repetition and five repetitions. Timing includes in-process
Flight RPC (`bufconn`) and TLS Redis access, not a remote client network. The
Docker Linux arm64 environment exposed 18 CPUs and about 23.7 GiB memory; Redis
had the 1 GiB database limit described above. Fixtures ran serially.

| Small-batch case, median | Unmerged | Merged |
| --- | ---: | ---: |
| Upload | 13.54 ms | 9.90 ms |
| Read all ten columns | 14.20 ms | 9.05 ms |
| Read one column | 1.98 ms | 1.32 ms |
| Stored batches | 100 | 4 |
| Stored Arrow IPC | 8,280,000 B | 8,011,200 B |
| Redis generation HASH memory, read fixture | 10,301,285 B | 8,850,505 B |
| Peak tracked Arrow allocation, upload | 576 B | 2,080,192 B |

See [all medians](benchmark-medians.json), [baseline output](baseline.log) and
[merged output](merged-bench.log), including ordinary-size and mixed-type controls.
This is local comparative evidence, not a production throughput or latency SLO.
The tracked Arrow allocator is not process RSS and excludes Go/native allocations;
concatenation temporarily needs additional output memory. Redis HASH memory is
one generation, not total Redis memory. INFO counters were cached/proxied as zero
and include fixture activity; they establish no command, network or CPU savings.
The buffer's 2 MiB retained-capacity and 1,024-reference limits, large-backing-array
bypass, changed/nested dictionary handling, error cleanup and endpoint/range
compatibility have separate unit/integration assertions in the private overlay.

## Coverage and repeatable commands

Use a fresh secured Software catalog and the paired enterprise builder, with
connection/management settings supplied privately. Serialize cooperating suites
through `scripts/run-integration.py` and one database scope. No SQL write is
replayed by this harness.

```sh
python3 scripts/run-integration.py -- \
  python3 /enterprise/build.py --source /source -- \
  go test -count=1 -race -p 1 -timeout 180s ./...
make test-shared-snapshot
python3 -m pytest -q tests/test_shared_snapshot_example.py
```

For all Python fault cases, set `FLIGHT_SNAPSHOT_LONG_TESTS=1`,
`FLIGHT_SNAPSHOT_FAULT_INJECTION=1`, `FLIGHT_SNAPSHOT_GATEWAY_CONTAINER` to the
**disposable** gateway, and `REDIS_URI`/`REDIS_CA_FILE` for verified native test
inspection. This sends SIGKILL to that container and removes only one precisely
identified newly acquired native pin in the lease-loss test. See the
[full opt-in contract](../../shared-read-snapshots.md#additional-fault-qualification).

| Acceptance area | Executable evidence |
| --- | --- |
| Exact schemas, values, NULLs, duplicates; metadata and two tables/view through DML/DDL/TRUNCATE/DROP | `test_two_tables_metadata_and_publications` |
| Real installed compaction while preserving a retained view | `test_snapshot_survives_observed_compaction`; protocol-17 inspector verifies a new installed `_compacted` index |
| Actual native lease loss, terminal failure and no live fallback | Python `test_native_lease_loss_never_falls_back`; Go `TestIntegrationSnapshotLeaseLoss` |
| Gateway SIGKILL/restart, no resume, native pin expiry and collection | `test_gateway_kill_restart_loses_session_and_expires_pin`, waiting for the real lease rather than altering Redis time |
| Idle abandoned transport releases native pin; reconnect cannot resume | `TestIntegrationSnapshotDisconnectAndIdleExpiry` |
| Foreign authenticated identity cannot use the owner's cookie/handle | `TestIntegrationSnapshotIndependentPrincipal` |
| Live Redis ACL and native SQL SELECT grant revocation | `TestIntegrationSnapshotIndependentPrincipal`, `TestIntegrationSnapshotSQLGrantRevocation`; supported Software management API, independent principals |
| Pending/active cancellation and abandoned-reader resource cleanup | `TestIntegrationSnapshotReaderCleanup` |
| Fixed native lifetime; exception cleanup; invalid/foreign handles | Existing actual-Python transport tests, including the 121-second expiry case |
| Primary extraction/cancellation errors survive failed release | `tests/test_shared_snapshot_example.py`; release errors after otherwise successful extraction still fail |
| Independent Airbyte check/discover/read and generator close | Separate connector `tests/test_flight_snapshot_integration.py`; real source path over ADBC Flight SQL |

The exact scope remains one native managed catalog/hash slot. No independent-shard
snapshot, arbitrary external HASH snapshot, failover deployment, migration,
durable resume or production release is qualified by these checks.
