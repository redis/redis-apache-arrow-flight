# Snapshot and upload qualification — 2026-11-17

Source change: [intent, specification and plan](../../../intent/snapshot-qualification/intent.md).
This record covers the gateway snapshot protocol, an independent Airbyte source
change, and the paired enterprise-only dataset upload optimization. A source
qualification is not a published gateway/connector release or production rollout.

## Inputs and environment

- Public base: `e4dcbd37322536f73ef923e0ab4b76a1abf3d4dc` plus this change.
- Private source base: `5fb5ff08da8c679b07559e8367cd5706b307afc1` plus the
  paired source composition; complete Go-module fingerprints are checked by its
  builder. The public repo contains no private implementation source.
- Latest stable native driver checked 2026-11-17: **v0.0.28**, release commit
  `b63a010d88d2b69707594cb9dc063992dac06b32`, Go submodule
  `v0.0.0-20261117191025-b63a010d88d2`. Default branch at check:
  `2b6634e0c7c92d0c8148dff62225671cb7a15c4e`; unreleased main was not selected.
- Go 1.27.3; Linux arm64 containerized integration runners. Local unit checks
  additionally run on macOS arm64.
- Disposable Redis Software `redislabs/redis:8.2.0-78.18`, Redis 8.6.2, Search,
  three active primary shards, active **4x QPF**, 1 GiB capacity, no replicas,
  `noeviction`. Management topology/QPF and Search read/write operations were
  verified, along with rejection of plaintext, absent/wrong credentials and an
  untrusted CA. Flight and Redis connections use verified TLS and username/password
  authentication. Secrets/certificates and raw operational records remain ignored.
- Python 3.12.15, ADBC Flight SQL/driver manager 1.13.0, PyArrow 26.0.0,
  pytest 9.2.0; native-catalog test inspector redis-py 8.1.0. The actual Flight
  Python client does not load the native Redis shared library.
- Independent connector base `addf873`, Airbyte CDK 7.45.0, with the separate
  proposed Flight snapshot lifecycle change. Published connector versions retain
  their existing behavior until that change is merged and released.

## Required final checks

Final exact-source checks and fingerprints are recorded here after execution.
Preliminary checks against the earlier driver/private checkout are not treated
as qualification of the final source.

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
