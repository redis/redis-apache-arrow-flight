# Shared snapshot validation — 2026-11-11

Runtime source: PR #67 commit `d655a6ac9df588fdb868ef9cc299216dc42f0c61`.
The follow-up changes only the Python negative test's header replacement and this
validation record/documentation. No production gateway change was needed for the
Python test failure described below. Local validation is not independent Airbyte
connector validation or release qualification.

## Environment

- Core public gateway, built without private overlays, Linux arm64 / Go 1.27.3.
- Native Redis ADBC v0.0.24, Go revision `b052eedad9f2`.
- Isolated Redis Software image `redislabs/redis:8.2.0-78.18`, Redis **8.6.2**,
  Search enabled, **three active primary shards**, no replicas, **active 4x QPF**,
  1 GiB database capacity and `noeviction`.
- Verified TLS and username/password authentication on both Flight and Redis.
  Generated CA/certificates and credentials stayed in ignored local files.
- Python **3.12.12**, ADBC Flight SQL/driver manager **1.13.0**, PyArrow **26.0.0**,
  pytest **9.2.0**, from a cached test image. The Python client loads Flight SQL,
  not the native Redis ADBC library.
- Separate Docker projects/network/database were created for this run; the
  pre-existing Redis container was not modified.

Provisioning verified the management topology/QPF configuration, Redis ACL
identity, Search read/write operations, and rejection of plaintext, absent/wrong
credentials and an untrusted certificate.

## Local checks

Passed:

- `env -u REDIS_URI -u REDIS_INTEGRATION_REQUIRED go test -count=1 -race ./...`
  on macOS arm64; backend tests explicitly skipped in this unit-only run.
- `go vet ./...`, `gofmt -l .` (empty), `git diff --check`.
- Seven `scripts/test_enterprise_release_gate.py` unit tests.
- Python syntax checks for provisioning/release scripts and snapshot example/test.
- `make build` and a static Linux arm64 gateway build. Go emitted a nonfatal
  read-only module stat-cache warning in the sandbox; builds exited zero.

Hosted PR checks were green for tests, docs build, binary/image builds, and
platform artifact verification. A green PR workflow is not a release receipt.

## Actual Python Flight SQL transport

Command inside the isolated test client, with connection settings supplied by a
local environment file and certificates mounted read-only:

```sh
python -m pytest -v -o cache_dir=/tmp/pytest-flight-snapshot --tb=short \
  tests/integrations/test_shared_snapshot.py
```

`FLIGHT_SNAPSHOT_LONG_TESTS=1` enables the native minimum two-minute expiry case.

The first run passed exact two-table/view metadata, schema/value/multiplicity
comparisons through DML/DDL/TRUNCATE/DROP, exception cleanup, and expiry. Its
negative-handle case failed during release because the actual C++ Flight SQL
client **appends nonempty RPC header values**. Restoring the handle by setting it
again retained the intentionally invalid value and produced duplicate headers.
The gateway correctly rejected those requests. The test now clears the header
with an empty string before replacing it; the focused ownership case passed.

**Final complete rerun: 4 passed in 122.07 seconds, no skips or warnings.**
The fixed-lifetime test waited 121 seconds and verified repeated reads fail
rather than falling back to live data.

Repeated updates exercise the native publication/compaction path, but this test
did not independently instrument completion of a compaction. Its foreign-session
case uses another connection with the same test credentials; independent-principal
coverage remains in the Go TLS test with a native test double, not this live case.

## Initial broader real-Redis Go integration results

These are the original failures, subsequently repaired as documented below.

The full Linux integration/race run **failed**. Package serialization (`-p 1`, as
used by the existing environment runner) reproduced the following unchanged
integration assertions:

| Test | Observed failure |
|---|---|
| `TestIntegrationCommunityBufferedQueries` | Its cursor assertion requests `FT.INFO idx:public:<table>` and receives `SEARCH_INDEX_NOT_FOUND`; this assumes the earlier physical index layout. |
| `TestIntegrationQueryCancellation/active_stream_across_replicas_frees_resources` | Its cleanup assertion uses the same legacy index name and fails with `SEARCH_INDEX_NOT_FOUND`. |
| `TestIntegrationMetadata` | Native table types include `MATERIALIZED VIEW`, `TABLE`, `VIEW`; the assertion still expects exactly `TABLE`, `VIEW`. |
| `TestIntegrationSIGTERM` | Initial shared-catalog runs reported a retriable concurrent catalog change during ingestion. An isolated rerun passed ingestion but failed before shutdown: the temporary-table fixture finds no index under its old naming assumption. |

Those fixture/test files were unchanged from main during the initial run and
were not skipped or relaxed. Their index assertions needed adaptation before
resource/shutdown checks could establish their intended guarantees. That initial
run did not establish a full real-Redis Go-suite pass; the subsequent repairs and
successful complete rerun are documented below.

## Remaining gates

Do not enable the external Airbyte connector based on this run alone. Still
required: connector lifecycle integration and independent validation, real
independent-principal SQL grant/Redis ACL revocation, forced native lease loss,
gateway kill/restart and disconnect cleanup, direct compaction evidence, and
any additional enterprise-composition qualification required for paid features.
No release, migration, production deployment or durable resume is qualified here.

## Follow-up: issues #68–#71 repaired

The user requested fixes for all four issue groups. The follow-up changes are
verification/harness code; production gateway behavior and the pinned driver are
unchanged. A fresh disposable Software instance used the same verified TLS/ACL,
three-primary-shard, active-4x-QPF topology described above.

- **#68:** test-only read-only protocol-17 metadata inspection now atomically
  resolves the committed root, metadata bucket and actual table descriptor.
  Cursor assertions inspect every primary/retained-segment index. Unsupported
  formats, broken descriptors, missing indexes and unavailable statistics fail;
  they do not become zero cursors or skipped assertions.
- **#69:** the metadata test asserts exactly MATERIALIZED VIEW, TABLE and VIEW.
  It creates a real materialized view and checks filtered discovery, type and
  serialized schema through Flight SQL.
- **#70:** SIGTERM setup resolves the actual temporary schema/index and captures
  all managed indexes. Post-shutdown checks require zero cursors, logical temp
  removal and physical temp-index reclamation after normal native sweep. The
  test still verifies deadline cancellation, actual unread row counts, successful
  upload completion and exact dataset read-back. It reaches and passes these
  checks rather than bypassing setup.
- **#71:** the supported integration target serializes Go packages with `-p 1`.
  `scripts/run-integration.py` holds a POSIX advisory scope lock for the command's
  lifetime. Cooperating Go/Python suites sharing a database use the same scope;
  container aliases require an explicit shared scope/lock location. Separate
  hosts need separate databases. No SQL write retry or error suppression was added.

Results:

- Focused previously failing cases: **passed** with `-race -p 1` against real Redis.
- Full Linux Go backend suite: **passed** with `REDIS_INTEGRATION_REQUIRED=1 go test
  -count=1 -p 1 -race ./...`; the command and server packages passed in 9.348s and
  29.595s respectively. All applicable core backend tests ran; existing
  enterprise-only optimization skips remain unchanged.
- The Python command was started while Go held the scope lock. Its log showed
  waiting, then acquisition only after Go exited: **4 passed in 121.70 seconds**,
  no skips, including the fixed two-minute native expiry case.
- Full macOS unit/race suite, vet, formatting/diff checks and static build passed.
- Test-runner checks cover shared-scope serialization, independent-scope
  concurrency, credential-independent scope normalization, nonzero exit status,
  and waiting for child cleanup with a failing exit status on termination.
- The resource-inspection regression verifies retained-segment enumeration and
  explicit rejection of malformed descriptors.

The four reported blockers are addressed. Independent Airbyte connector checks,
real grant/ACL revocation, forced lease loss, kill/disconnect coverage, direct
compaction evidence and any private-composition/release qualification remain
separate gates. No production release or automatic connector enablement follows.
