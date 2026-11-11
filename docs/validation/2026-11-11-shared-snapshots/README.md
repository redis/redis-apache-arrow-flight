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

## Broader real-Redis Go integration results

The full Linux integration/race run **failed**. Package serialization (`-p 1`, as
used by the existing environment runner) reproduced the following unchanged
integration assertions:

| Test | Observed failure |
|---|---|
| `TestIntegrationCommunityBufferedQueries` | Its cursor assertion requests `FT.INFO idx:public:<table>` and receives `SEARCH_INDEX_NOT_FOUND`; this assumes the earlier physical index layout. |
| `TestIntegrationQueryCancellation/active_stream_across_replicas_frees_resources` | Its cleanup assertion uses the same legacy index name and fails with `SEARCH_INDEX_NOT_FOUND`. |
| `TestIntegrationMetadata` | Native table types include `MATERIALIZED VIEW`, `TABLE`, `VIEW`; the assertion still expects exactly `TABLE`, `VIEW`. |
| `TestIntegrationSIGTERM` | Initial shared-catalog runs reported a retriable concurrent catalog change during ingestion. An isolated rerun passed ingestion but failed before shutdown: the temporary-table fixture finds no index under its old naming assumption. |

The fixture/test files above are unchanged from main; they were not skipped or
relaxed to produce a green result. The index assertions need adaptation to the
current native storage protocol before those resource/shutdown checks can
establish their intended guarantees. No full real-Redis Go-suite pass is claimed.

## Remaining gates

Do not enable the external Airbyte connector based on this run alone. Still
required: connector lifecycle integration and independent validation, real
independent-principal SQL grant/Redis ACL revocation, forced native lease loss,
gateway kill/restart and disconnect cleanup, direct compaction evidence, and
resolution of the broader Go integration failures above. No release, migration,
production deployment or durable resume is qualified here.
