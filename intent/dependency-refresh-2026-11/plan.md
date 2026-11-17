# Implementation and validation

Checked 2026-11-17 against baseline `e4dcbd37322536f73ef923e0ab4b76a1abf3d4dc`.

## Upstream inventory and changes

Queried `go list -m -u -json all`, PyPI's package JSON API and Maven Central's
JDBC metadata. Updated explicitly required Go modules with available updates,
then ran `go mod tidy`; see go.mod/go.sum for the complete transitive graph.
Dependencies appearing only in other modules' test/tool graphs are not promoted
into this module's requirements merely to force an update. A final repeat
reported no remaining updates for explicitly required Go modules.

- Redis ADBC: [v0.0.28](https://github.com/redis/redis-adbc-driver/releases/tag/v0.0.28),
  source `b63a010d88d2b69707594cb9dc063992dac06b32`, Go pseudo-version
  `v0.0.0-20261117191025-b63a010d88d2`. GitHub's latest stable release API
  identified this release; its Go module origin confirms the source commit.
  Default-branch HEAD was `55da25218f3606f2f8fb04d74f527903870805a4`;
  unreleased main was not selected.
- Apache ADBC Go 1.12.0 → 1.13.0; Arrow Go 18.8.0 → 18.9.0;
  gRPC 1.84.0 → 1.85.0; driverbase `4c82882827b5` → `809dff5b62b1`.
  Updated compression, JSON, regexp2, Prometheus helpers, x/* and generated
  gRPC API dependencies within their existing module major versions.
- Python ADBC Flight SQL/manager 1.12.0 → 1.13.0;
  PyArrow 26.0.0 → 26.0.1; pandas 3.1.0 → 3.1.1.
  Active inline script pins match requirements; quickstart now explicitly uses
  Python >=3.11, consistent with these packages' Requires-Python metadata.
- Go 1.27.3 → 1.27.4 in the minimum, Docker builder and Enterprise
  composition images. The first govulncheck run reported
  [GO-2026-6859](https://pkg.go.dev/vuln/GO-2026-6859) against Go 1.27.3;
  Go’s release JSON confirms 1.27.4 is stable and the advisory lists it as fixed.
- Already current: go-redis 9.23.0,
  Prometheus client 1.25.0, protobuf 1.36.12, OpenTelemetry 1.47.0,
  MkDocs 1.6.1, Polars 2.1.0, DuckDB 2.0.0, redis-py 8.1.0,
  psutil 7.2.2 and JDBC 19.0.0. Historical benchmark requirements and
  dated compatibility results remain unchanged.

## Custom-code review

The driver's v0.0.28 release fixes BindStream to retain its own reader reference,
matching the Go ADBC contract. Removed the gateway's transfer/nulling logic and
extra ingest Retain. Parameter-handle readers and executeUpdate inputs now release
the caller's reference unconditionally; ingest keeps the adapter's ownership.
This removes custom lifecycle bookkeeping and prevents leaks with the new driver.

Two inspected candidates still need their custom implementations:

- Arrow 18.9.0 `flightSqlServer.DoGet` still returns on WriteWithAppMetadata
  failure before releasing the current batch. Keep recordHandoffs and its
  real-adapter failed-send regression test.
- Arrow `util.TotalRecordSize` sums buffer lengths and deduplicates buffers.
  It cannot replace capacity-based recordMemory for the fallback budget without
  changing its conservative accounting. Keep recordBytes/dataBytes.

## Validation

- Added TestBindStreamOwnership with checked allocations for handle parameters,
  update parameters and ingestion, each on successful/refused binding. All six
  fail with leaks using the original sqlserver.go against the updated graph;
  all six pass with the fix.
- Python 3.12: uv resolved both active requirement sets together; imports and
  reported versions passed for ADBC Flight SQL/manager, PyArrow, pandas,
  Polars and DuckDB.
- Go 1.27.4 `govulncheck` (golang.org/x/vuln v1.10.0): no vulnerabilities found.
  Python `pip-audit` covering both active client requirement sets plus docs:
  no known vulnerabilities found.
- Python script unit tests (`python3 -m unittest discover -s scripts -p
  'test_*.py'`): all 15 passed.
- Both `golang:1.27.4-alpine` and `golang:1.27.4-bookworm` manifests exist,
  verified with `docker buildx imagetools inspect`.
- Initial vet caught a protobuf mutex copied by a test helper's value receiver;
  corrected it to a pointer receiver before final checks.
- Final Go 1.27.4 `env -u REDIS_URI -u REDIS_INTEGRATION_REQUIRED go test
  -count=1 -race ./...`: passed; backend integration tests intentionally skip.
- `go vet ./...`, `make build` (CGO_ENABLED=0), `go mod verify`,
  formatting (`gofmt -l .` empty), local Markdown link checks and
  `git diff --check`: passed.

The initial unit-only phase had no REDIS_URI configured. At the maintainer's
subsequent request, provisioned a dedicated Docker Desktop Redis Enterprise
instance for this PR, without reusing or modifying other chats' databases.
Its Redis endpoint is localhost:26390, management endpoint localhost:39443 and
public Flight endpoint grpc://localhost:28816. It uses the repository's existing
Redis Software image with a fresh volume and an isolated 10.227.88.0/24 network
(Docker's automatic address pool was exhausted). Generated credentials and CA
remain in ignored .local/enterprise files and are not committed.

The provisioning verification passed: three active primary shards, active 4x QPF,
verified TLS, ACL authentication, Search read/write and negative connection checks.
Bootstrap initially required retries while management authentication and shards
became ready. A Linux arm64 public Flight binary built from this PR runs in a
Go 1.27.4 container; its real SELECT 1 readiness check passed.

The integration runner uses the public/community source from this PR, Go 1.27.4,
a private CA trust bundle, the dedicated backend's generated data/management
credentials, REDIS_INTEGRATION_REQUIRED=1, a shared scope lock, serialized Go
packages and the race detector. No private source overlay is applied: available
private worktrees contain unrelated edits and are not qualification for this PR.
Full paid-feature/composition qualification and JDBC smoke remain unverified.
No Dockerfile image build was run; the live Flight container runs the separately
cross-compiled static binary. The full public/community race suite passed against this backend with
`go test -count=1 -p 1 -race ./...` (command package 10.300s, server 31.516s).
The quickstart passed, including 1,000 SQL rows, parameter binding, an UPDATE
readback, and a 10,000-row multi-batch Arrow dataset round trip. The dataframe
compatibility script passed with pandas 3.1.1, Polars 2.1.0 and DuckDB 2.0.0;
pandas still raises its documented NotImplementedError for ADBC params, and the
direct ADBC binding fallback passes.

The first dataset_to_sql run exposed an obsolete assertion that struct ingestion
must fail. Driver 0.0.28 supports typed stored structs. Updated the example to
assert successful nested import, exact values (including NULL/empty-list cases)
and Arrow nested types, followed by the existing scalar projection/readback.
Removed its obsolete exception handling and updated the workflow documentation.
The corrected example passed, including `actual.equals(source)` to check exact
Arrow types and values, followed by the scalar projection/readback. This is an
intentional compatibility expectation change backed by the live failure and
the native driver's stored_struct.go/types.go implementation.

The isolated Redis and Flight containers are left running for further testing.
This worktree's ignored `.local/enterprise/host.env` holds the host data URI and
CA path; `.local/enterprise/backend-env.py verify` rechecks backend readiness
without printing credentials. The local Compose file is
`.local/enterprise/compose.yaml`, with this checkout as its project directory.

Rollback: restore the dependency graph, inline/requirements pins and ownership
changes together. The release continues to use native SQL storage protocol 17;
this change adds no gateway storage migration. Native driver storage compatibility
and any deployment/rollback still require separate qualification.

Review: ownership success/refusal paths inspected; protocol routing, credentials,
tickets, publication and storage algorithms unchanged. No outstanding finding
from this focused review. Maintainer decision remains pending.
