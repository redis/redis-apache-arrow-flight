# Flight endpoint validation — October 29, 2026

Result: all executed checks passed.

Tested checkout: `b84b1e2a1693a0269dd503ae84f2a353dc724a39` (clean working tree at execution).
This records that revision only; subsequent main changes were not rerun.
The pinned Redis ADBC module resolves to v0.0.18, revision `2f053d3affad`.
Community build; Go 1.27.2 on macOS arm64; Redis 8.6.2.
Server built directly from this checkout, listening on localhost:18815.
Disposable standalone Redis: localhost:16379. Three-shard Cluster: localhost:17001–17003.

| Check | Result |
| --- | --- |
| Standalone: `REDIS_URI=redis://127.0.0.1:16379/0 go test -count=1 -race -json ./...` | 80 tests passed, 1 skipped; no race failures |
| Cluster: `REDIS_URI=redis://127.0.0.1:17001/0 go test -count=1 -race -json ./...` | 80 tests passed, 1 skipped; no race failures |
| Python quickstart | Passed: 1,000 SQL rows ingested, grouped/parameterized queries, update, schema/table discovery; 10,000-row nested/dictionary dataset round trip, projection, batch selection, listing and deletion |
| Python dataset-to-SQL | Passed: dataset equality, explicit rejection of unsupported Struct ingestion, scalar projection ingestion/query, NULL handling and cleanup |
| Arrow Flight SQL JDBC 19.0.0, Temurin Java 25 | All smoke checks passed: metadata, prepared INSERT/SELECT, aggregates, updates, concurrent reads/statements, session time zone and temporary-table persistence |
| Community metrics | HTTP 204, empty body, as documented |

Python clients ran in Python 3.14 containers with the repository's pinned requirements: ADBC Flight SQL/manager 1.12.0, PyArrow 26.0.0 and pandas 3.1.0.
The JDBC jar's SHA-1 matched Maven Central's published checksum: 300f1bc6d866aebe730d493605b82cf600cc1d3d.

## Limitations

- TestIntegrationStreamOverflowReject skipped in both suites because it requires the enterprise optimization implementation.
- JDBC 19 getTypeInfo returns an empty result: the smoke test explicitly records this known client limitation.
- DBeaver is not installed; no manual application validation was performed.
- No production load, deployment failover, TLS/authentication or Superset-over-Flight acceptance is claimed.

Cleanup: temporary Flight server and all four validation containers removed. Existing Redis container on port 6379 was left untouched. The validation run itself changed no repository files; this PR adds its records.

## Evidence

- [Standalone race suite](standalone.jsonl)
- [Cluster race suite](cluster.jsonl)
- [Python dependency installation](python-install.log)
- [Python quickstart](python-quickstart.log)
- [Dataset-to-SQL workflow](python-dataset-to-sql.log)
- [JDBC smoke test](jdbc.log)
- [Flight server log](server.log)

## Reproduction

Use a disposable Redis with Search. Start the built community server against
that Redis and run the pinned clients against its Flight port:

```sh
make build
./bin/redis-flight-server --redis-uri redis://127.0.0.1:16379/0 \
  --listen localhost:18815 --metrics-listen localhost:19815
```

In a separate shell with Python 3.14 and Java 25 available:

```sh
python -m pip install -r examples/python/requirements-smoke.txt
python examples/python/quickstart.py --uri grpc://localhost:18815
python examples/python/dataset_to_sql.py --uri grpc://localhost:18815
make jdbc-smoke JDBC_URL='jdbc:arrow-flight-sql://localhost:18815/?useEncryption=false'
REDIS_URI=redis://127.0.0.1:16379/0 go test -count=1 -race ./...
# Requires a disposable three-shard Redis Cluster on ports 17001–17003:
REDIS_URI=redis://127.0.0.1:17001/0 go test -count=1 -race ./...
```

The recorded run used Linux arm64 containers for Python and Java, reaching the
macOS server via `host.docker.internal:18815`. The Go suites ran on macOS and
started their own in-process Flight endpoints against each specified backend.
The community metrics check was `curl` against `http://127.0.0.1:19815/metrics`;
it returned HTTP 204 with a zero-byte body.
