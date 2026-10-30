# Testing and agent evaluation

Status: existing checks described from source; proposed agent evals are not installed.
Evidence: [Makefile](../../Makefile), [test workflow](../../.github/workflows/test.yml), and test files below.

## Local verification

Use Go as pinned by [go.mod](../../go.mod). Integration tests create and delete tables, keys, and ACL users named `it_flight_*`; examples also mutate data. Use a disposable Redis with Search, not production.

| Check | Command | Prerequisites and success signal |
| --- | --- | --- |
| Unit tests | `env -u REDIS_URI make test` | Go and dependencies available; exit 0. Redis-dependent tests intentionally skip |
| Formatting | `gofmt -l .` | Empty output |
| Static analysis | `go vet ./...` | Exit 0 |
| Static binary | `make build` | Exit 0; output `bin/redis-flight-server` |
| Standalone integration/race | `make test-integration` | Docker Compose; starts Redis on `REDIS_PORT` (6379 default); exit 0 |
| Cluster integration/race | `make test-cluster` | Docker Compose; ports 7001–7003 available; exit 0 |
| Existing disposable backend | `REDIS_URI=redis://localhost:6379/0 go test -count=1 -race ./...` | Search-enabled Redis already running; integration tests must not skip |
| Python client | `make example` | Redis and server running; uv installs script dependencies; exit 0 |
| DBeaver manual checks | [Setup, fixture and expected results](../dbeaver.md) | Running disposable endpoint, Generic driver and JDBC 19.0.0; record connection, navigator, columns, Data tab and queries separately |
| JDBC client | `make jdbc-smoke` | Redis and server running; Java and network for initial driver download; exit 0 |
| Dataframe / DuckDB clients | `make clients-smoke` | Disposable server and uv; pinned pandas, Polars, DuckDB and Flight dependencies; asserts results and reports pandas's ADBC `params` limitation |
| R Flight SQL client | `make r-smoke FLIGHT_SQL_DRIVER=/path/to/library` | R, adbcdrivermanager, nanoarrow and an OS/architecture-matched Flight SQL shared library; exit 0 |
| Dataset benchmarks | `REDIS_URI=redis://localhost:6379/0 go test ./server -run '^$' -bench Dataset -benchtime 10x` | Disposable Redis; inspect reported timings/allocations, no pass/fail performance threshold exists |

`make test-integration` starts local Compose Redis even when overriding `REDIS_URI`; the direct Go command avoids that when using another disposable backend. Do not count the unit-only invocation as evidence for backend behavior.

For client checks, first start `docker compose up --detach --build flight`; it starts its Redis dependency too. Wait for a real SQL round trip with `scripts/wait-flight.py`; community has no backend health probe. `make jdbc-smoke` defaults to JDBC driver 19.0.0 and adds the JVM memory options for the Java version. CI runs it separately on Java 17, 21, and 25. No `make lint` target exists.

## Behavior-to-test map

| Contract | Existing source evidence |
| --- | --- |
| Writes occur once when returned tickets are read repeatedly | `TestIntegrationWritesRunOnce` in [integration_test.go](../../server/integration_test.go) |
| Prepared parameters/types and ingestion | `TestIntegrationPrepared`, `TestIntegrationPreparedTypes`, `TestIntegrationIngest` in integration_test.go |
| Temporary state does not leak through pooling | `TestIntegrationTempObjectsDontLeak` in integration_test.go |
| Atomic upload outcome, ordering, persistent generation sequence | `TestIntegrationUploadCommitOutcome`, `TestIntegrationUploadOrder`, `TestIntegrationDatasetTTLKeepsSeq` in integration_test.go |
| Projection, batch ranges, layouts, paging | `TestIntegrationDatasetReads`, `TestIntegrationDatasetLayouts`, `TestIntegrationDatasetReadPages` in integration_test.go |
| Redis ACL and changed credentials | `TestIntegrationRedisACL`, `TestIntegrationStaleCredentials` in integration_test.go |
| Sessions, ownership, settings | `TestIntegrationSessions`, `TestIntegrationSessionOwner`, `TestIntegrationSessionSettings` in [session_integration_test.go](../../server/session_integration_test.go) |
| Streaming, fallback, cleanup | `TestIntegrationStreamResults` in [stream_integration_test.go](../../server/stream_integration_test.go); budget/token/panic tests in [unit_test.go](../../server/unit_test.go) |
| Health auth exception and draining | `TestHealthCheckSkipsAuthAndSessions`, `TestHealthWatchDoesNotHoldShutdown` in [health_test.go](../../server/health_test.go) |
| Shutdown and command behavior | `TestShutdownDrainsThenCancels` in unit_test.go; `TestSIGTERM`, `TestIntegrationSIGTERM` in [main_test.go](../../cmd/redis-flight-server/main_test.go) |
| Metrics | [metrics_test.go](../../server/metrics_test.go) |

This map identifies coverage to inspect; it does not assert all branches are covered or that tests passed in this session.

## CI and completion evidence

The test workflow checks formatting/vet, standalone and cluster race tests, a statically linked binary, Python quickstart against the container, metrics output, and JDBC smoke tests. Workflow configuration is not historical evidence of green runs.

For a code change, run the relevant local checks and retain CI links/results in its PR. For a regression, reproduce the failure before fixing it where practical; do not weaken existing assertions to manufacture a pass. Explain intentional test contract changes. For documentation-only changes, check file links, source references, and whitespace; no Redis startup is necessary.

## Proposed evaluation cases for agent instructions

These are candidate tasks for a future runner, not executable evals or measured scores. Run in isolated disposable checkouts with synthetic data and record the instruction/model version and source SHA.

| Task prompt | Expected evidence |
| --- | --- |
| Explain why reading a write ticket twice must not insert twice | Identifies execution timing and the write-once regression test |
| Plan a dataset storage change | Includes LIST/HASH compatibility, cluster hash tags, ordering, seq persistence, and mixed-version rollout |
| Diagnose `RESOURCE_EXHAUSTED` | Distinguishes pool/stream pressure, cursor limits, and fallback memory; does not claim the budget caps all memory |
| Review a session routing change | Checks owner identity, process-local state, setting restoration, temporary objects, and client cookies |
| Add a client compatibility statement | Requests actual client evidence; preserves untested BI-tool caveats |
| Propose a metrics or auth change | Preserves health's intentional exception, avoids credentials/high-cardinality labels, identifies public metrics exposure |

A future maintainer must choose scoring, budget, safe tool access, representative task fixtures, and gating thresholds. Do not enable paid model runs or assume a service integration from this document.
