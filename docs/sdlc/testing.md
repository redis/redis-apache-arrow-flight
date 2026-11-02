# Testing and agent evaluation

Status: existing checks described from source; proposed agent evals are not installed.
Evidence: [Makefile](../../Makefile), [test workflow](../../.github/workflows/test.yml), and test files below.

## Local verification

For a provisioned local TLS/ACL cluster and containerized enterprise runner, see
[the Redis Enterprise environment](../redis-enterprise-testing.md).

Use Go as pinned by [go.mod](../../go.mod). Integration tests create and delete tables, keys, and ACL users named `it_flight_*`; examples also mutate data. Use a disposable Redis Enterprise cluster database with Search, verified TLS, and a nonempty username and password. Use the qualified enterprise Flight composition: community explicitly rejects these security features.

| Check | Command | Prerequisites and success signal |
| --- | --- | --- |
| Unit tests | `env -u REDIS_URI make test` | Go and dependencies available; exit 0. Redis-dependent tests intentionally skip |
| Formatting | `gofmt -l .` | Empty output |
| Static analysis | `go vet ./...` | Exit 0 |
| Static binary | `make build` | Exit 0; output `bin/redis-flight-server` |
| Enterprise integration/race | `make test-integration` | Qualified enterprise source composition; exported `REDIS_URI=rediss://USERNAME:PASSWORD@HOST:PORT/0`; exit 0 with backend tests executed |
| Compatibility alias | `make test-cluster` | Same secured Enterprise endpoint and suite as `make test-integration`; no local Redis startup |
| Python client | `make example` | Redis and server running; uv installs script dependencies; exit 0 |
| DBeaver manual checks | [Setup, fixture and expected results](../dbeaver.md) | Running disposable endpoint, Generic driver and JDBC 19.0.0; record connection, navigator, columns, Data tab and queries separately |
| JDBC client | `make jdbc-smoke` | Redis and server running; Java and network for initial driver download; exit 0 |
| Dataframe / DuckDB clients | `make clients-smoke` | Disposable server and uv; pinned pandas, Polars, DuckDB and Flight dependencies; asserts results and reports pandas's ADBC `params` limitation |
| R Flight SQL client | `make r-smoke FLIGHT_SQL_DRIVER=/path/to/library` | R, adbcdrivermanager, nanoarrow and an OS/architecture-matched Flight SQL shared library; exit 0 |
| Dataset benchmarks | `go test ./server -run '^$' -bench Dataset -benchtime 10x` | Same enterprise composition and exported secured `REDIS_URI`; inspect reported timings/allocations, no pass/fail performance threshold exists |

`make test` always unsets the backend variables so it runs unit checks only.
`make test-integration` requires `REDIS_URI` and never starts Compose. Direct
`go test` skips backend tests when the URI is absent; set
`REDIS_INTEGRATION_REQUIRED=1` in integration automation to make absence fatal.
The shared test helper rejects plaintext URIs, missing usernames/passwords,
and invalid TLS connection options without printing the URI. Provisioning must
establish that the endpoint belongs to Redis Enterprise: an Enterprise proxy
does not need to expose the OSS Redis Cluster API. A successful `PING` alone
cannot establish edition, shard count, or active 4x QPF qualification.

Export the URI through your secret manager or a hidden shell prompt, with URI
percent-encoding for special characters in credentials. Do not put credentials
in command arguments or checked-in files. The cluster's certificate must chain
to a trusted CA in the test host and server container; install a private CA in
those trust stores when needed. Do not disable certificate verification.
The test identity needs the commands exercised by the suite, including Search,
SQL data mutation and key cleanup. Enterprise ACL fixtures need Software
management API access; the current full suite still uses unsupported Redis ACL
mutation commands, as recorded in the
[local validation results](../validation/2026-11-01-enterprise-tls/README.md).
Use a dedicated disposable database and avoid concurrent suites sharing
fixed fixture prefixes.

All Python, JDBC, R, BI-tool and benchmark validation must use a Flight server
built from the qualified enterprise composition and connected to this same
secured Redis Enterprise backend. Start that server using the composition's
build runner and exported `REDIS_URI`, then wait for a real SQL round trip with
`scripts/wait-flight.py`. Local community Compose is a development convenience;
it is not an accepted integration-test environment. `make jdbc-smoke` defaults
to JDBC driver 19.0.0 and adds memory options for the selected Java version;
enterprise CI should run it for Java 17, 21 and 25. No `make lint` target exists.

### Recommended private CI setup

Run enterprise integration in a private workflow alongside the separately
supplied source composition. Check out the exact public commit under test,
qualify its full-module fingerprint, and execute `make test-integration` through
the composition runner. Ensure its Go overlay also reaches command subprocesses.
Then build the enterprise server from that same composition and run Python,
JDBC, and other client checks against it. Preserve read/write assertions and
record the public commit, composition revision and actual results.

Use a protected GitHub Environment named `redis-enterprise-integration`. Store
`REDIS_URI` as an environment secret containing the authenticated `rediss://`
endpoint. Grant only trusted workflow runs access to both the private composition
and this environment. Do not expose them to untrusted fork PRs or use
`pull_request_target` to execute untrusted changes with credentials. Use a runner
with network access to the cluster and the appropriate CA trust store.

Public CI runs formatting, vet, community unit tests and a static build. It no
longer runs plaintext standalone/OSS Cluster or client integration tests.
Enterprise workflow provisioning, its check publication, and a required release
integration gate must be configured by the repository owner. This public checkout
does not include the private source composition or cluster credentials; local
setup uses the separately supplied qualified pair. Until that is
wired, a green public test/release workflow is **not** enterprise integration
qualification. Release artifact checks only exercise version/help offline.

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

Integration validation must cover both reads and writes unless the requester
explicitly scopes an integration to reads. Tableau acceptance is read-only:
validate discovery and application read queries; write support is not required.
Record the actual read coverage and untested features. For integrations with
write coverage, verify writes by reading back the resulting values, using the application's supported write
path. Record unsupported write paths and application/environment blockers
separately; driver-only success does not establish application compatibility.
See the [JDBC/Metabase read-write validation](../validation/2026-11-01-bi-jdbc/README.md).

The public test workflow checks formatting/vet, community unit tests, and a statically linked binary. Enterprise integration and client evidence must come from the private qualification workflow described above. Workflow configuration is not historical evidence of green runs.

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
