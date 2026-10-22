# Extracted project context

Extraction date: 2026-10-20. Baseline: `27bbc3e63ed1fdfe22c7018cc723b50f1681ed50`.
Status: source-derived baseline; runtime tests and production behavior have not been established by this document.

## Purpose and consumers

The Go service provides one gRPC endpoint for SQL over Redis and named Arrow table transfers. SQL suits indexed filters, aggregates, updates, metadata, and ingestion. Datasets suit dataframe/feature snapshots and nested Arrow types, with column and batch selection rather than SQL row predicates.

The requester clarified on 2026-10-20 that the project addresses the difficulty of streaming large, many-row Redis result sets to clients. [Project motivation in intent.md](../../intent.md#project-motivation) records that goal and its external background separately from this source-derived baseline. Comparative throughput, latency, and memory targets have not yet been agreed or measured here.

Evidence: [README interface comparison](../../README.md#choose-an-interface), [router](../../server/router.go), [configuration](../../server/config.go).

The repository exercises Python ADBC/PyArrow, Go clients, and the Flight SQL JDBC driver. README identifies Superset, Power BI, and Tableau as options requiring compatibility work, not tested integrations. Dataframe, DuckDB, R, and Redis Insight descriptions are also not integration-test coverage. See [client examples](../../examples/python/quickstart.py), [JDBC smoke](../../examples/jdbc/JdbcSmoke.java), and [tool caveats](../../README.md#data-tools).

## Dependencies and local topology

| Component | Checked-in value/source |
| --- | --- |
| Go | `go 1.26.10` in [go.mod](../../go.mod) |
| Arrow Go / ADBC | `v18.8.0` / `v1.12.0` in go.mod |
| Redis driver | Module path `github.com/adbc-drivers/redis/go`, replaced by `github.com/alberttwong/redis-adbc-driver/go`, pseudo-version `v0.0.0-20261022161448-9cc8e25c0f01` (v0.0.17 release, upstream checked 2026-10-22) |
| go-redis / gRPC / Prometheus client | `v9.22.0` / `v1.83.2` / `v1.24.1` in go.mod |
| Test Redis | `redis:8.6.2` standalone and three-node cluster in [compose.yaml](../../compose.yaml) |
| Container | Go build stage, static binary, distroless nonroot runtime in [Dockerfile](../../Dockerfile) |
| JDBC smoke | Driver `19.0.0`; CI runs Java 17, 21, 25 |

These are repository pins, not recommendations for latest versions. SQL requires the Query Engine; datasets can use Redis without Search. Compose binds host ports to loopback: Redis 6379, cluster 7001–7003, Flight 8815, metrics 9815 by default. Containers use their own service addresses. No production topology is declared.

## Request and resource flow

1. [Command](../../cmd/redis-flight-server/main.go) builds `server.Config` from flags/environment and handles termination.
2. [Server](../../server/server.go) configures TLS and middleware, then registers the router and gRPC health service.
3. [Authentication](../../server/auth.go) optionally validates Redis ACL credentials and seals bearer identity/expiry using AES-256-GCM. Encrypted token contents include the Redis password.
4. [Tenants](../../server/tenant.go) acquire identity-specific ADBC databases, pools, and go-redis clients. [Sessions](../../server/session.go) bind cookie state and dedicated connections to their creator; these sessions are process-local.
5. [Router](../../server/router.go) sends PATH descriptors, JSON dataset tickets, and `drop_dataset` to [datasets](../../server/datasets.go). Flight SQL commands/tickets go through [SQL](../../server/sqlserver.go) and [metadata](../../server/metadata.go).
6. [Pool](../../server/pool.go), [cursor checks](../../server/cursors.go), and [IPC](../../server/ipc.go) handle resource limits and Arrow ownership. [Errors](../../server/errors.go) map backend failures to gRPC status.
7. [Health](../../server/health.go) polls Redis independently; [metrics](../../server/metrics.go) reports calls and resource state. Shutdown drains calls before closing sessions and tenants.

## Data contracts

**SQL:** Redis ADBC owns the SQL engine and table layout (row HASHes and Search indexes). Read-only SELECT/SHOW statements execute when their tickets are consumed. Other statements execute while FlightInfo is requested; their results travel inline or through a Redis result key. Preserve the existing write-once behavior when a returned ticket is replayed; this is not a promise of general exactly-once semantics across retried client requests. Prepared clients must use the handle returned after binding. Transactions, savepoints and Substrait are unsupported. GetXdbcTypeInfo returns canonical scalar metadata. Explicit cancellation uses leased Redis query records; transport cancellation also cleans up calls.

**Datasets:** A name is joined PATH segments, URL-path-escaped inside Redis keys. With the default prefix:

| Key | Type and contract |
| --- | --- |
| `arrowflight:ds:{<name>}` | HASH of current generation, schema, rows, batches, bytes, updated time |
| `arrowflight:ds:{<name>}:g<N>` | HASH layout `2`: schema, batch count, `<b>:rows` and `<b>:<c>` column IPC payloads |
| Same generation key in legacy data | LIST of schema followed by batch IPC; still readable by current code |
| `arrowflight:ds:{<name>}:seq` | Persistent monotonic sequence; survives expiry and drop |
| `arrowflight:query:{<id>}` | STRING with owner digest, lifecycle state and active-read count; atomic writes with expiry, renewed by active readers |
| `arrowflight:result:{<id>}` | Oversized SQL statement result; expires after 10 minutes |

All keys involved in one dataset commit/drop share a cluster hash tag. Uploads publish via Lua only after the generation is complete. The later-started generation wins even if an older upload finishes last. Replaced generations have a five-minute grace period; abandoned uploads expire after one hour. A drop invalidates reads even for surviving old-generation payloads. Do not delete sequence keys as routine dataset cleanup.

Dataset tickets select generation, ordered column names, and a half-open batch range. With endpoint splitting enabled, consumers must read all endpoints. Approximate chunk size is 2 MiB; dataset read/write pages target 8 MiB and at most 64 batches. These values are not hard memory bounds for an oversized row/batch. Current readers handle LIST and HASH; old LIST-only readers cannot read new HASH generations.

Evidence: [dataset code](../../server/datasets.go), [IPC code](../../server/ipc.go), [SQL code](../../server/sqlserver.go), [feature reference](../../README.md#feature-reference).

## Selected defaults and boundaries

| Setting | Default | Implication |
| --- | --- | --- |
| Flight listener | `localhost:8815` | Container overrides to `0.0.0.0:8815` |
| Authentication / TLS | `none` / off | Shared Redis URI identity unless configured otherwise |
| Token TTL | 12h | Empty secret uses a random process key |
| Pool idle/open per identity | 8 / 32 | Pool capacity differs from dedicated session connections |
| Streams per identity / table | 8 / 32 | Identity streams are also capped at pool-open minus one |
| Fallback result budget | 1 GiB | Applies to eligible scans buffered because streams are unavailable; not all query/process memory |
| Stream stall timeout | 2m | Releases slow-consumer resources |
| Tenant / session idle timeout | 10m / 10m | Idle resource reclamation |
| Dataset TTL / endpoint splitting | 0 / 0 | Retain until drop; one endpoint |
| Maximum received gRPC message | 64 MiB | Distinct from outgoing chunk target |
| Shutdown timeout | 25s | Compose grants 30s |
| Metrics listener | Off | When enabled, plain HTTP `/metrics`, no authentication |

Standalone cursor-pressure detection uses `FT.INFO`; cluster does not provide the same count. Index cursor limits are shared across processes and other clients. A per-server table limit is not a global cluster guarantee.

Evidence: [Config defaults/validation](../../server/config.go), [CLI flags](../../cmd/redis-flight-server/main.go), [session limits](../../server/session.go), [Compose](../../compose.yaml).

## Unknowns

No production traffic samples, benchmark results, customer requirements, deployment inventory, incident records, alert thresholds, capacity baseline, named service owner, or approved availability/latency targets were supplied. GitHub-hosted settings and historical workflow results were not inspected. The checked-in benchmark fixture is synthetic (100,000 rows, 10 columns), not production data or measured performance.
