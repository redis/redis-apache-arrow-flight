<img referrerpolicy="no-referrer-when-downgrade" src="https://static.scarf.sh/a.png?x-pxid=a5e3e093-ccb6-4dd2-991a-0a75ab78f997" />

# Redis Arrow Flight server

Build profiles: this checkout builds **community** with paid-feature stubs.
Use an enterprise composition for TLS/authentication, monitoring, tuning and
automation. See [build profiles](docs/build-profiles.md) for exact availability.

A gRPC [Apache Arrow Flight](https://arrow.apache.org/docs/format/Flight.html)
and [Flight SQL](https://arrow.apache.org/docs/format/FlightSql.html) server
for Redis, written in Go. One endpoint serves two ways to move Arrow data in
and out of Redis:

| | **Flight SQL**, backed by the [Redis ADBC driver](https://github.com/redis/redis-adbc-driver) | **Flight datasets**, stored as Arrow IPC |
|-|-|-|
| Requests | Flight SQL commands (`CMD` descriptors) | `PATH` descriptors, such as `["sales", "2026-10"]` |
| What you do | SQL: filters, `GROUP BY`, joins, windows, `INSERT` / `UPDATE` / `DELETE`, DDL, bulk ingest, catalog metadata | Upload a whole table by name, download it, list and drop |
| Storage in Redis | One HASH per row, plus a RediSearch index per table, so other Redis clients can read it too | Arrow IPC streams encoded per column and batch |
| Arrow types | The SQL types (integers, floats, decimals, strings, binary, dates, times, timestamps, intervals) | Any type, including lists, structs, maps and dictionaries |
| Redis needed | The Query Engine (Redis 8, Redis Stack, Redis Cloud / Software with Search). Redis Flex is refused | Any Redis, including Flex and Cluster |
| Clients | ADBC Flight SQL driver (Python, Go, R, …), the Arrow Flight SQL JDBC driver | Any Flight client (`pyarrow.flight`, arrow-go, Arrow Java, …) |

## Quick start

You need Go 1.26.10 or newer, Docker and, for the Python example,
[uv](https://docs.astral.sh/uv/). For tagged builds, see [Install](#install).

```bash
docker compose up --detach --wait redis
```

```bash
make run
```

The server listens on `localhost:8815`. In another terminal, run the Python
tour (SQL with the ADBC Flight SQL driver, then datasets with
`pyarrow.flight`):

```bash
uv run examples/python/quickstart.py
```

To run Redis and the server together in Docker instead:

```bash
docker compose up --detach --build
```

Compose waits for Redis health before starting Flight. It does not establish
Flight readiness; do not use `--wait` with the community service, which has no
backend healthcheck.
Community health reports `UNKNOWN`; verify application readiness with a real SQL
query, as `scripts/wait-flight.py` does in CI. See [build profiles](docs/build-profiles.md).

### Install

Source builds download the pinned [public Redis ADBC driver](https://github.com/redis/redis-adbc-driver).
No GitHub token or private dependency secret is required for local, Docker,
or GitHub Actions builds. Run `make build` for a local binary, or `make docker`
for a container image. Public module downloads use Go checksum verification
and the checked-in `go.sum`.

The [release workflow](.github/workflows/release.yml) is configured to publish
static binaries for Linux (amd64, arm64) and macOS (arm64), with SHA-256
checksums, and a multi-arch image on the GitHub Container Registry. To use a
published [release](https://github.com/redis/redis-apache-arrow-flight/releases),
set `VERSION` to its tag and use these commands. If the release list is empty,
build from source with the [quick start](#quick-start).

```bash
curl -fsSL --remote-name-all https://github.com/redis/redis-apache-arrow-flight/releases/download/$VERSION/redis-flight-server-$VERSION-linux-amd64.tar.gz{,.sha256}
```

```bash
shasum -a 256 -c redis-flight-server-$VERSION-linux-amd64.tar.gz.sha256 && tar -xzf redis-flight-server-$VERSION-linux-amd64.tar.gz
```

```bash
docker run --rm -p 8815:8815 -e REDIS_URI=redis://host.docker.internal:6379/0 ghcr.io/redis/redis-apache-arrow-flight:${VERSION#v}
```

This container example uses Docker Desktop's `host.docker.internal` to reach
Redis on the host. With Docker Engine on Linux, put the image and Redis on a
shared Docker network and set `REDIS_URI` to Redis's name on that network.

Image tags are the version without the `v` (`1.2.3`), its minor version
(`1.2`) and `latest`; a prerelease (`1.2.3-rc1`) gets only its own.

## Contents

- [Choose an interface](#choose-an-interface) compares Flight SQL and datasets.
- [Integration paths](#integration-paths) explains ADBC, JDBC and native Flight clients, with tested coverage.
- [Data tools](#data-tools) lists compatible query, dataframe and inspection tools.
- [Client examples](#client-examples) covers Python, JDBC and Go.
- [Feature reference](#feature-reference) documents SQL and datasets.
- [Sessions](docs/server-guide.md#sessions) explains cookies, settings and temporary objects.
- [Deployment and operations](docs/server-guide.md#deployment-and-operations) covers security,
  configuration, health checks, metrics and shutdown.
- [Architecture](docs/server-guide.md#architecture) explains routing, storage and request flow.
- [Development](docs/server-guide.md#development) describes the code layout and test targets.
- [SDLC documentation](docs/sdlc/README.md) links project context, intent,
  specification, plan, review guidance, testing, and operations records.

See the [protocol capability matrix](docs/flight-capabilities.md) for supported commands, error behavior, and optional features.

## Choose an interface

Use **Flight SQL** when you want the data to stay *queryable* in Redis: to
filter, aggregate, update rows, or let other applications read the HASHes.
Use **datasets** to *park and fetch whole tables* (dataframes, feature
snapshots, query results, ML batches) as fast as possible, or when your
data has types that SQL tables can't hold.

Datasets avoid converting Arrow values to SQL rows. A dataset read can pick
columns (and fetch only those from Redis) and a range of batches, but only SQL
can filter rows by value. Both can be combined: for example, upload a dataset,
then bulk-ingest the rows you want to query into a SQL table. See the
[executable dataset-to-SQL workflow](docs/dataset-sql-workflow.md).

### Integration paths

The server supports three usual client integration paths through the same
gRPC endpoint:

| Integration path | Use it for | Client examples |
|-|-|-|
| **Flight SQL via ADBC** | SQL queries, parameters, catalog metadata and bulk ingestion with Arrow results | Python `adbc_driver_flightsql`; R through ADBC |
| **Flight SQL via JDBC** | SQL access from Java applications and tools that accept a JDBC driver | Arrow Flight SQL JDBC driver; DBeaver setup below |
| **Native Flight RPC** | Upload/download named Arrow datasets, discover datasets and invoke dataset actions | Python `pyarrow.flight`, Go Flight clients; other Arrow Flight SDKs |

These are client paths to two server interfaces: Flight SQL and Flight RPC
datasets. **gRPC is the underlying transport**, rather than another client
integration path. Flight SQL adds standardized SQL commands on top of Flight
RPC; ADBC and JDBC expose those commands through familiar database APIs.
Native Flight SDKs can also issue Flight SQL commands when they provide a
Flight SQL client, as the Go tests do.

#### Tested integration coverage

The table below summarizes recorded validation, not a guarantee that every
application using a driver has been tested. Versions, validation dates and
known limitations are tracked in the [client validation matrix](docs/client-compatibility.md).

| Integration path | Tested client | Recorded passing coverage | Evidence |
|-|-|-|-|
| Flight SQL via ADBC | Python ADBC Flight SQL driver/manager 1.12.0 | Metadata, parameter binding, queries/updates, bulk ingestion and dataset-to-SQL import | [Python tour](examples/python/quickstart.py), [dataset-to-SQL example](examples/python/dataset_to_sql.py) |
| Flight SQL via JDBC | Flight SQL JDBC 19.0.0, Java 25 | Metadata, prepared parameters, updates, session state and concurrent statement/result reads | [JDBC smoke test](examples/jdbc/JdbcSmoke.java); CI is configured for Java 17/21/25, but the recorded local run covers Java 25 |
| Native Flight RPC | Python PyArrow 26.0.0 | Dataset upload/download, nested and dictionary types, multiple batches, column/batch selection, discovery and drop | [Python tour](examples/python/quickstart.py), [dataset-to-SQL example](examples/python/dataset_to_sql.py) |
| Native Flight RPC / Flight SQL SDK | Go arrow-go 18.8.0 | Dataset transfers, metadata, prepared parameters, ingestion, scans, sessions, cancellation, errors and cleanup; standalone/Cluster race suites | [Go integration tests](server/integration_test.go) and related server test files |

The initial client validation was recorded on October 20, 2026. Go and both
Python examples passed again with Redis ADBC v0.0.17 on October 22; JDBC was
not rerun for that driver update. DBeaver, Tableau, Power BI, R and other
listed application workflows have not been individually validated. The pandas
check covers conversion of a query result to a dataframe.

SQL requires autocommit; transactions are unsupported. Native dataset access
supports `DoPut` / `DoGet`; bidirectional `DoExchange` is unimplemented. See the
[protocol capability matrix](docs/flight-capabilities.md) for the full scope.

To measure dataset upload, full-read and one-column read performance on your
Redis, run the [100,000-row, 10-column benchmarks](server/dataset_bench_test.go):

```bash
REDIS_URI=redis://localhost:6379/0 go test ./server -run '^$' -bench Dataset -benchtime 10x
```

## Data tools

See the [versioned client validation matrix](docs/client-compatibility.md) for
checks actually run and known client limitations.

The mirrored [ADBC support across 22 data engineering tools](docs/data-engineering-adbc-support.md)
compares Redis ADBC, Flight SQL, and connector options, with upstream validation evidence.

Use a **Flight SQL driver** to query SQL tables, or a **Flight client** to
upload and download named datasets. Tools that consume Arrow tables can
work with the results of either interface.

| Tool | How to use it with this project | Useful for | Tested |
|-|-|-|-|
| **DBeaver** | Configure the [Arrow Flight SQL JDBC driver](https://arrow.apache.org/java/current/flight_sql_jdbc_driver.html) with the URL below; see [JDBC setup](#jdbc-dbeaver-and-others) for JVM options, authentication and sessions | Interactive SQL, browsing schemas and tables, viewing query results | No — application not tested; underlying [JDBC driver smoke test](examples/jdbc/JdbcSmoke.java) passed. |
| **Apache Superset** | Superset [requires a DB-API driver and SQLAlchemy dialect](https://superset.apache.org/user-docs/6.0.0/configuration/databases/). A candidate adapter is the third-party, beta [`sqlalchemy-adbc[flightsql]`](https://pypi.org/project/sqlalchemy-adbc/), using `adbc+flightsql://localhost:8815`; validate autocommit, metadata and generated SQL with this server | SQL exploration, charts and dashboards; requires adapter setup and compatibility testing | No — no Superset integration test recorded for this server. |
| **Power BI** | Build a [custom ADBC-based Power Query connector](https://learn.microsoft.com/en-us/power-query/adbc) using the Flight SQL driver and server URI below; adapt the sample's type mappings, navigation queries and SQL generation to this server. This project does not ship a Power BI connector | Reports and dashboards; requires custom connector development and compatibility testing | No — custom connector and application integration untested. |
| **Tableau** | Try [Other Databases (JDBC)](https://help.tableau.com/current/pro/desktop/en-gb/examples_otherdatabases_jdbc.htm) with the Arrow Flight SQL JDBC driver and URL below; configure the [driver's JVM options](#jdbc-dbeaver-and-others) and validate metadata and generated SQL. Tableau's generic JDBC connector does not guarantee feature compatibility | Visual analysis and dashboards; requires driver setup and compatibility testing | No — application integration untested. |
| **Other JDBC applications** | Load `org.apache.arrow:flight-sql-jdbc-driver` and use the same JDBC URL, if the application supports custom JDBC drivers | Java applications and data tools that allow a custom driver | Driver only — Flight SQL JDBC 19.0.0 passed on Java 25; individual applications untested ([record](docs/client-compatibility.md)). |
| **Python / PyArrow** | Use [ADBC Flight SQL](https://arrow.apache.org/adbc/current/driver/flight_sql.html) (`adbc_driver_flightsql`) for SQL and [`pyarrow.flight`](https://arrow.apache.org/docs/python/flight.html) for datasets; see [Client examples](#client-examples) | Queries, bulk ingest, Arrow table transfers and ML batches | Yes — ADBC Flight SQL and PyArrow Flight smoke scripts passed ([record](intent/driver-v0.0.17/plan.md#validation)). |
| **pandas** | Pass an ADBC Flight SQL connection to [`pd.read_sql`](https://pandas.pydata.org/docs/reference/api/pandas.read_sql.html) (pandas 2.2+), or convert a downloaded Arrow table with `table.to_pandas()` | Dataframe analysis and reporting | Partial — Arrow-to-pandas conversion passed; `pd.read_sql` untested ([record](intent/driver-v0.0.17/plan.md#validation)). |
| **Polars** | Pass an ADBC Flight SQL connection to [`pl.read_database`](https://docs.pola.rs/api/python/stable/reference/api/polars.read_database.html), or convert a downloaded Arrow table with `pl.from_arrow(table)` | Dataframe transformations and analytics | No — no integration test recorded. |
| **DuckDB** | Fetch an Arrow table through ADBC or Flight, then [query it locally in DuckDB](https://duckdb.org/docs/current/guides/python/import_arrow); this uses an Arrow transfer rather than a direct DuckDB connection to the server | Joining Redis results with local files or other Arrow data | No — no integration test recorded. |
| **R** | Use the [`adbcflightsql` driver](https://arrow.apache.org/adbc/current/driver/flight_sql.html) through the R ADBC packages, setting the server URI below | SQL queries and Arrow-based analysis in R | No — no integration test recorded. |
| **Go / Arrow Java** | Use the Arrow Flight SQL client for SQL, or the Flight client for dataset `DoPut` / `DoGet`; see [Other clients](#other-clients) and the [Flight API](https://arrow.apache.org/docs/format/Flight.html) | Applications and data pipelines | Partial — Go Flight SQL / Flight integration suites passed; Java coverage is JDBC only ([record](docs/client-compatibility.md)). |
| **Redis Insight** | [Connect directly to the backing Redis database](https://redis.io/docs/latest/develop/tools/insight/), rather than the Flight endpoint | Inspecting SQL row HASHes, search indexes and Redis memory usage; dataset payloads remain encoded Arrow IPC | No — no application validation recorded. |

Local connection addresses (without TLS):

```text
Flight / ADBC: grpc://localhost:8815
JDBC:          jdbc:arrow-flight-sql://localhost:8815/?useEncryption=false
```

See [tested integration coverage](#tested-integration-coverage) for the ADBC,
JDBC and native Flight checks. Tested status reflects recorded validation
against this Flight server; the linked
records give versions, dates, and scope. Tests of the Redis ADBC driver in the
mirrored comparison do not establish Flight server integration coverage. No
integration suites were rerun for this documentation update.

For SQL, use autocommit: transactions are not supported. A tool's PostgreSQL
or generic Redis connector does not connect to the Flight endpoint; select
Flight SQL or install its driver. SQL tools see SQL tables, while named
datasets are accessed through Flight's dataset API.

## Client examples

### Flight SQL from Python (ADBC)

```python
import adbc_driver_flightsql.dbapi as flight_sql
import pyarrow as pa

with flight_sql.connect("grpc://localhost:8815", autocommit=True) as conn, conn.cursor() as cur:
    cur.adbc_ingest("orders", pa.table({"id": [1, 2, 3], "country": ["JPN", "USA", "JPN"]}), mode="replace")
    cur.execute("SELECT country, COUNT(*) AS n FROM orders WHERE id >= ? GROUP BY country", parameters=(2,))
    print(cur.fetch_arrow_table())
```

Pass `autocommit=True`: the driver runs every statement in autocommit mode.
The SQL dialect is the Redis ADBC driver's, which is close to PostgreSQL;
see its [Supported SQL](https://github.com/redis/redis-adbc-driver#supported-sql).
Parameters may be written `?` or `$1`.

### Datasets from Python (pyarrow.flight)

```python
import json
import pyarrow as pa
import pyarrow.flight as flight

client = flight.connect("grpc://localhost:8815")
table = pa.table({"id": [1, 2], "embedding": [[0.1, 0.2], [0.3, 0.4]]})
desc = flight.FlightDescriptor.for_path("features", "v1")

writer, results = client.do_put(desc, table.schema)
writer.write_table(table)
writer.done_writing()
print(json.loads(results.read().to_pybytes()))  # {'dataset': 'features/v1', 'generation': 1, 'rows': 2, ...}
writer.close()

info = client.get_flight_info(desc)
print(pa.concat_tables([client.do_get(ep.ticket).read_all() for ep in info.endpoints]))

# Just the embedding column, of the first 10 batches.
ticket = flight.Ticket(json.dumps({"dataset": "features/v1", "columns": ["embedding"], "batches": [0, 10]}).encode())
print(client.do_get(ticket).read_all())

print([f.descriptor.path for f in client.list_flights(b"features/")])
list(client.do_action(flight.Action("drop_dataset", b"features/v1")))
```

Read every endpoint of a FlightInfo. A dataset has one unless the server runs
with `--dataset-endpoint-batches`.

### JDBC (DBeaver and others)

Use the [Arrow Flight SQL JDBC driver](https://arrow.apache.org/java/current/flight_sql_jdbc_driver.html)
(`org.apache.arrow:flight-sql-jdbc-driver`). The recorded local validation used
19.0.0 on Java 25; CI is configured for Java 17, 21 and 25. See
[tested integration coverage](#tested-integration-coverage). The URL is:

```text
jdbc:arrow-flight-sql://localhost:8815/?useEncryption=false
```

- **Logging in:** with `--auth=redis-acl`, add `&user=<redis user>&password=<…>`
  to the URL. The driver logs in with the Flight handshake and keeps the bearer
  token.
- **TLS:** use `useEncryption=true`, plus `trustStore` or
  `disableCertificateVerification` as needed.
- **Sessions:** the driver keeps cookies, so each JDBC connection is a
  [session](docs/server-guide.md#sessions): `SET`, `search_path` and temporary tables last from one
  statement to the next. Plain scans [stream](#flight-sql-support) from Redis
  in a session too, so a `SELECT *` of a large table (DBeaver's data tab)
  isn't held in the server's memory, and the connection runs other
  statements while a `ResultSet` is read. A connection that has made a
  temporary table reads its results whole.
- **Browsing a large result:** a `ResultSet` left unread for longer than
  `--stream-stall-timeout` (2 minutes) fails on its next fetch. DBeaver
  fetches a page at a time as you scroll, so raise the timeout if people
  leave results open.
- **Java 17 and later:** run the JVM with
  `--add-opens=java.base/java.nio=ALL-UNNAMED`.
- **Java 23 and later:** also add `--sun-misc-unsafe-memory-access=allow`.
  Without it the driver fails before it connects, with an
  `ExceptionInInitializerError` in `DefaultAllocationManagerFactory`. Java 17
  to 22 don't know the option and refuse to start with it.
- **DBeaver:** add the options to `dbeaver.ini`, one per line after `-vmargs`
  (the second one only if DBeaver runs on Java 23 or later):

  ```text
  -vmargs
  --add-opens=java.base/java.nio=ALL-UNNAMED
  --sun-misc-unsafe-memory-access=allow
  ```

`examples/jdbc/JdbcSmoke.java` checks prepared statements with `?` parameters,
DatabaseMetaData (catalogs, schemas, tables, columns, table types, keys),
update counts and the session, including a 100,000-row result read while the
connection runs other statements. CI runs it on Java 17, 21 and 25 on every pull
request:

```bash
make jdbc-smoke
```

That target downloads the jar into `bin/` (checking its SHA-1) and runs the
smoke test against a server on `localhost:8815`, with the JVM options for the
`java` on the `PATH`. Pick another with `JAVA=/path/to/bin/java`, and another
server with `JDBC_URL=…`.

Known limits of the JDBC driver (19.0.0):
- **Statements without a result schema run as updates.** The driver treats a
  prepared statement that has none as an update, so it gets no rows back. The
  server leaves the schema out for two kinds of statement:
  - a script of several statements that changes something, such as
    `CREATE TABLE x …; SELECT … FROM x`
  - a statement whose result depends on a parameter of unknown type
    (`SELECT ?`)

  Run such a query as its own statement.
- **Untyped parameters accept only `null`.** A parameter whose type can't be
  inferred from its use is typed NULL.

### Other clients

- **Go:** `github.com/apache/arrow-go/v18/arrow/flight/flightsql` (`flightsql.NewClient`) for SQL, and
  `flight.NewClientWithMiddleware` for datasets. The integration tests in
  `server/integration_test.go` use both.

## Feature reference

### Flight SQL support

| Feature | Support |
|-|-|
| Queries (`CommandStatementQuery`), updates (`CommandStatementUpdate`) | Yes. A statement without a row count (DDL, `SET`) reports 0 rows |
| Prepared statements, with parameters | Yes. The handle carries the SQL and the bound values, so the server keeps no state. Clients must use the handle a bind returns (`DoPutPreparedStatementResult`, Flight SQL's stateless prepared statements). arrow-go, the ADBC Flight SQL driver and the JDBC driver do, and are tested; a client that ignores it loses the bound values. The parameter schema gives each parameter the type it's used as (a compared column's, an `INSERT` target's, a `CAST`'s, …), as the Redis ADBC driver infers it, or NULL. Parameters for a query are limited to 1 MiB |
| Bulk ingest (`CommandStatementIngest`) | Yes: create, append, replace, create-or-append, and pass-through options such as `adbc.redis.ingest.index_columns`. Temporary tables (`temporary`, or the `pg_temp` schema) need a [session](docs/server-guide.md#sessions). `adbc.ingest.*` options are refused: set the target and mode with the command's fields |
| `GetCatalogs`, `GetDbSchemas`, `GetTables` (with schemas), `GetTableTypes`, `GetSqlInfo`, `GetXdbcTypeInfo` | Yes. The catalog is `redis`; schemas are key namespaces (`public` by default) |
| Primary and foreign keys | Empty results: the driver has no primary/foreign-key constraints |
| Sessions: `SetSessionOptions`, `GetSessionOptions`, `CloseSession` | Yes, for clients that keep cookies: see [Sessions](docs/server-guide.md#sessions) |
| Explicit query cancellation | `CancelFlightInfo`; the deprecated Flight SQL `CancelQuery` is translated to it. Applies to read-only query tickets with cancellation metadata; see below |
| Transactions, savepoints, Substrait | No |

How statements run:

- **Read-only statements** (`SELECT`, `SHOW`) run when their ticket is read,
  so any server replica can serve the ticket.
- **Every other statement** (`INSERT`, `UPDATE`, DDL, `SET`, …) runs when its
  FlightInfo is requested, so it runs exactly once even if the client never
  reads the stream (DB-API cursors often don't) or reads it twice. The result,
  such as the rows of `INSERT … RETURNING`, travels in the ticket. If it is
  larger than 1 MiB, it is kept in Redis for 10 minutes instead.
- **Outside a [session](docs/server-guide.md#sessions), each request uses a pooled ADBC
  connection.** Session state then doesn't carry over between requests.
  A statement that changes it (`SET`, `BEGIN`, `CREATE TEMP TABLE`, or a
  table or view created in `pg_temp`) works within its own script, for
  example `SET TIME ZONE 'Asia/Tokyo'; SELECT …`, and its connection is
  closed afterwards rather than reused.
- **Plain scans stream from Redis.** For a query that reads one table
  through its index, row by row (`SELECT * FROM t`, or a filter and
  `ORDER BY` the index can do), the driver reads the rows a cursor page at
  a time while they are sent. The server then holds about one batch of
  them, whatever the table's size. Other queries (aggregates, joins,
  `DISTINCT`, window functions, sorts the driver does) are built whole in
  memory first. The driver's
  [Streamed results](https://github.com/redis/redis-adbc-driver#streamed-results)
  has the exact rules.
  - A streaming result keeps its pooled connection until it has been sent,
    or the client goes away. Each Redis user can have `--max-streams` (8)
    results streaming at once, but always fewer than `--pool-max-open`, so
    the user's other requests have a connection (with a pool of 1, nothing
    streams). With `--auth=none`, every client is the same user. Queries
    beyond that are read whole (see below), as are queries in a
    [session](docs/server-guide.md#sessions) that has made temporary tables or views.
  - A streaming result also keeps a cursor open on its table's index.
    RediSearch allows an index `search-index-cursor-limit` open cursors
    (128 by default), and the driver reads every query through one, so at
    the limit every query on the table fails with `RESOURCE_EXHAUSTED`
    until some are closed; clients can retry. The limit is shared by every
    client of the index: other servers (replicas), other tools, and every
    Redis user. So each server streams at most `--max-table-streams` (32)
    results from one table at once, across all users, and reads the rest
    whole.
  - On standalone Redis, the server also reads how many cursors the
    index has open (`FT.INFO`, at most once a second per table), counting
    every client, and stops streaming from the table once three quarters
    of `search-index-cursor-limit` are open. The last quarter is left for
    queries read whole and other clients' short reads. A raised limit is
    picked up by itself.
  - On a cluster, `FT.INFO` doesn't report the count, so with several
    replicas keep their total `--max-table-streams` under the limit, or
    raise `search-index-cursor-limit` in Redis (and then
    `--max-table-streams`).
  - Set `--stream-overflow=reject` (`FLIGHT_STREAM_OVERFLOW=reject`) to
    refuse an eligible scan with `RESOURCE_EXHAUSTED` before execution when
    no stream/cursor slot is available. Retry after a stream finishes. The
    default `buffer` preserves the behavior below. This option does not bound
    driver execution memory for joins, aggregates or other non-streaming queries.
  - A query that could stream but finds no stream free (past
    `--max-streams` or `--max-table-streams`, or with the index near its
    cursor limit) is read into memory before it is sent, and its
    connection and cursor are given back at once. Such results share
    `--result-memory` (1 GiB) across the server until they have been sent. One that doesn't fit fails with `RESOURCE_EXHAUSTED`
    as soon as it would go over, and clients can retry, or read fewer rows
    or columns.
  - A client that takes longer than `--stream-stall-timeout` (2 minutes) to
    read a batch gets a `DEADLINE_EXCEEDED` error instead of the rest, and
    the server stops reading from Redis. Redis deletes a cursor that isn't
    read for 5 minutes.
  - To read every result whole instead, pass
    `-adbc-option adbc.redis.stream_results=false`.
- **Batches are sent at about 2 MiB each,** under gRPC's default 4 MiB limit.

### Query cancellation

`CancelFlightInfo` cancels a pending read-only SQL ticket or all active reads
of it, including prepared-query tickets returned by GetFlightInfo. The legacy
Flight SQL `CancelQuery` action maps to the same implementation. Active
cancellation is cooperative: readers poll Redis every 250 ms, then cancel the
execution context and release their cursors, connections and retained results.
A completed query or already-executed write returns `NOT_CANCELLABLE`;
cancellation never rolls back an autocommitted write. A racing active read can
return `CANCELLING` until cleanup finishes. Partial data already sent may have
reached the client; `CANCELLED` means the result is incomplete.

Cancellation records live under `arrowflight:query:{<id>}` (or `-key-prefix`)
and contain an opaque identity digest and lifecycle state, with no SQL or bound
values. Newly issued cancellable tickets expire after 10 minutes without an
active reader; readers renew that lease. Expired/unknown records give
`NOT_FOUND`; another Redis identity gets `PERMISSION_DENIED`. Repeated
cancellation is safe. Legacy tickets still work without explicit cancellation.
With `--auth=none`, clients share one Redis identity and its cancellation scope.

The Redis identity needs EVAL/GET/SET permissions on the query-metadata
keys to create a cancellable ticket. If metadata creation is denied, SQL still
works with a non-cancellable ticket. Other Redis errors remain errors. For
multiple replicas, use the same Redis, key prefix and Redis identity; cancellation
state is shared there. This does not make cookie sessions portable.

### Datasets

- **Name.** A dataset's name is its `PATH` descriptor's segments joined with
  `/`.
- **DoPut** stores the stream as the dataset's new *generation*, then switches
  to it atomically with a Lua script. Readers never see a half-written upload;
  generation numbers only go up, so of two uploads the one that started later
  wins. An older one that finishes last is replaced at once: its reply says
  `"superseded": true`, and its data expires after the grace period. The
  reply's app metadata is JSON: `dataset`, `generation`, `rows`, `batches`,
  `bytes`, `updated`, and `superseded` when set.
- **GetFlightInfo / GetSchema** return the schema, row and byte counts, and a
  ticket for the current generation. A ticket for a replaced generation keeps
  working for 5 minutes, so reads already under way finish.
- **DoGet** takes a ticket from GetFlightInfo, or one you write yourself. A
  ticket is JSON:

  | Field | Meaning |
  |-|-|
  | `dataset` | The name. Required |
  | `generation` | The generation to read. Leave it out for the current one |
  | `columns` | Read just these columns, in this order. A name the dataset doesn't have, or one given twice, is `InvalidArgument` |
  | `batches` | `[start, end]`: read the stored batches from `start` up to, not including, `end`, counting from 0. An `end` past the last batch reads to the last; `start` equal to `end` returns the schema alone. A negative `start`, or an `end` before `start`, is `InvalidArgument` |

  For example, `{"dataset": "features/v1"}` reads the current generation, and
  `{"dataset": "features/v1", "columns": ["id", "label"], "batches": [0, 4]}`
  reads two columns of its first four batches. The stored batches are the
  uploaded ones, split to about 2 MiB each; the PutResult and the FlightInfo's
  app metadata count them (`batches`). Each column of a batch is stored on
  its own, so a read takes only the ticket's columns from Redis. The
  [dataset benchmarks](server/dataset_bench_test.go) measure full and
  one-column reads. A read takes the columns about 8 MiB at a time (up to
  64 batches), so it holds about that much of the dataset, whatever its size.
- **Parallel reads.** With `--dataset-endpoint-batches=N`, GetFlightInfo,
  PollFlightInfo and ListFlights return one endpoint per N batches. Clients can
  read the endpoints in parallel, and must read them all; together, in order,
  they are the dataset (the FlightInfo says `ordered`). Without the flag there
  is one endpoint, since a client that reads only the first would miss data.
- **ListFlights** lists datasets; the criteria expression, if given, is a name
  prefix.
- **`drop_dataset` action** deletes a dataset. Its body is the name, or
  `{"dataset": "<name>"}`.
- **Expiry.** `--dataset-ttl` makes uploaded datasets expire.

Keys (the prefix is set with `--key-prefix`, default `arrowflight:`). Every
key of one dataset shares a hash tag, so on a Redis Cluster it lives in one
slot:

| Key | Type | Holds |
|-|-|-|
| `arrowflight:ds:{<name>}` | HASH | `gen`, `schema` (IPC), `rows`, `batches`, `bytes`, `updated` |
| `arrowflight:ds:{<name>}:g<N>` | HASH | Generation N, by column: `layout` (`2`); `schema`, an IPC stream with the schema alone; `batches`, the number of batches (written last, so a generation without it is an unfinished upload); and for each batch `b` (from 0), `<b>:rows`, its row count, and `<b>:<c>`, an IPC stream of its column `c` alone (counted from 0, in the schema's order) |
| `arrowflight:ds:{<name>}:g<N>` | LIST | A generation stored by an earlier server: an IPC stream with the schema alone, then one IPC stream per batch. It stays readable, but a read of some columns takes them all from Redis |
| `arrowflight:ds:{<name>}:seq` | STRING | The last generation number. It never expires, not even with `--dataset-ttl` or a drop, so generation numbers never repeat |
| `arrowflight:result:{<id>}` | STRING | A statement result too big for its ticket (expires after 10 minutes) |

The name in a key is URL-path-escaped.

Storing each column of a batch on its own can use more Redis memory than
storing batches whole, since each column of each batch is a separate value.

**Upgrading.** Servers before the HASH layout fail to read the generations
newer ones store (Redis answers their `LLEN` with `WRONGTYPE`), so upgrade
every replica before uploading again. Generations they stored stay readable
by newer servers; upload a dataset again to read its columns separately.
Commits and drops work across versions.
