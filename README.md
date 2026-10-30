<img referrerpolicy="no-referrer-when-downgrade" src="https://static.scarf.sh/a.png?x-pxid=a5e3e093-ccb6-4dd2-991a-0a75ab78f997" />

# Redis Arrow Flight server

Access Redis through [Apache Arrow Flight](https://arrow.apache.org/docs/format/Flight.html)
and [Flight SQL](https://arrow.apache.org/docs/format/FlightSql.html). This Go
server provides one gRPC endpoint for SQL queries and Arrow dataset transfers,
with clients connecting through ADBC, JDBC or native Flight SDKs.

Use SQL for queryable Redis tables, or upload named Arrow datasets for dataframe
snapshots, feature tables and batch transfers.

- [Choose an interface](#choose-an-interface)
- [Integration paths](#integration-paths)
- [Get a server running](#get-a-server-running)
- [Client examples](#client-examples)
- [Tested integration coverage](#tested-integration-coverage)
- [Data tools](#data-tools)
- [Feature reference](#feature-reference)
- [Documentation](#documentation)

## Choose an interface

| | Flight SQL | Flight datasets |
|---|---|---|
| Use it for | Filters, aggregates, joins, updates, DDL and bulk ingestion | Upload, download, discover and drop named Arrow tables |
| Storage | One Redis HASH per row, plus a RediSearch index per table | Arrow IPC stored by column and batch |
| Read selection | SQL predicates, projections and aggregations | Columns and batch ranges |
| Types | SQL types supported by the [Redis ADBC driver](https://github.com/redis/redis-adbc-driver) | Arrow types, including lists, structs, maps and dictionaries |
| Redis requirements | Query Engine: Redis 8, Redis Stack, or Redis Cloud / Software with Search; Redis Flex is refused | Any Redis, including Flex and Cluster |

SQL tables remain readable by other Redis clients. Datasets preserve Arrow
values without converting them to SQL rows. You can combine the two: upload a
dataset, then ingest selected data into a SQL table. See the
[dataset-to-SQL workflow](docs/dataset-sql-workflow.md).

## Integration paths

All three paths connect to the same Flight endpoint. Flight SQL defines SQL
commands using Flight RPC; gRPC carries the requests and Arrow streams.

| Path | Typical use | Clients |
|---|---|---|
| **Flight SQL via ADBC** | SQL queries, bound parameters, metadata and bulk ingestion | Python `adbc_driver_flightsql`; R ADBC packages |
| **Flight SQL via JDBC** | Java applications and tools that accept custom JDBC drivers | Arrow Flight SQL JDBC driver; DBeaver configuration |
| **Native Flight RPC** | Named dataset upload/download, discovery and custom actions | Python `pyarrow.flight`, Go Flight clients and other Flight SDKs |

SDKs with Flight SQL support can also issue SQL commands directly, as the Go
tests do. Driver support and application validation are recorded separately in
[tested integration coverage](#tested-integration-coverage) below.

## Get a server running

[Run locally from source](docs/local-development.md) for prerequisites, Docker
Compose commands, source builds and the Python tour. For published binaries and
container images, follow [Install a release](#install-a-release).

See [build profiles](docs/build-profiles.md) for feature availability and
[startup and readiness](docs/build-profiles.md#startup-and-readiness) before
using a deployment health check.

### Install a release

You need a running Redis instance. SQL requires Redis with the Query Engine;
see [Redis requirements](#choose-an-interface). These commands start the Flight
server only.

Choose a published tag from [Releases](https://github.com/redis/redis-apache-arrow-flight/releases).
Replace `vX.Y.Z` below with that tag. If no release is available for your
platform, [build from source](docs/local-development.md).

```bash
VERSION=vX.Y.Z
```

Choose **one** of the following options.

#### Option 1: Download and run a binary

Set `PLATFORM` to `linux-amd64`, `linux-arm64` or `darwin-arm64` (Apple Silicon
macOS), matching your machine. For example:

```bash
PLATFORM=linux-amd64
ARCHIVE="redis-flight-server-${VERSION}-${PLATFORM}"
curl -fLO "https://github.com/redis/redis-apache-arrow-flight/releases/download/${VERSION}/${ARCHIVE}.tar.gz"
curl -fLO "https://github.com/redis/redis-apache-arrow-flight/releases/download/${VERSION}/${ARCHIVE}.tar.gz.sha256"
shasum -a 256 -c "${ARCHIVE}.tar.gz.sha256"
```

After the checksum passes, extract the archive and start the server. This
example connects to Redis running locally on port 6379:

```bash
tar -xzf "${ARCHIVE}.tar.gz"
"./${ARCHIVE}/redis-flight-server" -redis-uri redis://localhost:6379/0
```

#### Option 2: Run a container

The image tag is the release version without its leading `v`; `${VERSION#v}`
performs that conversion. With Docker Desktop and Redis running on your host:

```bash
docker run --rm -p 8815:8815 \
  -e REDIS_URI=redis://host.docker.internal:6379/0 \
  "ghcr.io/redis/redis-apache-arrow-flight:${VERSION#v}"
```

With Docker Engine on Linux, put Redis and Flight on the same Docker network
and use the Redis container's name in `REDIS_URI` instead of
`host.docker.internal`.

Both options expose Flight on port **8815**. Keep the server running while
trying the [client examples](#client-examples).

## Client examples

With a server listening on `localhost:8815`, use these addresses for local
connections without TLS:

```text
Flight / ADBC: grpc://localhost:8815
JDBC:          jdbc:arrow-flight-sql://localhost:8815/?useEncryption=false
```

### Flight SQL from Python (ADBC)

```python
import adbc_driver_flightsql.dbapi as flight_sql
import pyarrow as pa

with flight_sql.connect("grpc://localhost:8815", autocommit=True) as conn, conn.cursor() as cur:
    cur.adbc_ingest("orders", pa.table({"id": [1, 2, 3], "country": ["JPN", "USA", "JPN"]}), mode="replace")
    cur.execute("SELECT country, COUNT(*) AS n FROM orders WHERE id >= ? GROUP BY country", parameters=(2,))
    print(cur.fetch_arrow_table())
```

SQL uses autocommit; transactions are unsupported. Parameters may use `?` or
`$1`. The SQL dialect is documented by the
[Redis ADBC driver](https://github.com/redis/redis-adbc-driver#supported-sql).

### Datasets from Python (pyarrow.flight)

```python
import pyarrow as pa
import pyarrow.flight as flight

client = flight.connect("grpc://localhost:8815")
table = pa.table({"id": [1, 2], "embedding": [[0.1, 0.2], [0.3, 0.4]]})
desc = flight.FlightDescriptor.for_path("features", "v1")

writer, results = client.do_put(desc, table.schema)
writer.write_table(table)
writer.done_writing()
results.read()  # Consume the upload acknowledgement.
writer.close()

info = client.get_flight_info(desc)
received = pa.concat_tables([
    client.do_get(endpoint.ticket).read_all() for endpoint in info.endpoints
])
print(received)
```

Read every endpoint in the returned FlightInfo. For column/batch selection,
discovery, deletion and storage behavior, see the
[dataset reference](docs/data-access.md#datasets) and
[full Python example](docs/client-guide.md#datasets-from-python-pyarrowflight).

### JDBC (DBeaver and others)

Use `org.apache.arrow:flight-sql-jdbc-driver` with the JDBC URL above. Arrow
requires JVM options: Java 17+ needs
`--add-opens=java.base/java.nio=ALL-UNNAMED`; Java 23+ also needs
`--sun-misc-unsafe-memory-access=allow`.

Follow the [JDBC setup guide](docs/client-guide.md#jdbc-dbeaver-and-others) for
DBeaver configuration, authentication, TLS, session behavior and known driver
limitations. The JDBC smoke test passed; the DBeaver application attempt was blocked by a
macOS accessibility crash before connecting.

### Other clients

Go clients use `flightsql.NewClient` for SQL and
`flight.NewClientWithMiddleware` for datasets. See the
[client guide](docs/client-guide.md#other-clients),
[Python tour](examples/python/quickstart.py) and
[Go integration tests](server/integration_test.go).

## Tested integration coverage

All three integration paths have recorded passing checks. These are historical
results, with versions, dates and limitations in the
[client compatibility matrix](docs/client-compatibility.md).

| Path | Tested client | Recorded passing coverage | Evidence |
|---|---|---|---|
| Flight SQL via ADBC | Python driver/manager 1.12.0 | Metadata, parameters, queries/updates, bulk ingestion and dataset-to-SQL import | [Python tour](examples/python/quickstart.py), [import example](examples/python/dataset_to_sql.py) |
| Flight SQL via JDBC | Flight SQL JDBC 19.0.0 on Java 25 | Metadata, prepared parameters, updates, sessions and concurrent statement/result reads | [JDBC smoke test](examples/jdbc/JdbcSmoke.java) |
| Native Flight RPC | PyArrow 26.0.0 | Upload/download, nested and dictionary types, multiple batches, column/batch selection, discovery and drop | [Python tour](examples/python/quickstart.py), [import example](examples/python/dataset_to_sql.py) |
| Native Flight RPC / Flight SQL SDK | Go arrow-go 18.8.0 | Datasets, SQL metadata, parameters, ingestion, scans, sessions, cancellation and cleanup; standalone/Cluster race suites | [Go integration tests](server/integration_test.go) and related server tests |

Initial validation was recorded on October 20, 2026, with a Redis ADBC
v0.0.17 follow-up on October 22. On October 29, both Go race suites, both Python
examples and the JDBC smoke test passed again. Each Go suite had 80 passing
tests and one enterprise-only skip. CI is configured for Java 17/21/25; the
recorded local JDBC runs cover Java 25. See the
[core validation record](docs/client-compatibility.md#core-endpoint-validation-on-2026-10-29).
No integration suites were rerun for this documentation rewrite.

## Data tools

Choose the Flight SQL driver or native Flight API required by your workflow.
A tool's generic Redis or PostgreSQL connector does not connect to this endpoint.
SQL tools access SQL tables; named datasets use the Flight dataset API.

| Tool | Connection path | Tested status against this server |
|---|---|---|
| Python / PyArrow | ADBC Flight SQL for SQL; `pyarrow.flight` for datasets | Both smoke scripts passed |
| Go | Flight SQL client or native Flight client | Integration/race suites passed |
| Java | Flight SQL JDBC or native Flight SDK | JDBC passed; native Java Flight untested |
| DBeaver | Custom Flight SQL JDBC driver; [setup](docs/client-guide.md#jdbc-dbeaver-and-others) | Application attempt blocked by a macOS accessibility crash before connecting; JDBC smoke passed |
| Other JDBC applications | Custom Flight SQL JDBC driver | Driver tested; individual applications untested |
| pandas | ADBC `pd.read_sql`, or Arrow `to_pandas()` | Reads, NULLs and empty results passed; ADBC `params=` unsupported |
| Polars | ADBC `pl.read_database`, or `pl.from_arrow()` | Reads, NULLs, empty results and batch iteration passed |
| DuckDB | Download an Arrow table, then query it locally | Local SQL over Flight SQL and Flight RPC Arrow results passed |
| R | `adbcdrivermanager` with the Flight SQL shared driver | DDL, writes, queries, NULLs and empty results passed; DBI/adbi untested |
| Apache Superset | DB-API / SQLAlchemy adapter; candidate: beta `sqlalchemy-adbc[flightsql]` | Blocked: Superset 6.1.0 and adapter 0.1.0–0.3.0 require incompatible SQLAlchemy versions |
| Tableau | Other Databases (JDBC) with Flight SQL JDBC | Untested; metadata and generated-SQL validation required |
| Power BI | Custom ADBC Power Query connector | Untested; no connector shipped |
| Redis Insight | Connect directly to the backing Redis database | Untested; dataset payloads remain Arrow IPC |

The [October 29 client validation](docs/client-compatibility.md#flight-client-validation-on-2026-10-29)
records dataframe and R results and application blockers. See the
[client compatibility matrix](docs/client-compatibility.md) for the scope of recorded checks and the [tool setup notes](docs/client-guide.md#data-tools)
for driver links and connector requirements. The
[22-tool ADBC comparison](https://github.com/redis/redis-apache-arrow-flight/blob/main/docs/data-engineering-adbc-support.md)
includes upstream Redis ADBC evidence; those results do not establish Flight
server application compatibility.

## Feature reference

### Flight SQL support

The server supports queries and updates, prepared statements, bulk ingestion,
catalog/schema/table metadata, cookie sessions and explicit read-only query
cancellation. SQL runs in autocommit mode. Transactions, savepoints, Substrait
and bidirectional `DoExchange` are unsupported.

Supported commands, optional features and errors are listed in the
[protocol capability matrix](docs/flight-capabilities.md). For prepared-handle
rules, ingestion options, statement execution and streaming limits, see the
[Flight SQL reference](docs/sql-reference.md). Streaming, pooling and other
capabilities depend on the [build profile](docs/build-profiles.md).

## Documentation

| Topic | Guide |
|---|---|
| Local setup and source builds | [Run locally from source](docs/local-development.md) |
| ADBC, native Flight and JDBC examples | [Client guide](docs/client-guide.md) |
| Client versions and validation evidence | [Client compatibility](docs/client-compatibility.md) |
| SQL capabilities and execution behavior | [Flight SQL reference](docs/sql-reference.md) |
| Cancellation and named Arrow datasets | [Data access reference](docs/data-access.md) |
| Combining datasets and SQL | [Dataset-to-SQL workflow](docs/dataset-sql-workflow.md) |
| Community and enterprise availability | [Build profiles](docs/build-profiles.md) |
| Cookies, settings and temporary tables | [Sessions](docs/server-guide.md#sessions) |
| Configuration, security, shutdown and monitoring | [Deployment and operations](docs/server-guide.md#deployment-and-operations) |
| Routing, storage and request flow | [Architecture](docs/server-guide.md#architecture) |
| Code layout, tests and releases | [Development](docs/server-guide.md#development) |
| Dataset benchmarks and acceptance goals | [Endpoint acceptance](docs/endpoint-acceptance.md), [benchmarks](server/dataset_bench_test.go) |
| Project context and engineering process | [SDLC documentation](docs/sdlc/README.md) |
