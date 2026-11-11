<img referrerpolicy="no-referrer-when-downgrade" src="https://static.scarf.sh/a.png?x-pxid=a5e3e093-ccb6-4dd2-991a-0a75ab78f997" />

# Redis Arrow Flight server

Access Redis through [Apache Arrow Flight](https://arrow.apache.org/docs/format/Flight.html)
and [Flight SQL](https://arrow.apache.org/docs/format/FlightSql.html). This Go
server provides one gRPC endpoint for SQL queries and Arrow dataset transfers,
with clients connecting through ADBC, JDBC or native Flight SDKs.

Use SQL for queryable Redis tables, or upload named Arrow datasets for dataframe
snapshots, feature tables and batch transfers.

The SQL interface depends on the [Redis ADBC driver](https://github.com/redis/redis-adbc-driver),
which provides an **opinionated data model**: SQL tables follow the driver's
schema, Redis HASH row layout and Search index conventions. See the driver's
[data model](https://github.com/redis/redis-adbc-driver/blob/main/docs/driver-reference.md#rows-and-indexes).

For SQL workloads, we recommend **Redis with Query Engine (Search) enabled and
Query Performance Factor (QPF) set to 4× where supported**. Search is required
for SQL; 4× QPF is recommended for concurrent query performance.

- [Choose an interface](#choose-an-interface)
- [Integration paths](#integration-paths)
- [Get a server running](#get-a-server-running)
- [Data tools](#data-tools)
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

Flight SQL supports queries, updates, prepared statements, bulk ingestion,
metadata, sessions and read-only query cancellation. SQL uses autocommit;
transactions and savepoints are unsupported. See the
[protocol capabilities](docs/flight-capabilities.md) and
[SQL reference](docs/sql-reference.md) for supported commands and limits.

## Integration paths

All three paths connect to the same Flight endpoint. Flight SQL defines SQL
commands using Flight RPC; gRPC carries the requests and Arrow streams.

| Path | Typical use | Clients |
|---|---|---|
| **Flight SQL via ADBC** | SQL queries, bound parameters, metadata and bulk ingestion | Python `adbc_driver_flightsql`; R ADBC packages |
| **Flight SQL via JDBC** | Java applications and tools that accept custom JDBC drivers | Arrow Flight SQL JDBC driver; DBeaver configuration |
| **Native Flight RPC** | Named dataset upload/download, discovery and custom actions | Python `pyarrow.flight`, Go Flight clients and other Flight SDKs |

SDKs with Flight SQL support can also issue SQL commands directly, as the Go
tests do. Driver support and application validation are recorded separately in the
[client compatibility matrix](docs/client-compatibility.md).

## Get a server running

[Install the latest release](#install-a-release) with Docker or a binary. For
Docker Compose, source builds and the Python tour,
[run locally from source](docs/local-development.md).

See [build profiles](docs/build-profiles.md) for feature availability and
[startup and readiness](docs/build-profiles.md#startup-and-readiness) before
using a deployment health check.

### Install a release

Use the [latest release](https://github.com/redis/redis-apache-arrow-flight/releases/latest).
You need a running Redis instance; SQL requires **Query Engine (Search)**.
We recommend **4× QPF** where supported. These commands start the Flight server
on port **8815**.

#### Docker

With Docker Desktop and Redis running on your host:

```bash
docker run --rm --pull=always -p 8815:8815 \
  -e REDIS_URI=redis://host.docker.internal:6379/0 \
  ghcr.io/redis/redis-apache-arrow-flight:latest
```

`--pull=always` fetches the latest published image each time you run the command.
With Docker Engine on Linux, put Redis and Flight on the same Docker network
and use the Redis container's name in `REDIS_URI` instead of
`host.docker.internal`.

#### Binary (macOS or Linux)

Run this in a directory where you want to download the server. It detects your
platform, resolves the latest release, verifies its checksum and starts it
against local Redis. Requires `curl`, `tar` and `shasum`.

```bash
(
  set -eu
  case "$(uname -s)-$(uname -m)" in
    Darwin-arm64) PLATFORM=darwin-arm64 ;;
    Linux-x86_64) PLATFORM=linux-amd64 ;;
    Linux-aarch64|Linux-arm64) PLATFORM=linux-arm64 ;;
    *) echo "No published binary for this platform; use Docker or build from source." >&2; exit 1 ;;
  esac
  RELEASE_URL=$(curl -fsSL -o /dev/null -w '%{url_effective}' \
    https://github.com/redis/redis-apache-arrow-flight/releases/latest)
  VERSION=${RELEASE_URL##*/}
  ARCHIVE="redis-flight-server-${VERSION}-${PLATFORM}"
  DOWNLOAD="https://github.com/redis/redis-apache-arrow-flight/releases/download/${VERSION}"
  curl -fLO "${DOWNLOAD}/${ARCHIVE}.tar.gz"
  curl -fLO "${DOWNLOAD}/${ARCHIVE}.tar.gz.sha256"
  shasum -a 256 -c "${ARCHIVE}.tar.gz.sha256"
  tar -xzf "${ARCHIVE}.tar.gz"
  "./${ARCHIVE}/redis-flight-server" -redis-uri redis://localhost:6379/0
)
```

Release archives and container images are published by the Release workflow
once its checks finish. If a new release's artifacts are still being built,
wait for publication or [build from source](docs/local-development.md).
For a different Redis host, change `REDIS_URI` in Docker or `-redis-uri` above.
Keep the server running, then choose a [data tool guide](#data-tools).

## Integration testing

The [November 3, 2026 integration sweep](docs/validation/2026-11-03-flight-integrations/README.md)
recorded **scoped passes for all 27 inventoried Flight SQL routes**: 24 automated
tool workflows, RStudio and Tableau native UI checks, and Metabase API checks.
The automated run had **28 passed test cases, 2 strict expected failures and
0 unexpected failures/errors**. The expected failures reproduce a Datasets 5.1.0
empty save/reload defect; its one-shard workaround passed. Expected failures
are not passes. See the [tool-by-tool results](docs/validation/2026-11-03-flight-integrations/reports/summary.md)
and [repeatable testing guide](docs/integrations/testing.md).

That run used community source `c019afc` and standalone Redis Open Source 8.6.2
with Search. It does not establish Redis Enterprise/Software qualification,
4× QPF, TLS/authentication, distributed execution or full application
certification. That historical profile sweep has not been repeated in full against the
newer Enterprise composition; the separate guide recipe run below has its own
execution scope and provenance.

The [subsequent live guide recipe run](docs/validation/2026-11-03-guide-recipes/README.md)
passed scoped workflows for those 27 routes against the retained Enterprise-backed
endpoint, including output writes and read-back. Tableau used external ADBC
writes followed by a native preview. Three recipe defects were fixed: notebook
string schema conversion, Airflow DAG discovery and DuckDB 2.0's Arrow API.
This validates the documented client flows against that running deployment;
it does not requalify the current public source fingerprint or a release.

Integration validation requires a disposable Redis Enterprise cluster with Search,
verified TLS and username/password authentication. Basic TLS/ACL login is
available in this checkout; paid-feature tests additionally need their qualified
enterprise Flight composition. See [the local environment](docs/redis-enterprise-testing.md)
and [test/CI setup](docs/sdlc/testing.md). Historical client checks below retain
their recorded scope; public unit CI alone is not Enterprise qualification.

## Data tools

Choose the Flight SQL driver or native Flight API required by your workflow.
A tool's generic Redis or PostgreSQL connector does not connect to this endpoint.
SQL tools access SQL tables; named datasets use the Flight dataset API.
Each tool links to installation and read/write examples, including write
limitations and external ingestion alternatives where needed. Start with the
[shared prerequisites](docs/integrations/README.md).

| Tool | Connection path | Tested status against this server |
|---|---|---|
| [Python / PyArrow](docs/integrations/python.md) | ADBC Flight SQL for SQL; `pyarrow.flight` for datasets | Both smoke scripts passed |
| [Go](docs/integrations/go.md) | Flight SQL client or native Flight client | Integration/race suites passed |
| [Java](docs/integrations/java.md) | Flight SQL JDBC or native Flight SDK | JDBC passed; native Java Flight untested |
| [DBeaver](docs/dbeaver.md) | Generic Flight SQL JDBC 19.0.0 driver | Community 26.2.3: manual connection, table navigation and three-row SELECT passed on 2026-10-30; [scope and limits](docs/client-compatibility.md#dbeaver-rerun-and-manual-validation-on-2026-10-30) |
| [Other JDBC applications](docs/integrations/jdbc-applications.md) | Custom Flight SQL JDBC driver | Driver tested; individual applications untested |
| [pandas](docs/integrations/pandas.md) | ADBC `pd.read_sql`, or Arrow `to_pandas()` | Reads, NULLs and empty results passed; ADBC `params=` unsupported |
| [Polars](docs/integrations/polars.md) | ADBC `pl.read_database`, or `pl.from_arrow()` | Reads, NULLs, empty results and batch iteration passed |
| [DuckDB](docs/integrations/duckdb.md) | Download an Arrow table, then query it locally | Local SQL over Flight SQL and Flight RPC Arrow results passed |
| [R](docs/integrations/r.md) | `adbcdrivermanager` with the Flight SQL shared driver | DDL, writes, queries, NULLs and empty results passed; DBI/adbi untested |
| [Apache Superset](docs/superset.md) | Direct Redis ADBC for 6.1; Flight SQL candidate for 7 | Flight adapter blocked on 6.1; 7.0.0 development runtime SQL Lab API read/write passed; browser and full application validation pending |
| [Tableau](docs/integrations/tableau.md) | Other Databases (JDBC) with Flight SQL JDBC | Desktop Free Edition 2026.3: connection, discovery and three-row preview (including NULL) passed; [read validation and scope](docs/validation/2026-11-01-tableau/README.md) |
| [Metabase](docs/integrations/metabase.md) | Community Flight SQL plugin 0.8.0 with bundled JDBC 19.0.0 | Metabase 0.64.2.9 API reads, schema sync, generated queries and INSERT/UPDATE/DELETE Actions passed; [evidence and native-DDL limitation](docs/validation/2026-11-01-bi-jdbc/README.md) |
| [Power BI](docs/integrations/power-bi.md) | Custom ADBC Power Query connector | Untested; no connector shipped |
| [Redis Insight](docs/integrations/redis-insight.md) | Connect directly to the backing Redis database | Untested; dataset payloads remain Arrow IPC |
| [NumPy](docs/integrations/numpy.md) | Flight SQL via Arrow/ADBC | November 3: Arrow-to-array reads passed |
| [scikit-learn](docs/integrations/scikit-learn.md) | Flight SQL via Arrow/ADBC | November 3: Local model input/training passed |
| [SQLAlchemy](docs/integrations/sqlalchemy.md) | Flight SQL via Arrow/ADBC | November 3: Flight SQL dialect reads passed; DBAPI autocommit required |
| [Dagster](docs/integrations/dagster.md) | Flight SQL via Arrow/ADBC | November 3: Local ADBC resource materialization passed |
| [Jupyter / JupyterLab](docs/integrations/jupyter.md) | Flight SQL via Arrow/ADBC | November 3: Notebook execution passed; browser UI untested |
| [Hugging Face Datasets](docs/integrations/hugging-face-datasets.md) | Flight SQL via Arrow/ADBC | November 3: Arrow/filter workflow passed; two expected empty-persistence failures |
| [Streamlit](docs/integrations/streamlit.md) | Flight SQL via Arrow/ADBC | November 3: Local data workflow passed; browser rendering untested |
| [SciPy](docs/integrations/scipy.md) | Flight SQL via Arrow/ADBC | November 3: Local statistics over Flight rows passed |
| [XGBoost](docs/integrations/xgboost.md) | Flight SQL via Arrow/ADBC | November 3: CPU training and local model save/reload passed |
| [PyTorch](docs/integrations/pytorch.md) | Flight SQL via Arrow/ADBC | November 3: Local DataLoader and forward pass passed |
| [TensorFlow](docs/integrations/tensorflow.md) | Flight SQL via Arrow/ADBC | November 3: CPU dataset/batch input passed |
| [Keras](docs/integrations/keras.md) | Flight SQL via Arrow/ADBC | November 3: CPU inference and local model save/reload passed |
| [Hugging Face Transformers](docs/integrations/transformers.md) | Flight SQL via Arrow/ADBC | November 3: Tiny offline model input and local save/reload passed |
| [Plotly](docs/integrations/plotly.md) | Flight SQL via Arrow/ADBC | November 3: Local HTML/JSON artifacts passed |
| [Matplotlib](docs/integrations/matplotlib.md) | Flight SQL via Arrow/ADBC | November 3: Local PNG rendering passed |
| [Kubeflow Pipelines](docs/integrations/kubeflow.md) | Flight SQL via Arrow/ADBC | November 3: Local SubprocessRunner component passed |
| [Metaflow](docs/integrations/metaflow.md) | Flight SQL via Arrow/ADBC | November 3: Local flow/datastore passed |
| [Apache Spark](docs/integrations/spark.md) | Flight SQL JDBC | November 3: JDBC reads and predicate partitions passed |
| [Apache Airflow](docs/integrations/airflow.md) | Flight SQL via Arrow/ADBC | November 3: Local TaskFlow DAG execution passed |
| [Quarto](docs/integrations/quarto.md) | Flight SQL via Arrow/ADBC | November 3: Python-cell reads and HTML rendering passed |
| [RStudio](docs/integrations/rstudio.md) | R ADBC in the native IDE | November 3: Native R Console read/write harness passed; DBI/adbi untested |

The table records operation-specific scopes and dates. The [live recipe results](docs/validation/2026-11-03-guide-recipes/summary.md)
add scoped read/write evidence; native application writes and external ADBC
ingestion are identified separately.
The [November 3 sweep](docs/client-compatibility.md#all-27-inventoried-flight-sql-routes--november-3-2026)
adds fresh evidence for dataframe, ML, plotting, workflow and application routes;
R DBI/adbi, Tableau worksheets/extracts/dashboards, and Metabase browser
dashboards remain untested. See the
[client compatibility matrix](docs/client-compatibility.md) for versions, dates
and the scope of recorded checks. The
[ADBC ecosystem support inventory](https://github.com/redis/redis-adbc-driver/blob/main/docs/data-engineering-adbc-support.md)
includes upstream Redis ADBC evidence; those results do not establish Flight
server application compatibility.

## Documentation

Read the [documentation website](https://redis.github.io/redis-apache-arrow-flight/)
for browsable guides and full-text search. See
[documentation publishing](docs/documentation-site.md) to preview or maintain it.

| Topic | Guide |
|---|---|
| Data tool setup and examples | [Integration guides](docs/integrations/README.md) |
| Local setup and source builds | [Run locally from source](docs/local-development.md) |
| ADBC, native Flight and JDBC examples | [Client guide](docs/client-guide.md) |
| DBeaver driver setup and manual tests | [DBeaver guide](docs/dbeaver.md) |
| Client versions and validation evidence | [Client compatibility](docs/client-compatibility.md) |
| ADBC, JDBC, native Flight and redis-py performance | [Performance measurements](docs/performance.md) |
| SQL capabilities and execution behavior | [Flight SQL reference](docs/sql-reference.md) |
| Cancellation and named Arrow datasets | [Data access reference](docs/data-access.md) |
| Combining datasets and SQL | [Dataset-to-SQL workflow](docs/dataset-sql-workflow.md) |
| Community and enterprise availability | [Build profiles](docs/build-profiles.md) |
| Shared managed-catalog read snapshots (qualification pending) | [Snapshot protocol](docs/shared-read-snapshots.md) |
| Cookies, settings and temporary tables | [Sessions](docs/server-guide.md#sessions) |
| Configuration, security, shutdown and monitoring | [Deployment and operations](docs/server-guide.md#deployment-and-operations) |
| Routing, storage and request flow | [Architecture](docs/server-guide.md#architecture) |
| Code layout, tests and releases | [Development](docs/server-guide.md#development) |
| Dataset benchmarks and acceptance goals | [Endpoint acceptance](docs/endpoint-acceptance.md), [benchmarks](server/dataset_bench_test.go) |
| Project context and engineering process | [SDLC documentation](docs/sdlc/README.md) |
