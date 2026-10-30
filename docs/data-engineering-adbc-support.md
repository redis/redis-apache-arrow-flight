# ADBC support across 22 data engineering tools

> Mirrored from [redis/redis-adbc-driver](https://github.com/redis/redis-adbc-driver/blob/main/docs/data-engineering-adbc-support.md). References to “this repository” below refer to the upstream driver repository; relative documentation and evidence links point upstream.

For installation and diagnostics, start with [Choose your tool and connect](https://github.com/redis/redis-adbc-driver/blob/main/docs/setup.md).


Support and testing status refreshed on **2026-10-29** against this repository
and selected upstream ADBC documentation. The broader connector inventory retains
the **2026-10-16** review baseline; this refresh is not a new audit of every
third-party connector. This is a selection of prominent tools across the data
engineering stack, not a measured popularity ranking.
Dask documentation checked on **2026-10-20**. Ray worker reader checked on **2026-10-29**.
dbt adapter documentation and linked CI evidence checked on **2026-10-21**.

**ADBC (Arrow Database Connectivity)** is a database interface designed to
exchange data in Apache Arrow's columnar format. Support has two distinct
directions: a tool can **consume an ADBC driver** to access another database,
or **have an ADBC driver** that lets other applications query the tool.
The latter does not imply that the tool can load the Redis driver.

## Tool comparison

**Redis ADBC support** means loading this repository's driver locally.
**Redis Arrow Flight SQL support** means connecting over the network to the
[Redis Arrow Flight server](https://github.com/alberttwong/redis-apache-arrow-flight#client-examples),
which runs the Redis ADBC driver on the server side. It requires a separate server
deployment; it is not a built-in endpoint of Redis. Its Flight datasets API is a
separate interface and does not establish Flight SQL compatibility.

Recorded Redis integration coverage includes direct pandas SQL reads/writes, Polars,
Dagster's community ADBCResource and this repository's IO manager, two DuckDB
paths, Airflow TaskFlow and the packaged RedisAdbcHook, Prefect custom Python
tasks, Ray worker-side reads and
the Arrow-table bridge, and
the Dask partitioned reader. The repository also ships an append-only dlt
destination with recorded live validation on earlier sources and partial public-base
validation; see its evidence below. The companion dbt adapter project records separate
v1 and experimental v2 end-to-end validation. See [Testing status](#testing-status)
for dated evidence and limits. Ray's worker reader streams batches in local
worker-process tests; multi-node operation remains unverified. The older bridge
materializes the full result in the caller and has separate validation.

"Candidate" describes an inferred connection path from documented APIs, not
confirmed compatibility. A passing test applies to its recorded versions,
environment, and operations. General ADBC validation and the related Flight
server's client coverage do not constitute tests of all 22 tools.

The connector columns distinguish **Redis-built / maintained** projects from
**third-party-built** projects. Redis Field Engineering and the former RedisLabs
organization count as Redis provenance; Apache, Prefect, Airbyte, Fivetran,
CData, and independent extension authors do not. "None verified" is not a claim
that no such project exists. An official integration for a tool is not necessarily
built by Redis. The Redis ADBC driver and related Flight server are listed in
their own columns; the Redis name alone does not establish Redis, Inc. authorship
or support.

The **Redis connection solution** column summarizes qualifying connection paths:

- ✅ means a documented ADBC/Flight SQL integration path for Redis SQL/Arrow data,
  a Redis-built tool-specific data connector (including source/sink connectors),
  or a third-party integration providing Query Engine-backed SQL or native Redis
  command access. Driver installation, configuration, and compatibility testing
  may still be needed; candidate paths remain explicitly labeled in the table.
- ❌ means no qualifying path is established for the row, or a qualifying
  connector/adapter still needs development. Cache-only access is insufficient.
- A generic SDK such as **redis-py is not a tool connector**. Its availability in a
  Python runtime does not qualify a row for ✅ or fill either connector column.

The icons beside each green checkmark identify the qualifying path(s):

- 🔌 **Redis ADBC** — the tool can consume this driver's ADBC connection.
- ✈️ **Redis Arrow Flight SQL** — the tool can reach the Redis Flight SQL server
  through an ADBC Flight SQL or JDBC integration path.
- 🧩 **Redis-built connector** — a qualifying tool-specific connector built by
  Redis / Redis Field Engineering / RedisLabs.
- 🤝 **Third-party connector** — a qualifying tool-specific integration built by
  someone else, such as the Apache Airflow Redis provider or prefect-redis.

Multiple icons mean multiple paths contribute to the green status. An icon does
not establish an end-to-end Redis integration test; the adjacent columns retain
candidate, configuration, runtime, and validation limits. Spark's 🧩 refers to
spark-redis, and its ✈️ identifies the separate Flight SQL candidate. Kafka and
Flink qualify through their Redis-built source/sink connectors (✅ 🧩); their
connector columns describe supported data structures and operations. These badges
do not claim unrestricted command passthrough or Query Engine support where it
is not documented. Prefect's 🤝 refers to prefect-redis, not a Redis-built
connector or the generic redis-py SDK. Its 🔌 now refers to the validated custom
Python ADBC task path, not a packaged Prefect ADBC connector. Red rows keep ❌ without path icons because
none of their listed paths currently qualifies.

**Query Engine-backed SQL is a qualifying solution**, including Redis SQL Trino.
It does not need to expose arbitrary Redis commands to qualify through that SQL
path. ADBC/Flight SQL also expose supported SQL rather than unrestricted command
passthrough. For native-command connector claims, full Redis API access including
Query Engine commands must be established; cache-only access is insufficient.

Snowflake remains ❌ because its Openflow Redis cache service establishes neither
that command access nor Query Engine-backed SQL access. Trino is ✅ because
Redis-built **Redis SQL Trino** provides indexed SQL queries through the Query
Engine. Its README attributes copyright and support to Redis, Inc. The Trino
project's built-in Redis connector is a different, more limited connector.

Connector ownership and connection availability are separate: a third-party ADBC
integration can provide a solution even when no Redis-built connector is verified.
These symbols describe documented or explicitly labeled candidate paths, not
completed end-to-end tests or guarantees for every runtime.

| # | Tool | Redis connection solution | Main purpose | General ADBC support | Redis ADBC support | Redis Arrow Flight SQL support | Redis-built / maintained connector | Third-party-built connector / alternative |
|---|---|---|---|---|---|---|---|---|
| 1 | Apache Spark | ✅ ✈️ 🧩 | Distributed processing | **Driver available for querying Spark.** Its built-in external database reader uses JDBC; consuming another ADBC driver requires integration work. [Driver list](https://arrow.apache.org/adbc/main/driver/index.html), [JDBC reader](https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html). | Custom Python or data-source integration; untested. | Candidate via Flight SQL JDBC and Spark JDBC reader; validate JVM setup, metadata, partitioning, and SQL. | [spark-redis](https://github.com/RedisLabs/spark-redis), RedisLabs: Redis data structures and Spark SQL; older documented Spark targets. Full Redis command / Query Engine coverage is not established for this connector. | None verified. |
| 2 | Databricks | ✅ ✈️ 🧩 | Lakehouse and processing | **Driver available** for ADBC clients to query Databricks. This does not establish a generic facility for loading the Redis driver into Databricks. [Driver list](https://arrow.apache.org/adbc/main/driver/index.html). | Custom notebook/Python integration; untested. Databricks ADBC is an outbound driver for querying Databricks. | Candidate via Spark JDBC on compute permitting custom drivers, or Python Flight SQL; untested. | [spark-redis](https://github.com/RedisLabs/spark-redis), RedisLabs: Spark library candidate, not a verified bundled Databricks connector. Check runtime compatibility; full Redis command / Query Engine coverage is not established for this connector. | None verified. |
| 3 | Snowflake | ❌ | Cloud warehouse | **Driver available** for accessing Snowflake from ADBC clients. [Driver list](https://arrow.apache.org/adbc/main/driver/index.html). | No generic Redis ADBC loading path verified inside the warehouse; an external pipeline can copy data. | No built-in Redis Flight SQL source verified; use an external ingestion pipeline. | None verified. | [Snowflake Openflow Redis cache service](https://docs.snowflake.com/en/user-guide/data-integration/openflow/controllers/redisdistributedmapcacheclientservice), Snowflake/Apache NiFi component. Cache-only access does not qualify; full Redis API and Query Engine commands are not established. |
| 4 | Google BigQuery | ❌ | Cloud warehouse | **Driver available** for accessing BigQuery from ADBC clients. [Driver list](https://arrow.apache.org/adbc/main/driver/index.html). | No generic Redis ADBC loading path verified inside BigQuery; an external pipeline can copy data. | No native Redis Flight SQL federation verified; use an external ingestion pipeline. | None verified. | No native Redis federation connector verified in [BigQuery supported federated sources](https://docs.cloud.google.com/bigquery/docs/federated-queries-intro). External replication is a separate pipeline. |
| 5 | Trino | ✅ 🧩 | Distributed SQL queries | **Driver available** for querying Trino; this is distinct from a Trino connector to an arbitrary ADBC source. [Driver list](https://arrow.apache.org/adbc/main/driver/index.html). | No generic Redis ADBC source connector verified; Trino would need connector work. | No built-in Redis Flight SQL connector verified; a custom Trino connector would be needed. | [Redis SQL Trino](https://github.com/redis-field-engineering/redis-sql-trino), built by Redis Field Engineering / Redis, Inc.: SQL over indexed HASH/JSON data with filters and aggregations pushed to the Query Engine. [Documentation](https://redis-field-engineering.github.io/redis-sql-trino/) explicitly describes FT.SEARCH / FT.AGGREGATE use and Redis Cluster configuration. This is separate from the Trino project's built-in Redis connector; unrestricted native command passthrough is not established. | [Built-in Trino Redis connector](https://trino.io/docs/current/connector/redis.html), Trino project: selected key/value reads, no Redis Cluster. This is a different connector from Redis SQL Trino. |
| 6 | Microsoft Fabric / Power Query | ❌ | Data preparation and analytics | **Yes, for supported connectors.** Power Query also documents custom ADBC connectors; availability depends on the host. [Connector development](https://learn.microsoft.com/en-us/power-query/adbc). | Custom Power Query connector required; untested with Redis. | Documented custom connector approach using ADBC Flight SQL; navigation, types, folding, gateway, and host support need validation. | None verified. | [CData Redis Power BI connector](https://www.cdata.com/kb/tech/redis-powerbi-setup.rst), CData: commercial ODBC-based Desktop/Gateway integration. Full Redis command and Query Engine passthrough not verified; not all Fabric hosts. |
| 7 | dbt | ✅ 🔌 | SQL transformations | **Yes, through adapters.** The Rust-based dbt v2 engine uses ADBC; dbt Core v1 can use it through a database-specific Python adapter. An ADBC driver alone does not register a database in either engine. [ADBC requirements](https://docs.getdbt.com/docs/dbt/adbc), [Adapter development](https://docs.getdbt.com/docs/contribute-dbt-adapters-v2?version=2). | Available: [dbt-redis-adbc for Core 1.10–1.x](https://github.com/alberttwong/redis-dbt-adapter-with-project/tree/main/v1/adapter) and an [experimental native v2 adapter](https://github.com/alberttwong/redis-dbt-adapter-with-project/tree/main/v2) compiled into the [custom engine fork](https://github.com/alberttwong/dbt). The standard v2 binary does not include Redis; install the native Redis driver separately. [Validation scope](https://github.com/alberttwong/redis-dbt-adapter-with-project/blob/main/v2/VALIDATION.md). | Unverified. The existing adapters load the Redis driver directly; they do not establish Flight SQL compatibility. Adapter connection/configuration work and end-to-end validation remain necessary. | None verified. | Community [v1/v2 adapter project](https://github.com/alberttwong/redis-dbt-adapter-with-project), with v2 Rust implementation in the engine fork. Redis, Inc. maintenance or support is not established. |
| 8 | Apache Airflow | ✅ 🔌 ✈️ 🤝 | Workflow orchestration | **Yes, through the Apache Arrow provider's `AdbcHook`.** Install the provider and database driver. [ADBC connection](https://airflow.apache.org/docs/apache-airflow-providers-apache-arrow/stable/connections/adbc.html). | Locally tested with Airflow 3.3.2 TaskFlow and Arrow provider 0.1.1: ingest, SQL aggregation and cleanup. The packaged `RedisAdbcHook` validates `get_records()` / `get_first()`, bound queries, and writes using autocommit and bundled-driver discovery. Import it explicitly inside a Python task; the generic Arrow provider 0.1.1 hook still rejects `get_records()` transaction mode. [Hook guide](https://github.com/redis/redis-adbc-driver/blob/main/docs/python-integrations.md#redis-hook-for-get_records). [Setup and scope](https://github.com/redis/redis-adbc-driver/blob/main/docs/python-integrations.md#apache-airflow). | Candidate through AdbcHook with the Flight SQL driver, or Python tasks; untested. | None verified. | [Apache Airflow Redis provider / RedisHook](https://airflow.apache.org/docs/apache-airflow-providers-redis/stable/_api/airflow/providers/redis/hooks/redis/index.html), Apache Airflow: get_conn() exposes a native Redis client for command calls. |
| 9 | Dagster | ✅ 🔌 ✈️ | Data orchestration | **Yes, through a community integration.** `dagster-adbc` provides `ADBCResource`. [Integration guide](https://docs.dagster.io/integrations/libraries/adbc). | **Tested repository integration:** `adbc_driver_redis.dagster.RedisADBCIOManager` supports Arrow, pandas, Polars, and reader inputs/outputs, including partition tests. Install the `dagster` extra; [guide](https://github.com/redis/redis-adbc-driver/blob/main/docs/python-integrations.md#dagster). The community `dagster-adbc` 0.0.2 `ADBCResource` is also locally validated with manifest or library-path discovery and explicit `autocommit=True`; [evidence](https://github.com/redis/redis-adbc-driver/blob/main/docs/sdlc/changes/dagster-adbc-resource/validation.md). | Candidate through ADBCResource with driver="flightsql"; untested. | None verified. | Community dagster-adbc supplies generic ADBC integration. This repository's Redis-specific IO manager is listed in the Redis ADBC column; its presence does not establish Redis, Inc. maintenance. No separate native-command connector verified. |
| 10 | Prefect | ✅ 🔌 🤝 | Python workflow orchestration | **Custom Python integration.** Tasks can call ADBC libraries; a dedicated ADBC connector was not verified. [Python task API](https://reference.prefect.io/prefect/tasks/). | **Tested with Prefect 3.9.0 and Redis Software 8.6.2:** local flows, Arrow ingestion/fetch, dependencies, concurrent queries, retries, and SQL errors. [Usage](https://github.com/redis/redis-adbc-driver/blob/main/docs/python-integrations.md#prefect-custom-python-tasks), [test evidence](https://github.com/redis/redis-adbc-driver/blob/main/docs/sdlc/changes/prefect-integration/plan.md). | Custom Python tasks using adbc_driver_flightsql; untested. | None verified. | [prefect-redis](https://docs.prefect.io/integrations/prefect-redis), Prefect: RedisDatabase, locks, and messaging. Its [RedisDatabase.get_client()](https://reference.prefect.io/prefect_redis/blocks/#prefect_redis.blocks.RedisDatabase.get_client) exposes a native Redis client for direct command calls; this tool-specific integration is distinct from installing redis-py alone. |
| 11 | Apache Kafka / Kafka Connect | ✅ 🧩 | Event streaming and integration | **No standard ADBC connector verified.** The common Confluent database connector uses JDBC. [JDBC connector](https://docs.confluent.io/kafka-connectors/jdbc/10.8/overview.html). | Custom Kafka Connect connector required; the JDBC connector cannot directly load an ADBC library. | Candidate through a JDBC source connector plus Flight SQL JDBC; verify polling/incremental SQL and type mappings. Not CDC. | [Redis Kafka Connector](https://redis-field-engineering.github.io/redis-kafka-connect/), Redis Field Engineering: sinks and keys/Streams sources; full command API / Query Engine access not established. | Confluent JDBC connector is a Flight SQL candidate, not a verified unrestricted Redis command connector. |
| 12 | Apache Flink | ✅ 🧩 | Stream processing | **No built-in generic ADBC connector verified.** Its documented relational database connector uses JDBC. [JDBC connector](https://nightlies.apache.org/flink/flink-docs-stable/docs/connectors/table/jdbc/). | Custom source/sink integration required; untested. | Candidate through the JDBC connector plus Flight SQL JDBC; validate dialect, types, batching, and streaming semantics. | [Redis Flink connector](https://github.com/redis-field-engineering/redis-flink-connector-dist), Redis Field Engineering / Redis, Inc.: Streams sources and Streams/JSON/Hash sinks. Full command API / Query Engine access not established. | Apache Flink JDBC connector is a Flight SQL candidate; check dialect and runtime compatibility. |
| 13 | Apache Beam | ❌ | Batch and streaming pipelines | **No built-in generic ADBC I/O verified.** Custom code is possible; documented database I/O includes JDBC. [JDBC I/O](https://beam.apache.org/releases/pydoc/current/apache_beam.io.jdbc.html). | Custom Beam I/O required; untested. | Candidate through Java JdbcIO / Python JDBC I/O with Flight SQL JDBC; runner packaging and query/write behavior need validation. | None verified. | [Java RedisIO](https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/io/redis/RedisIO.html), Apache Beam: key/value reads/writes and Streams writes. Full command API / Query Engine access not established. |
| 14 | Apache NiFi | ❌ | Visual data flows | **No standard ADBC processor verified.** Its standard SQL processor uses JDBC. [ExecuteSQL](https://nifi.apache.org/components/org.apache.nifi.processors.standard.ExecuteSQL/). | Custom processor or script integration required; untested. | Candidate through JDBC processors and Flight SQL JDBC; validate JVM options, metadata, and result conversion. | None verified. | [Redis connection/cache services](https://nifi.apache.org/components/org.apache.nifi.redis.service.RedisDistributedMapCacheClientService/), Apache NiFi: Redis-backed caching. Full command API / Query Engine access not established. |
| 15 | Airbyte | ❌ | Data replication | **No generic ADBC connector verified.** An ADBC driver alone does not automatically become an Airbyte connector. [Connector catalog](https://docs.airbyte.com/integrations), [ADBC feature request](https://github.com/airbytehq/airbyte/issues/24546). | Custom Airbyte connector required; untested. | Custom connector required; no generic Flight SQL connector verified. | None verified. | [Marketplace Redis destination](https://docs.airbyte.com/integrations/destinations/redis), Airbyte ecosystem: data-structure writes. Redis source and full command API / Query Engine access not verified. |
| 16 | Fivetran | ❌ | Managed data replication | **Custom integration candidate.** Its Python Connector SDK can use database libraries, but ADBC packaging and runtime compatibility need validation. [SDK guide](https://fivetran.com/docs/connector-sdk/getting-started). | Custom Connector SDK source; binary packaging/runtime compatibility untested. | Custom Connector SDK source using the Python Flight SQL driver; untested. | None verified. | [Community Redis Connector SDK example](https://github.com/fivetran/community_connectors/tree/main/redis), Fivetran community: reads keyspace/TimeSeries data for replication. Does not expose unrestricted Redis API or Query Engine commands as a tool connector; custom SDK code could be extended. |
| 17 | dlt | ✅ 🔌 | Python data loading | **Yes, for selected destinations and custom integrations.** This repository supplies an ADBC ingestion destination. [Guide](https://github.com/redis/redis-adbc-driver/blob/main/docs/python-integrations.md#dlt). | `adbc_driver_redis.dlt.redis_adbc` is an append-only custom destination using normalized Parquet Arrow batches and `adbc_ingest`; install the `dlt` extra. Earlier live runs covered all 12 dlt tests; public-base validation was **5 passed, 7 skipped**, with no live Redis run on that base. [Evidence](https://github.com/redis/redis-adbc-driver/blob/main/docs/sdlc/changes/dlt-integration/validation.md). | No Flight SQL destination validated; custom Python resource loading remains untested. | None verified. | This repository's [dlt destination](https://github.com/redis/redis-adbc-driver/blob/main/python/adbc_driver_redis/dlt.py) supplies the ADBC path; no Redis, Inc. maintenance or support claim. Append only; replace/merge are rejected, and retries can duplicate partially written rows. |
| 18 | pandas | ✅ 🔌 ✈️ | DataFrame processing | **Yes, directly.** SQL read APIs accept ADBC connections through the Python driver manager. [Integration guide](https://arrow.apache.org/adbc/main/integrations.html). | Tested `read_sql` / `read_sql_query` and `DataFrame.to_sql` with an open Redis ADBC connection: nullable typed round trips, append, replacement, filtering/aggregation, empty reads, and multiple Arrow batches. Combined read/write suite: **12 passed**. [Evidence](https://github.com/redis/redis-adbc-driver/blob/main/docs/sdlc/changes/pandas-smoke/validation.md). | Pass an open ADBC Flight SQL connection to read_sql; tool-level Redis integration untested. | None verified. | No dedicated Redis connector verified. Custom extraction can feed [pandas.DataFrame.from_records](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.from_records.html). |
| 19 | Polars | ✅ 🔌 ✈️ | Fast DataFrame processing | **Yes, directly.** Database read/write APIs support ADBC connections. [Integration guide](https://arrow.apache.org/adbc/main/integrations.html). | **Locally validated, 2026-10-29:** connection/URI reads, typed write/append round trips, filtering and aggregation, NULLs, and empty-result schemas. The unchanged [Polars regression suite](https://github.com/redis/redis-adbc-driver/blob/main/python/tests/test_polars.py) plus driver tests passed all 8 cases with warnings treated as errors on Redis Software 8.6.2 with verified 4x QPF. [Evidence and reproduction](https://github.com/redis/redis-adbc-driver/blob/main/docs/sdlc/changes/polars-regression-rerun/validation.md). | Pass an open ADBC Flight SQL connection to read_database; tool-level Redis integration untested. | None verified. | No dedicated Redis connector verified. Custom extraction can feed [polars.from_dicts](https://docs.pola.rs/api/python/stable/reference/api/polars.from_dicts.html). |
| 20 | DuckDB | ✅ 🔌 ✈️ | Embedded analytical SQL | **Yes, in both directions.** DuckDB exposes an ADBC driver, and distinct community extensions consume drivers: [Query.Farm adbc_scanner](https://query.farm/products/extensions/adbc_scanner/) and [Columnar adbc](https://duckdb.org/community_extensions/extensions/adbc). | **Tested:** Query.Farm `adbc_scanner` attachment, reads/joins, ingestion, and autocommit writes; [guide](https://github.com/redis/redis-adbc-driver/blob/main/docs/duckdb.md). Also tested: this repository's standalone [`redis_adbc`](https://github.com/redis/redis-adbc-driver/blob/main/duckdb-extension/README.md), which embeds the driver and streams SELECT results. The registry submission has merged for DuckDB 2.0; installation still requires a published build for the exact engine/platform. Source builds are documented. Columnar's separate `adbc` remains untested here. | Candidate through a generic ADBC extension with Flight SQL, or fetch Arrow in Python and register it; untested. The standalone `redis_adbc` extension connects directly to Redis and does not consume Flight SQL. | None verified. | Experimental [redis extension](https://duckdb.org/community_extensions/extensions/redis), lmangani/gigapi: selected Redis functions, not verified full API / Query Engine access. Generic `adbc_scanner` and `adbc` are separate projects; Redis test coverage is specific to the extension named in the Redis ADBC column. |
| 21 | Ray / Ray Data | ✅ 🔌 | Distributed Python and ML data processing | **Via this repository’s Redis adapter; no built-in generic ADBC reader claimed.** Ray’s documented Datasource/ReadTask interface supports worker-side Arrow blocks. [Extension API](https://docs.ray.io/en/latest/data/api/doc/ray.data.Datasource.html). | [adbc_driver_redis.ray.read_query](https://github.com/redis/redis-adbc-driver/blob/main/docs/python-integrations.md#worker-side-arrow-reader) plans live ADBC partitions and streams Arrow batches from worker-local connections. Source build with the ray extra, pinned to Ray 2.59.0; matching native library required on every worker. [Local validation](https://github.com/redis/redis-adbc-driver/blob/main/docs/sdlc/changes/ray-worker-164/validation.md). | Unverified. This adapter loads the Redis driver directly; it does not establish Flight SQL compatibility. | None verified. | This repository’s [Ray adapter](https://github.com/redis/redis-adbc-driver/blob/main/python/adbc_driver_redis/ray.py) supplies the ADBC path; Redis, Inc. authorship/support and upstream Ray endorsement are not established. Live unordered reads; no shared snapshot or stable retries. |
| 22 | Dask | ✅ 🔌 | Parallel DataFrames and Python computation | **Via this repository's Redis adapter; no built-in generic ADBC reader verified.** Dask SQL readers use SQLAlchemy URIs; custom readers can use from_map. [SQL integration](https://docs.dask.org/en/stable/dataframe-sql.html), [Custom loading](https://docs.dask.org/en/latest/generated/dask.dataframe.from_map.html). | [adbc_driver_redis.dask.read_query](https://github.com/redis/redis-adbc-driver/blob/main/docs/python-integrations.md#dask) provides lazy partitioned reads with worker-local ADBC connections and pandas Arrow dtypes. [Recorded local validation](https://github.com/redis/redis-adbc-driver/blob/main/docs/sdlc/changes/dask-353/validation.md) includes two worker processes; caller supplies partition parameters and exact schema. | Custom from_map loading with adbc_driver_flightsql and pandas partitions; untested. The bundled Redis helper does not establish Flight SQL compatibility. | None verified. | This repository's [Dask adapter](https://github.com/redis/redis-adbc-driver/blob/main/python/adbc_driver_redis/dask.py) supplies the ADBC path; Redis, Inc. authorship or support is not established. Native Redis command access is not claimed. |

"No support verified" means the checked documentation did not establish generic
support; it does not rule out custom or third-party integrations. JDBC connector
documentation establishes a documented connection path, not proof that no ADBC
integration exists. Arrow support alone does not imply ADBC support. Some linked
pages track development or latest versions, so check the installed release.

## Custom SDK code is not a connector

[redis-py](https://github.com/redis/redis-py) is a Redis client SDK. Users can write
custom tasks, notebooks, or resources with it, including calls to Query Engine
commands, but that work is not a pre-existing tool-specific connector. It is
excluded from both connector inventory columns and is not a solution indicator
on its own. ADBC and Flight SQL paths are evaluated separately.

## Implications for the Redis ADBC driver

Start with the implemented **Polars, Dagster, DuckDB, Airflow, and Prefect** paths in the table.
The [Python integration guide](https://github.com/redis/redis-adbc-driver/blob/main/docs/python-integrations.md) documents the published
[`adbc-driver-redis`](https://pypi.org/project/adbc-driver-redis/) package,
DB-API connection setup, TLS alias, and native driver manifests. Wheels bundle
the driver for Python 3.12+ (3.14 recommended for current source) on Linux amd64/arm64 (glibc 2.28+) and macOS arm64
(macOS 12+). Non-Python driver consumers still need a manifest or library path.
Redis with Search must run separately; see [server requirements](https://github.com/redis/redis-adbc-driver/blob/main/docs/driver-reference.md#server-requirements).
Writes autocommit, and the driver does not provide transaction rollback or isolation.

Direct pandas `read_sql` / `read_sql_query`, Polars reads/writes, and Dagster's
community resource now have dedicated validation records from October 29.
Prefect custom Python tasks also have an October 29 local validation record.
Those results supplement the earlier IO-manager and DuckDB evidence; they do
not establish Flight SQL compatibility or every package/platform combination.

Fabric / Power Query is another candidate with custom connector work. dbt has
community Redis adapters for Core 1.10–1.x and experimental v2 support in a custom
engine fork. The v1 Python package does not enable Redis in the standard v2
executable. Both documented paths load the native Redis ADBC driver directly;
Flight SQL remains unverified.
dlt now has this repository's append-only Redis ADBC destination. It does not
provide replace/merge, automatic schema migrations, rollback, or exactly-once
retries. Local pipeline state must be retained; see the [dlt guide](https://github.com/redis/redis-adbc-driver/blob/main/docs/python-integrations.md#dlt).
Fivetran would require a custom connector and validation of driver deployment.

Ray is ✅ 🔌 through this repository's worker-side Arrow reader. It uses
live ADBC partition descriptors, closes planning handles before worker reads,
and streams batches from independent worker connections. Install a build with
the `ray` extra and matching native library on every worker. SQL restrictions,
Search/ACL requirements, unordered live reads and retry limits remain explicit
in the [Ray guide](https://github.com/redis/redis-adbc-driver/blob/main/docs/python-integrations.md#worker-side-arrow-reader).
Upstream acceptance and broader rollout remain tracked in
[#164](https://github.com/redis/redis-adbc-driver/issues/164).

Dask is ✅ 🔌 through
this repository's partitioned read adapter. Its caller must supply disjoint,
exhaustive partition predicates and an exact Arrow schema; each partition is
materialized in worker memory, and tasks do not share a snapshot. Install the
package and native driver on every worker. See the
[Dask partitioning contract](https://github.com/redis/redis-adbc-driver/blob/main/docs/python-integrations.md#dask).

The package also ships [SQLAlchemy and Superset integrations](https://github.com/redis/redis-adbc-driver/blob/main/docs/python-integrations.md#sqlalchemy-and-superset),
outside this 22-tool selection. Their recorded checks cover reflection, bound SQL,
Superset discovery, dataset queries, and the SQL Lab cursor path; they do not
establish browser-rendered chart coverage or transactional ORM support.

## Testing status

These are **recorded results from the linked PRs**, not integration suites rerun
for this documentation refresh. The latest merged evidence is:

- [#226](https://github.com/redis/redis-adbc-driver/pull/226): initial direct pandas
  query reads and SQLAlchemy checks, **4 passed**. [Record](https://github.com/redis/redis-adbc-driver/blob/main/docs/sdlc/changes/pandas-read-sql/validation.md).
- [#227](https://github.com/redis/redis-adbc-driver/pull/227): community Dagster
  resource and existing IO manager, **14 passed**, zero skips. [Record](https://github.com/redis/redis-adbc-driver/blob/main/docs/sdlc/changes/dagster-adbc-resource/validation.md).
- [#228](https://github.com/redis/redis-adbc-driver/pull/228): expanded direct pandas
  query reads, **9 passed**, zero skips. This includes the two pandas cases
  introduced in #226; counts are not additive. [Record](https://github.com/redis/redis-adbc-driver/blob/main/docs/sdlc/changes/pandas-direct-reads/validation.md).
- [#229](https://github.com/redis/redis-adbc-driver/pull/229): unchanged Polars
  regression and driver tests, **8 passed**, zero skips, warnings treated as
  errors. [Record](https://github.com/redis/redis-adbc-driver/blob/main/docs/sdlc/changes/polars-regression-rerun/validation.md).

- [#230](https://github.com/redis/redis-adbc-driver/pull/230): Prefect local flows
  and tasks, **4 passed**, zero skips after rebuilding on public main. Coverage
  includes Arrow ingestion/fetch, dependencies, concurrent queries, retry after
  a committed write, and driver-error propagation. [Record](https://github.com/redis/redis-adbc-driver/blob/main/docs/sdlc/changes/prefect-integration/plan.md).

- [#231](https://github.com/redis/redis-adbc-driver/pull/231): Ray worker-side
  Arrow datasource, **17 passed**, zero skips in the final TLS suite, with three
  Ray DataContext deprecation warnings. [Record](https://github.com/redis/redis-adbc-driver/blob/main/docs/sdlc/changes/ray-worker-164/validation.md).

- [#232](https://github.com/redis/redis-adbc-driver/pull/232): packaged
  `RedisAdbcHook` for autocommit query helpers and bundled-library discovery.
  **8 passed**, zero skips: five focused Airflow hook cases plus installation
  and autocommit regression checks. The earlier two TaskFlow smoke tests were
  preserved but were not rerun for that adaptation. [Record](https://github.com/redis/redis-adbc-driver/blob/main/docs/sdlc/changes/airflow-get-records/validation.md).

- [#234](https://github.com/redis/redis-adbc-driver/pull/234): append-only dlt
  destination. Earlier source revisions recorded all **12 dlt tests** passing;
  the later pre-port rerun had **16 passed** including four driver tests. On
  public base `71625e1`, the library built and dlt tests reported **5 passed,
  7 skipped** without a live Redis fixture. That public-base check does not
  establish an end-to-end pipeline pass. [Record](https://github.com/redis/redis-adbc-driver/blob/main/docs/sdlc/changes/dlt-integration/validation.md).
- [#235](https://github.com/redis/redis-adbc-driver/pull/235): pandas writes,
  nullable typed round trips, append, filtering/aggregation, and existing-table
  modes. Combined read/write validation: **12 passed**, zero skips on public
  base `71625e1`. This includes the nine earlier read cases, so totals are not
  additive. [Record](https://github.com/redis/redis-adbc-driver/blob/main/docs/sdlc/changes/pandas-smoke/validation.md).

The #234 earlier live runs used macOS arm64, Python 3.12, dlt 1.31.0,
PyArrow 26.0.0 and ADBC manager 1.13.0. The later pre-port run verified Redis
Software 8.6.2 with active 4x QPF; the record distinguishes earlier sources
from the public base, where seven live cases skipped. A development wheel
packaging check is not evidence of a published native release wheel.
The #235 public-port run used macOS arm64, pandas 3.1.0, PyArrow 26.0.0,
ADBC manager 1.13.0, and Redis Software 8.6.2 with Search and verified 4x QPF.
Neither PR's record claims completed hosted CI or a full current-main suite.

The #232 current-main run used macOS arm64, Python 3.12.15, Airflow 3.3.2,
Arrow provider 0.1.1, common-sql 2.1.1, ADBC manager 1.12.0 and PyArrow 26.0.0.
Its Redis Software 8.6.2 fixture had verified active 4x QPF. TLS option
forwarding was checked; a live TLS handshake, Airflow 2.x and configured Linux
CI were not validated by that run.

The #231 run used macOS arm64, Python 3.12.14, Ray 2.59.0, PyArrow 26.0.0,
ADBC manager 1.13.0, and a disposable one-shard Redis Software 8.6.2 fixture
with Search, verified active 4x QPF, and verified TLS. Its two-CPU Ray runtime
used separate local worker processes, not local mode. This does not validate
multi-node deployments, Linux execution, release wheels, or performance.

The #230 run used macOS arm64, Python 3.15.0, Prefect 3.9.0, PyArrow 26.0.0,
ADBC manager 1.13.0, and a disposable one-shard Redis Software 8.6.2 RAM database
with Search and verified active 4x QPF. Remote workers, Prefect Cloud,
`prefect-redis`, Flight SQL, and Linux CI results were not validated by that run.

The final #227–#229 runs used macOS arm64, Python 3.12, ADBC manager 1.13.0,
PyArrow 26.0.0, and Redis Software 8.6.2 with verified active 4x QPF. Package
versions were Dagster 1.13.27 / dagster-adbc 0.0.2, pandas 3.1.0, and Polars 2.1.0,
respectively. Each record identifies its source revision and tested topology.
Older IO-manager, wheel, DuckDB, and Superset evidence remains linked to the
[2026-10-20 Python verification record](https://github.com/redis/redis-adbc-driver/blob/main/docs/sdlc/changes/271-280/validation.md) and
[DuckDB record](https://github.com/redis/redis-adbc-driver/blob/main/docs/sdlc/changes/duckdb-community/validation.md); those used different
versions and a disposable Redis 8.6.2 with Search, not the later Software/QPF setup.

| Path | Recorded evidence | Limits and remaining gaps |
|---|---|---|
| Direct pandas SQL reads/writes | [Read/write tests](https://github.com/redis/redis-adbc-driver/blob/main/python/tests/test_pandas_writes.py) and [record](https://github.com/redis/redis-adbc-driver/blob/main/docs/sdlc/changes/pandas-smoke/validation.md): **12 passed** across three suites; reads, nullable typed round trips, append, replacement, filtering/aggregation, NULLs, empty results, and multiple Arrow batches. | Table-read APIs, bound query parameters, `chunksize`, TLS, Flight SQL, clusters, other platforms, and other versions were not validated. Replacement drops/recreates the table; writes autocommit without rollback. |
| dlt append destination | [Tests](https://github.com/redis/redis-adbc-driver/blob/main/python/tests/test_dlt.py) and [record](https://github.com/redis/redis-adbc-driver/blob/main/docs/sdlc/changes/dlt-integration/validation.md): 12 dlt cases passed on earlier sources; public-base run **5 passed, 7 live cases skipped**. | No live public-base result. Append only; replace/merge rejected. Failed writes/checkpoint replay can duplicate rows; no automatic schema migration or restored source state from Redis. Hosted CI and release-wheel compatibility remain unverified by that record. |
| DB-API and Polars | [Polars and driver tests](https://github.com/redis/redis-adbc-driver/blob/main/docs/sdlc/changes/polars-regression-rerun/validation.md): **8 passed**, including typed connection/URI reads, writes/appends, filtering, aggregation, NULLs, and empty-result schemas. An earlier clean-wheel smoke run recorded **4 passed**. | Latest rerun used a source build; wheels, other platforms, Cloud, cluster topologies, TLS handshakes, Flight SQL, and performance were not validated by that rerun. |
| Ray worker-side reader | [Tests](https://github.com/redis/redis-adbc-driver/blob/main/python/tests/test_ray_datasource.py) and [record](https://github.com/redis/redis-adbc-driver/blob/main/docs/sdlc/changes/ray-worker-164/validation.md): **17 passed**, including partition/type coverage, empty schemas, parameters, worker-local streaming, cleanup/cancellation, retry, and TLS certificate rejection. | Source build pinned to Ray 2.59.0 with a matching native library on each worker. Live unordered reads have no shared snapshot or stable retries. Multi-node, abrupt process death, other versions/platforms, Flight SQL, and release wheels remain unverified. |
| Prefect custom Python tasks | [Tests](https://github.com/redis/redis-adbc-driver/blob/main/python/tests/test_prefect.py) and [record](https://github.com/redis/redis-adbc-driver/blob/main/docs/sdlc/changes/prefect-integration/plan.md): **4 passed**, zero skips; local orchestration, Arrow values/types, dependencies, concurrency, retries, and SQL errors. | Custom tasks, not a packaged ADBC connector. Remote workers, Prefect Cloud, `prefect-redis`, Flight SQL, and other platforms remain untested. Writes autocommit; retries need idempotent writes to avoid duplicates. |
| Airflow TaskFlow and packaged hook | Earlier [TaskFlow record](https://github.com/redis/redis-adbc-driver/blob/main/docs/sdlc/changes/airflow-smoke/validation.md): **2 passed**. [Hook record](https://github.com/redis/redis-adbc-driver/blob/main/docs/sdlc/changes/airflow-get-records/validation.md): **8 passed** across five hook cases and installation/autocommit checks; query values, parameters, NULLs, empty/multiple results, writes, option forwarding, and transaction rejection. | Import `RedisAdbcHook` explicitly: it is not a registered provider, so `SQLExecuteQueryOperator` still selects the generic Arrow hook for an `adbc` connection. Use `autocommit=True` for inherited `insert_rows()`. Remote executors, Flight SQL, Airflow 2.x, and a live hook TLS handshake remain untested. |
| Dagster community resource and IO manager | [Record](https://github.com/redis/redis-adbc-driver/blob/main/docs/sdlc/changes/dagster-adbc-resource/validation.md): **14 passed** (six community cases, eight IO-manager cases), including library/manifest discovery, ingestion/fetch, bound values, option forwarding, reconnect persistence, assets, and cleanup. | Use `autocommit=True` for ADBCResource. Other platforms, TLS, Redis Cluster API, and other versions remain untested by this run. Multi-dimensional partitions and atomic/transactional replacement are unsupported by the IO manager. |
| DuckDB via Query.Farm `adbc_scanner` | [Test](https://github.com/redis/redis-adbc-driver/blob/main/python/tests/test_duckdb.py) and [verification record](https://github.com/redis/redis-adbc-driver/blob/main/docs/sdlc/changes/duckdb-community/validation.md): reads, joins, ingestion, direct INSERT/CTAS, read-only enforcement, and transaction rejection. | Columnar's `adbc` extension, Flight SQL, older extension builds, and universal predicate pushdown. |
| Standalone DuckDB `redis_adbc` | [Native tests](https://github.com/redis/redis-adbc-driver/blob/main/duckdb-extension/test/test_native.py): **16 passed** with DuckDB 1.5.6; matching CLI harness: **12 scenarios** on both 1.5.6 and the pinned DuckDB 2.0 registry source, checking 10,003 rows. [Record](https://github.com/redis/redis-adbc-driver/blob/main/docs/sdlc/changes/duckdb-community/validation.md). | Registry publication for every engine/platform remains unverified; the current build targets DuckDB 2.0. Writes, attached catalogs, and bound query parameters are unsupported; this extension is a SELECT-only interface. |
| SQLAlchemy / Superset (outside the table) | Separate Superset environment: **3 passed**, including SQLAlchemy reflection and Superset dataset / SQL Lab paths. [Tests](https://github.com/redis/redis-adbc-driver/blob/main/python/tests/test_superset.py). | Browser-rendered charts, broad application workflows, and transaction semantics beyond autocommit. |

The Python suite's skipped Superset test ran separately in its own environment.
Configured CI jobs are not evidence that every job has passed; consult the linked
records for platform limits and the workflows for reproducible commands. Historical
passes do not certify the current checkout or latest upstream releases.

The [Python ADBC validation suite](https://github.com/redis/redis-adbc-driver/blob/main/go/validation/tests/) exercises the Redis
shared library, including queries, metadata, ingestion, errors, and cancellation.
PyArrow is used by the [row-value tests](https://github.com/redis/redis-adbc-driver/blob/main/go/validation/tests/test_row_values.py)
and [sample loader](https://github.com/redis/redis-adbc-driver/blob/main/go/examples/load_sample.py). See the README's
[Compatibility and validation](https://github.com/redis/redis-adbc-driver/blob/main/README.md#compatibility-and-validation) and
[Python usage](https://github.com/redis/redis-adbc-driver/blob/main/docs/driver-reference.md#python-usage) for recorded results and connection setup.
This coverage does not establish end-to-end compatibility with every tool above.

Ray's [bridge validation record](https://github.com/redis/redis-adbc-driver/blob/main/docs/sdlc/changes/ray-356/validation.md) and
[regression tests](https://github.com/redis/redis-adbc-driver/blob/main/python/tests/test_ray_arrow.py) cover Redis ADBC → Arrow
table → Ray Data with Ray 2.59.0, including SELECT 1, nullable scalar types,
empty-result schemas and a normal worker-side map_batches operation. All three
tests passed locally on macOS arm64 against standalone Redis 8.6.2. This is a
source-build, small-data bridge test, not distributed Redis ingestion, a
multi-node Ray test or evidence for every release wheel or deployment.

Dask has a separate [local validation record](https://github.com/redis/redis-adbc-driver/blob/main/docs/sdlc/changes/dask-353/validation.md)
for partitioned Redis ADBC reads, including two worker processes, range and NULL
partitions, empty results, Arrow dtypes, and connection cleanup. This is recorded
source-install validation, not evidence for every deployment or Flight SQL.

The companion [dbt adapter project](https://github.com/alberttwong/redis-dbt-adapter-with-project)
records separate end-to-end validation. Its [v1 guide](https://github.com/alberttwong/redis-dbt-adapter-with-project/blob/main/v1/README.md#validation-and-testing)
describes full dbt workflows and the dbt adapter functional suite for Core
1.10–1.x. The [v2 validation record](https://github.com/alberttwong/redis-dbt-adapter-with-project/blob/main/v2/VALIDATION.md)
reports macOS arm64 and Linux coverage with dbt-oss 2.0.8, Redis 8.6.2 and Redis
ADBC v0.0.16, using custom engine commit
`90178580bd6f3353d758fff770fdc2c58c985ad0`. The linked
[Linux CI run](https://github.com/alberttwong/redis-dbt-adapter-with-project/actions/runs/39623533131)
completed successfully on October 21, 2026. Its recorded coverage includes smoke
and feature fixtures, trusted TLS, and the full taxi workflow: 82 successful
build results plus one configured source warning, and 82 successes for the empty
build. Python table models, snapshots, contracts, merge, microbatch and persisted
docs are included in the recorded scenarios. This is experimental custom-engine
support; the standard dbt v2 executable does not include Redis, static analysis
remains disabled, and these results do not establish Flight SQL compatibility or
support for every deployment. SQL grants and materialized views remain unsupported
in both adapters.

The [differential harness](https://github.com/redis/redis-adbc-driver/blob/main/go/pgdiff/README.md) replays captured dbt-postgres SQL
through Redis ADBC. It does not validate a Redis dbt adapter or a full dbt run,
snapshots, or documentation generation against Redis.

The related [Redis Arrow Flight server](https://github.com/alberttwong/redis-apache-arrow-flight#development)
documents Python ADBC, Go, and JDBC client coverage. Those results do not establish
tests of each application against this driver's current checkout. Application
configuration, metadata discovery, generated SQL, type mappings, authentication,
and refresh behavior still require validation where applicable.

No integration suites were rerun for this refresh. The claims above cite historical
results. SQL syntax similarity alone does not establish JDBC, ODBC, or PostgreSQL
wire-protocol compatibility, or SQLAlchemy compatibility beyond the explicitly
tested dialect operations.

