# Client guide

[Back to the README](../README.md). Examples and setup for ADBC, native Flight
and JDBC clients. Connect to a running server before using these examples.

## Examples


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
[tested integration coverage](../README.md#tested-integration-coverage). The URL is:

```text
jdbc:arrow-flight-sql://localhost:8815/?useEncryption=false
```

- **Logging in:** with `--auth=redis-acl`, add `&user=<redis user>&password=<…>`
  to the URL. The driver logs in with the Flight handshake and keeps the bearer
  token.
- **TLS:** use `useEncryption=true`, plus `trustStore` or
  `disableCertificateVerification` as needed.
- **Sessions:** the driver keeps cookies, so each JDBC connection is a
  [session](../docs/server-guide.md#sessions): `SET`, `search_path` and temporary tables last from one
  statement to the next. Plain scans [stream](../README.md#flight-sql-support) from Redis
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

## Data tools


See the [versioned client validation matrix](../docs/client-compatibility.md) for
checks actually run and known client limitations.

The mirrored [ADBC support across 22 data engineering tools](https://github.com/redis/redis-apache-arrow-flight/blob/main/docs/data-engineering-adbc-support.md)
compares Redis ADBC, Flight SQL, and connector options, with upstream validation evidence.

Use a **Flight SQL driver** to query SQL tables, or a **Flight client** to
upload and download named datasets. Tools that consume Arrow tables can
work with the results of either interface.

| Tool | How to use it with this project | Useful for | Tested |
|-|-|-|-|
| **DBeaver** | Configure the [Arrow Flight SQL JDBC driver](https://arrow.apache.org/java/current/flight_sql_jdbc_driver.html) with the URL below; see [JDBC setup](#jdbc-dbeaver-and-others) for JVM options, authentication and sessions | Interactive SQL, browsing schemas and tables, viewing query results | Blocked — DBeaver 26.2.3 crashed in macOS accessibility handling before connecting; underlying [JDBC smoke test](../examples/jdbc/JdbcSmoke.java) passed ([record](../docs/client-compatibility.md#dbeaver-application-attempt-superset-excluded)). |
| **Apache Superset** | Superset [requires a DB-API driver and SQLAlchemy dialect](https://superset.apache.org/user-docs/6.0.0/configuration/databases/). A candidate adapter is the third-party, beta [`sqlalchemy-adbc[flightsql]`](https://pypi.org/project/sqlalchemy-adbc/), using `adbc+flightsql://localhost:8815`; Superset 6.1.0 and adapter 0.3.0 have incompatible SQLAlchemy requirements | SQL exploration, charts and dashboards; requires adapter setup and compatibility testing | Blocked — Superset 6.1.0 and sqlalchemy-adbc 0.1.0–0.3.0 require incompatible SQLAlchemy versions ([record](../docs/client-compatibility.md#flight-client-validation-on-2026-10-29)). |
| **Power BI** | Build a [custom ADBC-based Power Query connector](https://learn.microsoft.com/en-us/power-query/adbc) using the Flight SQL driver and server URI below; adapt the sample's type mappings, navigation queries and SQL generation to this server. This project does not ship a Power BI connector | Reports and dashboards; requires custom connector development and compatibility testing | No — custom connector and application integration untested. |
| **Tableau** | Try [Other Databases (JDBC)](https://help.tableau.com/current/pro/desktop/en-gb/examples_otherdatabases_jdbc.htm) with the Arrow Flight SQL JDBC driver and URL below; configure the [driver's JVM options](#jdbc-dbeaver-and-others) and validate metadata and generated SQL. Tableau's generic JDBC connector does not guarantee feature compatibility | Visual analysis and dashboards; requires driver setup and compatibility testing | No — application integration untested. |
| **Other JDBC applications** | Load `org.apache.arrow:flight-sql-jdbc-driver` and use the same JDBC URL, if the application supports custom JDBC drivers | Java applications and data tools that allow a custom driver | Driver only — Flight SQL JDBC 19.0.0 passed on Java 25; individual applications untested ([record](../docs/client-compatibility.md)). |
| **Python / PyArrow** | Use [ADBC Flight SQL](https://arrow.apache.org/adbc/current/driver/flight_sql.html) (`adbc_driver_flightsql`) for SQL and [`pyarrow.flight`](https://arrow.apache.org/docs/python/flight.html) for datasets; see [Client examples](../README.md#client-examples) | Queries, bulk ingest, Arrow table transfers and ML batches | Yes — ADBC Flight SQL and PyArrow Flight smoke scripts passed ([record](../intent/driver-v0.0.17/plan.md#validation)). |
| **pandas** | Pass an ADBC Flight SQL connection to [`pd.read_sql`](https://pandas.pydata.org/docs/reference/api/pandas.read_sql.html) (pandas 2.2+), or convert a downloaded Arrow table with `table.to_pandas()` | Dataframe analysis and reporting | Yes — `pd.read_sql` results, NULLs and empty results passed; ADBC `params=` is unsupported ([record](../docs/client-compatibility.md#flight-client-validation-on-2026-10-29)). |
| **Polars** | Pass an ADBC Flight SQL connection to [`pl.read_database`](https://docs.pola.rs/api/python/stable/reference/api/polars.read_database.html), or convert a downloaded Arrow table with `pl.from_arrow(table)` | Dataframe transformations and analytics | Yes — `pl.read_database`, NULLs, empty results and batch iteration passed. ([record](../docs/client-compatibility.md#flight-client-validation-on-2026-10-29)). |
| **DuckDB** | Fetch an Arrow table through ADBC or Flight, then [query it locally in DuckDB](https://duckdb.org/docs/current/guides/python/import_arrow); this uses an Arrow transfer rather than a direct DuckDB connection to the server | Joining Redis results with local files or other Arrow data | Yes — local SQL over Arrow fetched through Flight SQL and Flight RPC passed. ([record](../docs/client-compatibility.md#flight-client-validation-on-2026-10-29)). |
| **R** | Use R `adbcdrivermanager` with the [Flight SQL shared driver](https://arrow.apache.org/adbc/current/driver/flight_sql.html); see [the R smoke script](../examples/r/flight_sql.R) | SQL queries and Arrow-based analysis in R | Yes — driver-manager queries, DDL, writes, NULLs and empty results passed; DBI/adbi untested. ([record](../docs/client-compatibility.md#flight-client-validation-on-2026-10-29)). |
| **Go / Arrow Java** | Use the Arrow Flight SQL client for SQL, or the Flight client for dataset `DoPut` / `DoGet`; see [Other clients](#other-clients) and the [Flight API](https://arrow.apache.org/docs/format/Flight.html) | Applications and data pipelines | Partial — Go Flight SQL / Flight integration suites passed; Java coverage is JDBC only ([record](../docs/client-compatibility.md)). |
| **Redis Insight** | [Connect directly to the backing Redis database](https://redis.io/docs/latest/develop/tools/insight/), rather than the Flight endpoint | Inspecting SQL row HASHes, search indexes and Redis memory usage; dataset payloads remain encoded Arrow IPC | No — no application validation recorded. |

Local connection addresses (without TLS):

```text
Flight / ADBC: grpc://localhost:8815
JDBC:          jdbc:arrow-flight-sql://localhost:8815/?useEncryption=false
```

See [tested integration coverage](../README.md#tested-integration-coverage) for the ADBC,
JDBC and native Flight checks. Tested status reflects recorded validation
against this Flight server; the linked
records give versions, dates, and scope. Tests of the Redis ADBC driver in the
mirrored comparison do not establish Flight server integration coverage. No
additional Go suites were rerun for these client-only changes. Run
`make clients-smoke` for pandas, Polars and DuckDB, and
`make r-smoke FLIGHT_SQL_DRIVER=/path/to/library` for R. The dataframe smoke
test is included in the Test workflow; R remains a local target.

For SQL, use autocommit: transactions are not supported. A tool's PostgreSQL
or generic Redis connector does not connect to the Flight endpoint; select
Flight SQL or install its driver. SQL tools see SQL tables, while named
datasets are accessed through Flight's dataset API.

