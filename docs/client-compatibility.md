# Client compatibility

Validated locally on 2026-10-20 against Redis 8.6.2. The server uses Arrow Go
18.8.0, Apache ADBC 1.12.0 and Redis ADBC revision a66467bc5e6a. The local run
used macOS arm64 for Go/Python and a disposable Temurin Java 25 Docker image.
Hosted workflow configuration is not itself evidence of a passing run.

| Client | Version | Automated coverage | Local evidence / limitations |
| --- | --- | --- | --- |
| Go Flight SQL / Flight RPC | arrow-go 18.8.0 | Metadata schemas/types/capabilities, prepared parameters, ingestion, scans, sessions, cancellation, datasets, errors and cleanup; standalone/Cluster race suites | Passed. GetXdbcTypeInfo queried directly, including type filters and real DDL |
| Python ADBC Flight SQL | driver/manager 1.12.0 | quickstart.py and dataset_to_sql.py through Test workflow / make example | Passed: metadata, parameters, queries/updates, ingestion and explicit dataset projection/import |
| Python PyArrow Flight | 26.0.0 | Same Python scripts | Passed: nested/dictionary datasets, multiple batches, projections/ranges, discovery and drop |
| Flight SQL JDBC | 19.0.0 | JdbcSmoke.java; workflow runs Java 17/21/25 | Local Java 25 passed metadata, parameters, update, session state and concurrent statement/read checks. Java 17/21 were not rerun locally |
| pandas | 3.0.6 | Python tour converts a query result to a dataframe | Passed conversion; no claim of arbitrary pandas SQL API coverage |
| DBeaver | Not manually validated on 2026-10-20 | Underlying JDBC smoke only at that date | Later manual connection, navigator and SELECT passed; see [2026-10-30 evidence](#dbeaver-rerun-and-manual-validation-on-2026-10-30) |
| Tableau | Not validated | None | Requires generic JDBC setup and actual generated-SQL/metadata validation |
| Power BI | Not validated | None | Requires a custom ADBC Power Query connector; none is shipped |
| Polars, DuckDB, R | Not validated | None | Arrow/ADBC consumption examples are not a compatibility guarantee |

## Driver v0.0.17 follow-up validation

On 2026-10-22, Redis ADBC v0.0.17 (revision `9cc8e25c0f01`) passed the Go
standalone and Cluster race suites against disposable Redis 8.6.2, plus both
Python smoke scripts against the rebuilt server. Python pins were ADBC
driver/manager 1.12.0, PyArrow 26.0.0 and pandas 3.1.0. JDBC and manual BI
clients were not rerun. See the [validation record](../intent/driver-v0.0.17/plan.md).
The earlier matrix retains its original versions and evidence.

## Flight client validation on 2026-10-29

Source: `84ea021` plus the client smoke scripts added with this validation.
The community server was built on macOS arm64 against its pinned Redis ADBC
revision `9cc8e25c0f01`. Tests used a new disposable Redis 8.6.2 container
on port 16389 and a Flight endpoint on port 18815. Clients ran in Linux arm64
containers. These results supplement the historical matrix above.

| Client | Versions exercised | Result and scope |
| --- | --- | --- |
| pandas `pd.read_sql` | Python 3.11, pandas 3.1.0, ADBC driver/manager 1.12.0, PyArrow 26.0.0 | PASS ordered results, NULLs and empty results through Flight SQL. `params=` raises `NotImplementedError` in pandas's ADBC path; direct ADBC binding followed by Arrow-to-pandas conversion passes |
| Polars `pl.read_database` | Polars 2.1.0, same ADBC/PyArrow versions | PASS ordered results, NULLs, empty results and batch iteration through an autocommit Flight SQL connection |
| DuckDB | DuckDB 2.0.0, same PyArrow version | PASS local aggregates over Flight SQL Arrow results and ordered/NULL results over a Flight RPC dataset uploaded in multiple batches and downloaded from every endpoint. Uses Arrow transfer into local DuckDB; no native DuckDB Flight extension tested |
| R | R 4.5.2, adbcdrivermanager 0.25.0.9000, nanoarrow 0.9.0.9000, Flight SQL shared driver 1.12.0 | PASS DDL, INSERT, ordered query, NULLs, empty results and SUM through the driver manager. DBI/adbi not tested |
| DBeaver / its JDBC driver | DBeaver Community 26.2.3, bundled Temurin 25.0.4.1; Flight SQL JDBC 19.0.0 | JDBC smoke PASS: metadata, parameters, updates, sessions and 100,000-row concurrent read. Application test BLOCKED: repeated macOS accessibility serialization crashes on the custom connection form before a connection was attempted; browsing/generated SQL remain unvalidated |
| Apache Superset | Superset 6.1.0, sqlalchemy-adbc 0.1.0, 0.2.0 and 0.3.0 | BLOCKED before application execution: Superset requires SQLAlchemy `>=1.4,<2`; all three adapter versions require `>=2.0`. pip returns `ResolutionImpossible`. No dashboard or SQL Lab pass is claimed |

The R packages came from `https://apache.r-universe.dev` with CRAN as fallback.
These are development versions, not release pins. `adbcflightsql` was unavailable
from those repositories for this runtime. Instead, the R test loaded the official
Linux arm64 `libadbc_driver_flightsql.so` from the Python 1.12.0 wheel through
`adbc_driver(path, entrypoint = "AdbcDriverFlightSQLInit")`. Supply the driver
library built for the R runtime's OS and architecture; the Python installation
is only a source of that shared library and is not used to execute R queries.

### Reproduce dataframe and R checks

Against a disposable server, run:

```sh
make clients-smoke FLIGHT_URI=grpc://localhost:8815
# Or install requirements-clients.txt into an isolated Python environment:
python -m pip install -r examples/python/requirements-clients.txt
python examples/python/client_compatibility.py --uri grpc://localhost:8815

# Install R's adbcdrivermanager and nanoarrow first, then provide the library:
make r-smoke FLIGHT_URI=grpc://localhost:8815 FLIGHT_SQL_DRIVER=/path/to/libadbc_driver_flightsql.so
```

Scripts create unique tables/datasets and clean up their fixtures. Their scope
is an unauthenticated local endpoint; TLS/authentication and arbitrary generated
BI SQL are not covered. The dataframe script reports the pandas parameter
limitation explicitly and verifies the direct-ADBC workaround. Its remaining
assertions must all pass. The Test workflow now invokes it; hosted CI was not
run during this local validation. R remains an explicit local target.

Reproduce the Superset installation failure in a fresh Python 3.11 environment:

```sh
python -m pip install 'apache-superset==6.1.0' 'sqlalchemy-adbc[flightsql]==0.3.0'
```

Do not force incompatible dependencies with `--no-deps` and count that as a
supported installation. This blocks this specific adapter route, not every
possible future Superset Flight integration.

### DBeaver application attempt (Superset excluded)

After the user requested continuing without Superset, DBeaver Community 26.2.3
was downloaded from the official site into a temporary macOS arm64 installation
with an isolated workspace. Its bundled Temurin 25.0.4.1 already included the
README's two Arrow JVM options. The connection picker had no Arrow entry.
Driver Manager successfully saved a custom generic driver with these settings:

```text
Name:         Redis Flight SQL
Class:        org.apache.arrow.driver.jdbc.ArrowFlightJdbcDriver
URL template: jdbc:arrow-flight-sql://{host}:{port}/?useEncryption=false
Default port: 18815 (isolated test endpoint; normal project default is 8815)
Library:      flight-sql-jdbc-driver-19.0.0.jar
```

Opening the custom connection form repeatedly crashed DBeaver during UI
automation, including an attempt using screenshots instead of accessibility
tree inspection. Three local macOS crash reports recorded `EXC_BAD_ACCESS` /
`SIGABRT`; the inspected stack included `CFStringGetLength`,
`cfAttributedStringSerialize`, `AXSerializeCFType` and
`_AXXMIGCopyParameterizedAttributeValue`. This is a local application/automation
blocker, not a demonstrated Flight SQL protocol failure. No successful Test
Connection, schema browsing or query execution was verified in that attempt.
See the [later manual pass](#dbeaver-rerun-and-manual-validation-on-2026-10-30).
Reported upstream as [DBeaver issue #42640](https://github.com/dbeaver/dbeaver/issues/42640).

Manual validation remains: connect with autocommit, expand `redis/public`,
inspect columns, open a table's data tab, and run a filtered aggregate. Record
the generated SQL and results separately from the successful JDBC smoke test.
Superset follow-up is deferred under the user's instruction; it is tracked in
[the endpoint repository](https://github.com/redis/redis-apache-arrow-flight/issues/41)
and [the adapter repository](https://github.com/drls-io/sqlalchemy-adbc/issues/16).

### DBeaver rerun and manual validation on 2026-10-30

Rebuilt source `4a26c34` and reran against a fresh disposable Redis 8.6.2
on `127.0.0.1:16389`, with Flight on `127.0.0.1:18815`. JDBC 19.0.0
in Temurin 25 passed every `JdbcSmoke.java` check, including metadata,
prepared statements, updates, session state and the 100,000-row concurrent
read. The known empty JDBC `getTypeInfo()` result remains unchanged.

Retried the installed macOS arm64 DBeaver Community 26.2.3 with the custom
driver configuration above. Two application attempts crashed: opening the
connection form, then a retry that clicked Test Connection and reached
“Set connection defaults.” No successful connection result, schema browsing,
data tab or generated SQL was verified. Crash reports at 08:00:22 and
08:00:58 PDT reproduce `EXC_BAD_ACCESS` / `SIGABRT`; the inspected stack
again includes `CFStringGetLength`, `cfAttributedStringSerialize`,
`AXSerializeCFType` and `_AXXMIGCopyParameterizedAttributeValue`.
These automation attempts remained blocked by the local accessibility crash.

A later manual test on the same date succeeded with the installed DBeaver
Community 26.2.3, bundled Temurin 25.0.4.1 and Flight SQL JDBC 19.0.0, using
**Generic** driver type and the same local ports. The user confirmed connection;
subsequent screen inspection showed `redis → public → Tables → dbeaver_sample`
and a result grid for:

```sql
SELECT "id", "name", "qty"
FROM "redis"."public"."dbeaver_sample";
```

The grid contained `(1, Apples, 10)`, `(2, Oranges, 20)` and `(3, Pears, 30)`.
This establishes manual connection, catalog/schema/table navigation and a quoted
SELECT with correct results. The fixture was created through JDBC in a separate
connection and found through JDBC metadata before the manual test. It does not
establish DDL or writes from DBeaver, expanded column metadata, a table Data tab,
filtered aggregates or arbitrary generated SQL. TLS/authentication and large-result
UI paging were not tested. The automation crash is separate from this manual pass.

Reproduce the workflow with [DBeaver setup and manual tests](dbeaver.md), which
links the screenshot guide and the exact 19.0.0 JAR. The direct Maven Central
download was reported faster for manual setup; no download benchmark was run.
Built-in driver support is proposed in [DBeaver PR #42645](https://github.com/dbeaver/dbeaver/pull/42645),
linked to [feature request #15535](https://github.com/dbeaver/dbeaver/issues/15535).
The tested application used a custom driver; the proposed packaged driver entry
has not been validated in a rebuilt DBeaver product.

## Core endpoint validation on 2026-10-29

A separate run against source `b84b1e2a1693a0269dd503ae84f2a353dc724a39`
passed both standalone and three-shard Cluster Go race suites (80 tests passed
and one enterprise-only admission test skipped in each). Python 3.14 with the
pinned ADBC 1.12.0, PyArrow 26.0.0 and pandas 3.1.0 passed the quickstart and
dataset-to-SQL scripts; Flight SQL JDBC 19.0.0 on Temurin Java 25 passed its
smoke test. Community metrics returned the expected empty HTTP 204 response.

The [validation record and raw logs](validation/2026-10-29-core-endpoint/README.md)
identify the tested revision, commands, versions and limitations. This run did
not exercise DBeaver, Superset or production deployment behavior, and does not
supersede the separate application attempts above. JDBC's empty type information
and the enterprise-only skip remain explicit limitations.

## Reproducing the standard client checks

Use a disposable Redis with Search and a running server. Pin Python dependencies
with `examples/python/requirements-smoke.txt`, then run:

```sh
python examples/python/quickstart.py --uri grpc://localhost:8815
python examples/python/dataset_to_sql.py --uri grpc://localhost:8815
make jdbc-smoke
```

The scripts accept `--user` and `--password` for ACL-authenticated test servers.
Use ephemeral test credentials; do not embed production credentials in commands.
CI uses uv with the pinned requirements. Go tests require REDIS_URI; without it,
backend tests skip. The JDBC Make target supplies the required JVM memory flags.

## JDBC type information and schema caveats

JDBC 19's `DatabaseMetaData.getTypeInfo()` returns an empty result: its
[ArrowFlightMetaImpl](https://github.com/apache/arrow-java/blob/v19.0.0/flight/flight-sql-jdbc-core/src/main/java/org/apache/arrow/driver/jdbc/ArrowFlightMetaImpl.java)
inherits Avatica's default implementation rather than forwarding this call to
Flight SQL GetXdbcTypeInfo. JdbcSmoke records that client limitation explicitly;
the Go wire-level tests validate the endpoint's type information separately.
This is independent of table schemas returned through GetTables/GetSchema.

[#17](https://github.com/redis/redis-apache-arrow-flight/issues/17) covers
JDBC 19 classifying prepared statements with no dataset schema as updates. Use
separate statements for scripts whose final schema depends on earlier writes,
and explicit casts/types for parameter-only projections. Do not claim a newer
JDBC release fixes this without checking and retesting it.
