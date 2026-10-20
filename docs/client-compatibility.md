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
| DBeaver | Not manually validated here | Underlying JDBC smoke only | Follow README JDBC setup; #23 still applies. Test actual schema browsing and generated SQL in the application |
| Tableau | Not validated | None | Requires generic JDBC setup and actual generated-SQL/metadata validation |
| Power BI | Not validated | None | Requires a custom ADBC Power Query connector; none is shipped |
| Polars, DuckDB, R | Not validated | None | Arrow/ADBC consumption examples are not a compatibility guarantee |

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

[#23](https://github.com/alberttwong/redis-apache-arrow-flight/issues/23) covers
JDBC 19 classifying prepared statements with no dataset schema as updates. Use
separate statements for scripts whose final schema depends on earlier writes,
and explicit casts/types for parameter-only projections. Do not claim a newer
JDBC release fixes this without checking and retesting it.
