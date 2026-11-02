# Tableau Desktop Free Edition — November 1, 2026

This follow-up is separate from the Metabase changes in
[PR #50](https://github.com/redis/redis-apache-arrow-flight/pull/50).
The earlier Metabase record predates the Tableau installation.

Tableau Desktop Free Edition 2026.3 (Apple silicon) was installed and operated
through its native UI. The verified Arrow Flight SQL JDBC 19.0.0 JAR was copied
to `~/Library/Tableau/Drivers`, then Tableau was restarted. No custom connector,
TDC capability overrides or Tableau JVM configuration changes were applied.

Backend: community server binary from source `f8a2723`, Redis ADBC
`2f053d3affad`, disposable Redis 8.6.2 on port 16389 and Flight on port 18825.
The fixture was seeded using `examples/jdbc/BiFixture.java` from PR #50.

## Read test: passed

1. Start page > To a Server > More > Other Databases (JDBC).
2. URL: `jdbc:arrow-flight-sql://localhost:18825/?useEncryption=false`.
3. Keep SQL92 dialect; leave username/password blank for the local unauthenticated endpoint.
4. Sign in. Select/type database `redis`, then schema `public`.
5. Double-click `bi_jdbc_validation`, then click **Update Now** in the preview.

Tableau discovered the table and three columns and displayed **3 fields 3 rows**:

| Id | Category | Amount |
| --- | --- | --- |
| 1 | east | 10.50000 |
| 2 | west | 20.00000 |
| 3 | east | null |

This validates connection, catalog/schema/table/column discovery and Tableau's
generated data-preview SELECT. Worksheets, aggregations, dashboards, extracts,
joins, authentication and TLS were not tested.

## Scope

The requester chose read-only acceptance for Tableau after reviewing its
analytics workflow. Writes are not a requirement for this integration. An
earlier exploratory INSERT through Custom SQL was rejected because Tableau
wrapped it as a SELECT subquery; it does not change the passing read result.
No write support, Initial SQL customization or extension is claimed here.

## Reproduction prerequisites

Build the server and obtain the pinned JDBC JAR using the Makefile. Start a
disposable Redis with Search and the community Flight server, then seed the
three-row table with [BiFixture.java](../../../examples/jdbc/BiFixture.java).
The helper's `create` mode replaces `bi_jdbc_validation`, so use a disposable
backend. The [Metabase validation instructions](../2026-11-01-bi-jdbc/README.md#reproduction)
include the same Redis/Flight startup and Java-container fixture commands;
Metabase itself is not needed for this Tableau test.

Copy `bin/flight-sql-jdbc-driver-19.0.0.jar` to
`~/Library/Tableau/Drivers/` and restart Tableau. Follow the read-test steps
above. After checking the displayed values, run the fixture helper in `drop`
mode and stop the disposable services.

## Evidence and limits

The native UI showed database `redis`, schema `public`, the table name, three
column names/types and the three preview rows recorded above. No screenshot
file or Tableau query log was captured; this is a manual UI validation record.
The source revision and client versions describe the actual executed test, not
a rerun against the documentation commit. The original JDBC driver smoke suite
is separate from this Tableau application result.

## Cleanup

The failed SQL dialog was cancelled. The test fixture was dropped via JDBC,
the disposable Redis container was removed, and the Flight server was stopped.
Tableau remains open with the read preview; its endpoint is now stopped.
The JDBC JAR remains installed for subsequent testing. No workbook was saved
or published. Reading Tableau's logs under Documents was blocked by macOS
filesystem privacy permissions, including an unsandboxed read attempt; the
recorded application read results come from the visible UI.
