# Tableau

[All integration guides](README.md). Tableau Desktop Free Edition 2026.3 with
Flight SQL JDBC 19.0.0 passed connection, metadata discovery and a three-row
preview. The acceptance scope is read-only; worksheets, aggregates, extracts,
joins and dashboards remain untested. See the [manual validation record](../validation/2026-11-01-tableau/README.md).

## Install the driver

Start the server and create the [sample SQL table](README.md#optional-sample-sql-table).
From the repository root:

```sh
make bin/flight-sql-jdbc-driver-19.0.0.jar
```

Copy the JAR to Tableau's driver directory and restart Tableau. The validated
macOS setup used `~/Library/Tableau/Drivers/`. For other platforms, follow
[Tableau's JDBC driver locations](https://help.tableau.com/current/pro/desktop/en-gb/examples_otherdatabases_jdbc.htm).
The recorded macOS test needed no Tableau JVM changes, TDC overrides or custom connector.

## Connect and preview

1. Select **To a Server → More → Other Databases (JDBC)**.
2. Enter `jdbc:arrow-flight-sql://localhost:8815/?useEncryption=false`.
3. Keep the SQL92 dialect. Leave credentials blank for this local unauthenticated setup.
4. Sign in and select/type database `redis`, then schema `public`.
5. Add `flight_tool_sample` and click **Update Now** in the data preview.

Expect three columns and three rows, including the NULL label on row 2.
The historical validation used a different fixture, `bi_jdbc_validation`, and
Flight port 18825; its exact commands and expected values are in the linked
record. The guide's shared sample uses the normal port 8815.

## Scope and cleanup

Preview success establishes that read path only. Test worksheet-generated SQL,
filters and aggregates before depending on them. Custom SQL may be wrapped as
a SELECT subquery, so it is not a general DDL/DML editor. No write support is
claimed. Authentication and TLS were not covered by the recorded run.

Remove the sample through the [fixture cleanup](README.md#optional-sample-sql-table).
JDBC sees SQL tables; named Arrow datasets need the native Flight API.
