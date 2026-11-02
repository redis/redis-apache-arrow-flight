# Other JDBC applications

[All integration guides](README.md). Applications that accept a custom JDBC
driver can try Flight SQL. The JDBC driver is validated; each application's
metadata handling, generated SQL and write behavior still needs acceptance testing.

## Configure a custom driver

1. Start a server and create the [sample SQL table](README.md#optional-sample-sql-table).
2. Run `make bin/flight-sql-jdbc-driver-19.0.0.jar` from the repository root.
   Add that JAR to the application's driver libraries, or use Maven coordinate
   `org.apache.arrow:flight-sql-jdbc-driver:19.0.0`.
3. Set the driver class to `org.apache.arrow.driver.jdbc.ArrowFlightJdbcDriver`.
4. Set URL to `jdbc:arrow-flight-sql://localhost:8815/?useEncryption=false`
   for the local unauthenticated endpoint. Use the actual host from the client.
5. Configure the application's JVM with
   `--add-opens=java.base/java.nio=ALL-UNNAMED`. On Java 23+ also add
   `--sun-misc-unsafe-memory-access=allow`, then restart the application.
6. Use autocommit. Browse catalog `redis`, schema `public`, and table
   `flight_tool_sample`.

For TLS, credentials, session behavior and long-result timeouts, follow the
[JDBC connection reference](../client-guide.md#jdbc-dbeaver-and-others) and
[official driver options](https://arrow.apache.org/java/current/flight_sql_jdbc_driver.html).

## Check application behavior

Run these statements separately in the application's SQL editor:

```sql
SELECT id, label, amount FROM flight_tool_sample ORDER BY id;
SELECT COUNT(*) AS n, SUM(amount) AS total FROM flight_tool_sample;
```

Expect ids 1–3, a NULL label for id 2, and aggregate values `3` and `7.5`.
Check table/column discovery and generated filters separately. If writes are
part of the workflow, insert, update and delete a disposable row through that
application and verify each operation with a SELECT. Fixture creation through
another client does not prove application write support.

Run `make jdbc-smoke` to distinguish a driver/server issue from an application
issue. The driver's empty `getTypeInfo()` can affect application type discovery;
see [known caveats](../client-compatibility.md#jdbc-type-information-and-schema-caveats).
SQL tools see SQL tables; named datasets require a native Flight client.
Finish with the [sample cleanup](README.md#optional-sample-sql-table).
