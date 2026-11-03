# Metabase

[All integration guides](README.md). The validated route uses Metabase
0.64.2.9 with the community Flight SQL plugin 0.8.0, which bundles JDBC 19.0.0.
API reads, schema sync, generated queries and custom SQL Actions passed.
Browser/dashboard rendering was not tested. See [versions, commands and logs](../validation/2026-11-01-bi-jdbc/README.md).

## Install the plugin

Start a server using the [shared prerequisites](README.md). In the repository root:

```sh
mkdir -p bin/metabase-plugins
curl -fL https://github.com/J0hnG4lt/metabase-flightsql-driver/releases/download/0.8.0/arrow-flight-sql.metabase-driver.jar \
  -o bin/metabase-plugins/arrow-flight-sql.metabase-driver.jar
echo 'b97e9cac4ac43d1011d3dc7100d49adee928684bddb33e42e4bd17e21d654b60  bin/metabase-plugins/arrow-flight-sql.metabase-driver.jar' | shasum -a 256 -c -
```

Use a **writable** plugins directory: the recorded read-only mount failed to
load the plugin. Configure the Metabase JVM with the flags used in validation:

```text
--add-opens=java.base/java.nio=ALL-UNNAMED
--add-opens=java.base/sun.nio.ch=ALL-UNNAMED
--sun-misc-unsafe-memory-access=allow
--enable-native-access=ALL-UNNAMED
-Dio.netty.tryReflectionSetAccessible=true
```

The `sun.misc` option applies to Java 23+; older JVMs reject it. The
[plugin's own instructions](https://github.com/J0hnG4lt/metabase-flightsql-driver)
describe loading and configuration. Restart Metabase after installation.

## Add the connection and read data

Select the plugin's Arrow Flight SQL database type. Configure the Flight host,
port `8815`, catalog `redis`, and encryption disabled for the local example.
Use `host.docker.internal` when Metabase runs in Docker Desktop and Flight runs
on the host. Create the [shared sample](README.md#optional-sample-sql-table),
sync the database schema and inspect its columns. Run a native query:

```sql
SELECT id, label, amount FROM flight_tool_sample ORDER BY id;
```

Expect ids 1–3 and a NULL label on row 2. Check a visual filter and grouped count
separately; API validation covered generated queries, not their rendered UI.

## Write with Actions

Enable Actions in the connection details and database settings. The recorded
API configuration sets `enable-actions` in details and `database-enable-actions`
in database settings. Use custom SQL Actions for INSERT, UPDATE and DELETE,
and verify effects with a SELECT after each action. The
[smoke script](../../examples/jdbc/metabase_smoke.py) shows the exact tested API calls.

Do not use the native read-query endpoint as a DDL runner. The recorded DROP
failed with `Select statement did not produce a ResultSet for native query`;
an error does not guarantee that a write had no effects. Create/drop fixtures
through JDBC or ADBC. CSV uploads, implicit model actions and transactions
are outside the validated scope.

For full reproduction, follow the [disposable Docker setup](../validation/2026-11-01-bi-jdbc/README.md#reproduction).
That recipe uses Flight port 18825 and a separate fixture, and refuses an
already configured Metabase. Remove this guide's sample using the
[shared cleanup](README.md#optional-sample-sql-table).

## Write example with read-back

Use only the disposable shared sample and ensure id `99` is unused. Execute
one statement at a time through a custom SQL Action for each write; use a native
read query for each SELECT. Expect a new row with amount `9.5`,
then amount `10.5`, then an empty result after deletion:

```sql
INSERT INTO flight_tool_sample (id, label, amount) VALUES (99, 'guide', 9.5);
SELECT * FROM flight_tool_sample WHERE id = 99;
UPDATE flight_tool_sample SET amount = 10.5 WHERE id = 99;
SELECT amount FROM flight_tool_sample WHERE id = 99;
DELETE FROM flight_tool_sample WHERE id = 99;
SELECT * FROM flight_tool_sample WHERE id = 99;
```

This is a suggested application acceptance recipe. Record the actual result
before claiming writes for an untested application. SQL uses autocommit.
