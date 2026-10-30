# DBeaver with Arrow Flight SQL

[Back to the client guide](client-guide.md#jdbc-dbeaver-and-others).
DBeaver connects to this server through the Apache Arrow Flight SQL JDBC driver.
Use **Generic** as the driver type and the tested **19.0.0** driver below.

The [Voltron Data walkthrough](https://github.com/voltrondata/setup-arrow-jdbc-driver-in-dbeaver)
has helpful screenshots of Driver Manager, library selection and connection
creation. Use the settings on this page: that walkthrough shows JDBC 11.0.0,
SQLite mode and TLS settings for a different endpoint. Do not copy those settings.

## Start the test endpoint

Use a disposable Redis with Search. From the repository root, start Redis on
port 16389 and build the Flight server. If the test endpoint is already running,
reuse it and skip these startup commands:

```bash
make redis-up REDIS_PORT=16389
make build
./bin/redis-flight-server --redis-uri redis://localhost:16389/0 --listen localhost:18815
```

Keep the Flight process running while testing. These commands use isolated test
ports; the project's normal defaults are Redis 6379 and Flight 8815. The DBeaver
JDBC URL must use the **Flight** port, not the Redis port.

## Download and configure the driver

Download [flight-sql-jdbc-driver-19.0.0.jar](https://repo1.maven.org/maven2/org/apache/arrow/flight-sql-jdbc-driver/19.0.0/flight-sql-jdbc-driver-19.0.0.jar)
directly from Maven Central. The [19.0.0 directory](https://repo1.maven.org/maven2/org/apache/arrow/flight-sql-jdbc-driver/19.0.0/)
also lists checksums and signatures. Choose the plain `.jar`, rather than a
sources or tests JAR. The direct download was faster in the reported manual
setup; this is an observation from that run, not a download speed guarantee.

1. Open **Database → Driver Manager → New**.
2. In **Settings**, enter the values below and select driver type **Generic**.
3. In **Libraries**, choose **Add File** and select the downloaded JAR.
4. Save the driver, then open **Database → New Database Connection** and select it.

| Setting | Value |
| --- | --- |
| Driver name | Apache Arrow Flight SQL |
| Driver type | Generic |
| Class name | `org.apache.arrow.driver.jdbc.ArrowFlightJdbcDriver` |
| URL template (local test without TLS) | `jdbc:arrow-flight-sql://{host}:{port}/?useEncryption=false` |
| Host | `localhost` |
| Port | `18815` for the endpoint above; `8815` for the normal default |
| Username/password | Empty for this unauthenticated local test |

The resulting URL for the test endpoint is:

```text
jdbc:arrow-flight-sql://localhost:18815/?useEncryption=false
```

Use autocommit (**Auto**); this server does not support transactions. Click
**Test Connection**, then **Finish**. For TLS and authenticated deployments,
follow the [JDBC connection options](client-guide.md#jdbc-dbeaver-and-others)
and [build profile availability](build-profiles.md).

### JVM options

The [Arrow JDBC documentation](https://arrow.apache.org/java/current/flight_sql_jdbc_driver.html)
requires `--add-opens=java.base/java.nio=ALL-UNNAMED`. For the tested JDBC
19.0.0 configuration, Java 23+ also needs
`--sun-misc-unsafe-memory-access=allow`.

Check `dbeaver.ini`, after its existing `-vmargs` line. Both options were already
present in the tested DBeaver Community 26.2.3 macOS installation, so no edit
was needed. If missing, add the applicable options and restart DBeaver. The
macOS file is `/Applications/DBeaver.app/Contents/Eclipse/dbeaver.ini`.
See [JDBC setup](client-guide.md#jdbc-dbeaver-and-others) for the exact options.

## Create a table and verify browsing

A fresh Redis test instance has no SQL tables. SQL tables appear in DBeaver;
arbitrary Redis keys and named Flight datasets do not become SQL tables
automatically. Open an SQL console on the connection and execute each statement
separately. Use this fixture name only in the disposable test database; if it
already exists, use a fresh name throughout.

```sql
CREATE TABLE dbeaver_sample (id BIGINT NOT NULL, name VARCHAR, qty INTEGER);
INSERT INTO dbeaver_sample (id, name, qty) VALUES (1, 'Apples', 10);
INSERT INTO dbeaver_sample (id, name, qty) VALUES (2, 'Oranges', 20);
INSERT INTO dbeaver_sample (id, name, qty) VALUES (3, 'Pears', 30);
```

Refresh the connection in the navigator, then expand
**redis → public → Tables → dbeaver_sample**. Expand **Columns** and inspect
`id`, `name` and `qty`. Run:

```sql
SELECT "id", "name", "qty"
FROM "redis"."public"."dbeaver_sample"
ORDER BY "id";
```

Expected result:

| id | name | qty |
| --- | --- | --- |
| 1 | Apples | 10 |
| 2 | Oranges | 20 |
| 3 | Pears | 30 |

For additional manual coverage, open the table's **Data** tab, inspect the SQL
DBeaver issues, and execute this filtered aggregate in the SQL console:

```sql
SELECT COUNT(*) AS n, SUM(qty) AS total
FROM dbeaver_sample
WHERE qty >= 20;
```

Expect `n = 2` and `total = 50`. Record each check separately; the
[2026-10-30 manual result](client-compatibility.md#dbeaver-rerun-and-manual-validation-on-2026-10-30)
confirms connection, navigator browsing and the three-row SELECT. It does not
claim completion of every additional check above. Remove the fixture when done:

```sql
DROP TABLE dbeaver_sample;
```

Stop the foreground Flight process with Ctrl+C. To stop the Compose test Redis,
run `docker compose stop redis`. Keep both services running until manual testing
is complete.

## Automated checks and known limits

With a full JDK installed, run this from another terminal against the endpoint:

```bash
make jdbc-smoke JDBC_URL='jdbc:arrow-flight-sql://localhost:18815/?useEncryption=false'
```

This downloads and verifies JDBC 19.0.0, selects the Java memory options, creates
unique fixtures and cleans them up. It checks metadata, parameters, writes,
sessions and a 100,000-row concurrent read. DBeaver's bundled runtime may lack
the compiler needed to run the Java source file; use a full JDK via
`JAVA=/path/to/jdk/bin/java` if necessary. A JDBC smoke pass does not establish
that the DBeaver UI passed.

Earlier accessibility automation attempts crashed DBeaver on macOS; the later
manual connection and query succeeded. Track the automation crash separately
in [DBeaver #42640](https://github.com/dbeaver/dbeaver/issues/42640).
[DBeaver PR #42645](https://github.com/dbeaver/dbeaver/pull/42645) proposes a
built-in Flight SQL driver entry; the validated application setup used a custom
driver. See the [client matrix](client-compatibility.md) for remaining JDBC and
application coverage limits.
