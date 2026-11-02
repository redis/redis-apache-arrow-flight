# JDBC and Metabase validation — November 1, 2026

Server source: `f8a2723b737626eec3dc1ec529f9ebf3d70ff21f` (community build).
Redis ADBC revision: `2f053d3affad`, pinned in `go.mod`.
Go 1.27.2 on macOS arm64; clients in Linux arm64 Docker containers.
Disposable Redis 8.6.2 on port 16389; Flight on 18825; Metabase on 13030.

| Integration | Reads | Writes | Result |
| --- | --- | --- | --- |
| Arrow Flight SQL JDBC 19.0.0 / Temurin Java 25 | Prepared SELECT, aggregates, catalogs/schemas/tables/columns, 100,000-row concurrent read | CREATE/DROP, prepared INSERT, UPDATE, temporary tables and session settings | PASS existing `JdbcSmoke.java` |
| Metabase 0.64.2.9 / community Flight SQL plugin 0.8.0 / bundled JDBC 19.0.0 | Native ordered rows, NULLs, aggregates, empty results, schema/column discovery, Metabase-generated filter/order and grouped count | Custom SQL Actions: INSERT, UPDATE and DELETE, each verified by a fresh SELECT | PASS application API smoke; no browser rendering test |
| Metabase native read-query endpoint with DDL | Connection succeeded | DROP returned HTTP 400: `Select statement did not produce a ResultSet for native query` | FAIL for this write path; Actions tested separately |
| Tableau | Not executed | Not executed | BLOCKED: Tableau is not installed; JDBC success does not establish Tableau compatibility |

The native DDL attempt also failed while preparing the fixture. Cleanup's DROP
error superseded the original exception; the retained log shows the DROP failure.
Do not infer that an error means a write had no effects. The final passing test
seeds and drops its fixture through JDBC, and tests application writes through
Metabase Actions. It does not claim CSV uploads, implicit model CRUD actions,
transactions, TLS/authentication, dashboards, or broader SQL/type compatibility.
The JDBC driver's empty `getTypeInfo()` remains a known limitation.

## Dependencies

- [Metabase Flight SQL plugin 0.8.0](https://github.com/J0hnG4lt/metabase-flightsql-driver/releases/tag/0.8.0), SHA-256:
  `b97e9cac4ac43d1011d3dc7100d49adee928684bddb33e42e4bd17e21d654b60`.
- `metabase/metabase:v0.64.2.9`, pulled manifest digest:
  `sha256:a314433f07c192b18eb41a0c44cdadbe688525d2b3d1597c085e11f6c684c6a9`.
- `eclipse-temurin:25-jdk`, `redis:8.6.2`.
- JDBC JAR downloaded by the Makefile and checked against Maven Central's SHA-1.

Metabase requires a **writable** plugin directory. With a read-only mount it
falls back to `/tmp` and does not load the community plugin. All runs used the
Arrow JVM flags below. Application credentials were generated in memory and
not saved in the evidence.

## Reproduction

Run from the repository root, with Docker available. Use fresh disposable
services: the fixture helper replaces `bi_jdbc_validation`, and the Python
script refuses an already configured Metabase. The shell below is for macOS
Docker Desktop (`host.docker.internal`).

```sh
make build bin/flight-sql-jdbc-driver-19.0.0.jar
mkdir -p bin/metabase-plugins
curl -fL https://github.com/J0hnG4lt/metabase-flightsql-driver/releases/download/0.8.0/arrow-flight-sql.metabase-driver.jar \
  -o bin/metabase-plugins/arrow-flight-sql.metabase-driver.jar
echo 'b97e9cac4ac43d1011d3dc7100d49adee928684bddb33e42e4bd17e21d654b60  bin/metabase-plugins/arrow-flight-sql.metabase-driver.jar' | shasum -a 256 -c -
docker run -d --name redis-flight-bi-validation -p 127.0.0.1:16389:6379 redis:8.6.2
./bin/redis-flight-server --redis-uri redis://127.0.0.1:16389/0 \
  --listen 0.0.0.0:18825 --metrics-listen localhost:19825
```

With the Flight server running, use another shell in the repository root:

```sh
bi_java() {
  docker run --rm -v "$PWD:/work" -w /work eclipse-temurin:25-jdk \
    java --add-opens=java.base/java.nio=ALL-UNNAMED \
    --sun-misc-unsafe-memory-access=allow \
    -cp bin/flight-sql-jdbc-driver-19.0.0.jar "$@"
}
bi_java examples/jdbc/JdbcSmoke.java 'jdbc:arrow-flight-sql://host.docker.internal:18825/?useEncryption=false'
bi_java examples/jdbc/BiFixture.java 'jdbc:arrow-flight-sql://host.docker.internal:18825/?useEncryption=false' create
docker run -d --name redis-flight-metabase-validation -p 127.0.0.1:13030:3000 \
  -v "$PWD/bin/metabase-plugins:/plugins" \
  -e MB_DB_TYPE=h2 -e MB_DB_FILE=/tmp/metabase-test \
  -e MB_ANON_TRACKING_ENABLED=false -e MB_CHECK_FOR_UPDATES=false \
  -e 'JAVA_OPTS=--add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --sun-misc-unsafe-memory-access=allow --enable-native-access=ALL-UNNAMED -Dio.netty.tryReflectionSetAccessible=true' \
  metabase/metabase:v0.64.2.9
python3 examples/jdbc/metabase_smoke.py
# Run cleanup even if a test fails, then stop Flight in its terminal.
bi_java examples/jdbc/BiFixture.java 'jdbc:arrow-flight-sql://host.docker.internal:18825/?useEncryption=false' drop
docker rm -f redis-flight-metabase-validation redis-flight-bi-validation
```

## Evidence

- [JDBC assertion output](jdbc.log) (allocator startup logging omitted).
- [Metabase API assertion output](metabase.log).
- [Earlier native DDL failure](metabase-native-ddl.log).
- [Fixture cleanup](cleanup.log).
- [Metabase smoke script](../../../examples/jdbc/metabase_smoke.py).
- [JDBC fixture helper](../../../examples/jdbc/BiFixture.java).

Both test containers and the Flight process were removed after validation.
Other running services were left untouched. No server behavior or dependency
pins changed. Python syntax compilation and `git diff --check` passed.
