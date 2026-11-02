# Java

[All integration guides](README.md). Flight SQL JDBC 19.0.0 is the validated
Java route. Native Java Flight SDK access is untested against this server.

## Install and query through JDBC

Follow the [server and sample table setup](README.md). Install a full JDK
(recorded local tests used Java 25), then download the verified driver from the
repository root:

```sh
make bin/flight-sql-jdbc-driver-19.0.0.jar
```

For Maven applications use `org.apache.arrow:flight-sql-jdbc-driver:19.0.0`.
See the [official driver instructions](https://arrow.apache.org/java/current/flight_sql_jdbc_driver.html).
Save the following as `FlightQuery.java`:

```java
import java.sql.*;

public class FlightQuery {
    public static void main(String[] args) throws Exception {
        String url = "jdbc:arrow-flight-sql://localhost:8815/?useEncryption=false";
        try (Connection conn = DriverManager.getConnection(url);
             PreparedStatement stmt = conn.prepareStatement(
                 "SELECT id, label FROM flight_tool_sample WHERE id >= ? ORDER BY id")) {
            stmt.setInt(1, 2);
            try (ResultSet rows = stmt.executeQuery()) {
                while (rows.next()) {
                    System.out.println(rows.getLong(1) + ": " + rows.getString(2));
                }
            }
        }
    }
}
```

On Java 25, run:

```sh
java --add-opens=java.base/java.nio=ALL-UNNAMED \
  --sun-misc-unsafe-memory-access=allow \
  -cp bin/flight-sql-jdbc-driver-19.0.0.jar FlightQuery.java
```

Expect `2: null` and `3: three`. Java 17–22 must omit the
`--sun-misc-unsafe-memory-access` option; Java 23+ needs it for this setup.
Use `executeUpdate` for writes and verify them with a fresh SELECT. Keep
autocommit enabled and execute statements separately.

## Validation and limits

Run `make jdbc-smoke` against a disposable running server. Override the address
with `JDBC_URL` and the JDK with `JAVA=/path/to/bin/java`. The smoke suite tests
metadata, prepared parameters, writes, sessions and concurrent statement/result
reads. See the [JDBC caveats](../client-compatibility.md#jdbc-type-information-and-schema-caveats)
for empty `getTypeInfo()` and prepared statements without a result schema.

For named datasets, use the [native Java Flight SDK](https://arrow.apache.org/java/current/flight.html)
with the server's [dataset API](../data-access.md#datasets); JDBC does not expose
that API. This is an unvalidated development route. Remove the shared sample
table with the [fixture cleanup](README.md#optional-sample-sql-table).
