import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * A smoke test of the Redis Arrow Flight server through the Arrow Flight SQL
 * JDBC driver, the driver DBeaver uses. Run it with the driver's jar (see the
 * README's JDBC section, or `make jdbc-smoke`):
 *
 * <pre>
 * java --add-opens=java.base/java.nio=ALL-UNNAMED -cp flight-sql-jdbc-driver-19.0.0.jar \
 *   examples/jdbc/JdbcSmoke.java "jdbc:arrow-flight-sql://localhost:8815/?useEncryption=false"
 * </pre>
 *
 * Besides prepared statements, metadata and updates, it checks the
 * connection's session: a large result streams while the connection runs
 * other statements, and SET and temporary tables last.
 *
 * It exits with status 1 if a check fails.
 */
public class JdbcSmoke {
    private static int failures;

    private static void check(boolean ok, String what) {
        System.out.println((ok ? "ok    " : "FAIL  ") + what);
        if (!ok) {
            failures++;
        }
    }

    private static void eq(Object got, Object want, String what) {
        check(Objects.equals(got, want), what + (Objects.equals(got, want) ? "" : ": got " + got + ", want " + want));
    }

    public static void main(String[] args) throws Exception {
        String url = args.length > 0 ? args[0] : "jdbc:arrow-flight-sql://localhost:8815/?useEncryption=false";
        String t = "jdbc_smoke_" + (System.nanoTime() % 1_000_000_000L);
        try (Connection c = DriverManager.getConnection(url)) {
            DatabaseMetaData md = c.getMetaData();
            System.out.println("connected to " + md.getDatabaseProductName() + " " + md.getDatabaseProductVersion()
                    + " with " + md.getDriverName() + " " + md.getDriverVersion());
            try (ResultSet types = md.getTypeInfo()) {
                Set<String> names = new HashSet<>();
                while (types.next()) {
                    names.add(types.getString(1));
                }
                // JDBC 19 inherits Avatica's empty getTypeInfo implementation;
                // it does not forward this call to Flight SQL GetXdbcTypeInfo.
                check(names.isEmpty(), "JDBC 19 getTypeInfo is empty (known client limitation)");
            }
            try (Statement st = c.createStatement()) {
                st.execute("CREATE TABLE " + t + " (id BIGINT NOT NULL, name VARCHAR, qty INTEGER)");
            }
            try {
                run(c, md, t);
            } finally {
                try (Statement st = c.createStatement()) {
                    st.execute("DROP TABLE IF EXISTS " + t);
                }
            }
        }
        if (failures > 0) {
            System.out.println(failures + " check(s) failed");
            System.exit(1);
        }
        System.out.println("all checks passed");
    }

    private static void run(Connection c, DatabaseMetaData md, String t) throws Exception {
        // Prepared statements with parameters.
        try (PreparedStatement ps = c.prepareStatement("INSERT INTO " + t + " (id, name, qty) VALUES (?, ?, ?)")) {
            eq(ps.getParameterMetaData().getParameterCount(), 3, "INSERT has 3 parameters");
            for (int i = 1; i <= 3; i++) {
                ps.setLong(1, i);
                ps.setString(2, "row " + i);
                ps.setInt(3, i * 10);
                eq(ps.executeUpdate(), 1, "prepared INSERT of row " + i);
            }
        }
        try (PreparedStatement ps = c.prepareStatement("SELECT name, qty FROM " + t + " WHERE id = ?")) {
            ps.setLong(1, 2);
            try (ResultSet rs = ps.executeQuery()) {
                check(rs.next(), "prepared SELECT returns a row");
                eq(rs.getString(1), "row 2", "prepared SELECT name");
                eq(rs.getInt(2), 20, "prepared SELECT qty");
                check(!rs.next(), "prepared SELECT returns one row");
            }
            ps.setLong(1, 3);
            try (ResultSet rs = ps.executeQuery()) {
                check(rs.next() && "row 3".equals(rs.getString(1)), "prepared SELECT with a new parameter value");
            }
        }

        // Plain statements.
        try (Statement st = c.createStatement(); ResultSet rs = st.executeQuery("SELECT COUNT(*), SUM(qty) FROM " + t)) {
            check(rs.next(), "aggregate returns a row");
            eq(rs.getLong(1), 3L, "COUNT(*)");
            eq(rs.getLong(2), 60L, "SUM(qty)");
        }
        try (Statement st = c.createStatement()) {
            eq(st.executeUpdate("UPDATE " + t + " SET qty = 0 WHERE id > 1"), 2, "UPDATE count");
        }

        // DatabaseMetaData, as DBeaver's navigator reads it.
        try (ResultSet rs = md.getCatalogs()) {
            check(rs.next() && "redis".equals(rs.getString("TABLE_CAT")), "getCatalogs: redis");
        }
        try (ResultSet rs = md.getSchemas()) {
            boolean found = false;
            while (rs.next()) {
                found |= "public".equals(rs.getString("TABLE_SCHEM"));
            }
            check(found, "getSchemas: public");
        }
        try (ResultSet rs = md.getTables(null, null, t, null)) {
            check(rs.next() && t.equals(rs.getString("TABLE_NAME")) && "TABLE".equals(rs.getString("TABLE_TYPE")),
                    "getTables finds the table");
        }
        try (ResultSet rs = md.getColumns(null, null, t, null)) {
            List<String> cols = new ArrayList<>();
            while (rs.next()) {
                cols.add(rs.getString("COLUMN_NAME"));
            }
            eq(cols, List.of("id", "name", "qty"), "getColumns");
        }
        try (ResultSet rs = md.getTableTypes()) {
            Set<String> types = new HashSet<>();
            while (rs.next()) {
                types.add(rs.getString(1));
            }
            check(types.containsAll(Set.of("TABLE", "VIEW")), "getTableTypes: TABLE, VIEW");
        }
        try (ResultSet rs = md.getPrimaryKeys(null, null, t)) {
            check(!rs.next(), "getPrimaryKeys: none");
        }
        check(md.nullsAreSortedAtEnd(), "SqlInfo: NULLs sort at the end");

        // Before the temporary table below: in a session that has one,
        // results are read whole on its connection.
        streamInSession(c, t);

        // The connection's session: SET and temporary tables last.
        try (Statement st = c.createStatement()) {
            st.execute("CREATE TEMP TABLE " + t + "_tmp AS SELECT 7 AS a");
            try (ResultSet rs = st.executeQuery("SELECT a FROM " + t + "_tmp")) {
                check(rs.next() && rs.getInt(1) == 7, "a temp table lasts to the next statement");
            }
            st.execute("SET TIME ZONE 'Asia/Tokyo'");
            try (ResultSet rs = st.executeQuery("SHOW TimeZone")) {
                check(rs.next() && "Asia/Tokyo".equals(rs.getString(1)), "SET lasts to the next statement");
            }
        }
        c.setCatalog("redis"); // SetSessionOptions
        check(true, "setCatalog(\"redis\")");
    }

    /**
     * A large result streams in the connection's session too: while it is
     * read, the connection runs other statements, and the result keeps the
     * session's schema and time zone as of the query.
     */
    private static void streamInSession(Connection c, String t) throws Exception {
        String schema = t + "_s";
        String ts = "CAST(TIMESTAMPTZ '2024-01-01 00:00:00+00' AS TEXT)";
        try (Statement st = c.createStatement()) {
            st.execute("CREATE SCHEMA " + schema);
            try {
                st.execute("SET search_path TO " + schema);
                st.execute("SET TIME ZONE 'Asia/Tokyo'");
                // More rows than the driver's stream batches (65,536).
                st.execute("CREATE TABLE big AS SELECT x AS id FROM generate_series(0, 99999) AS g(x)");
                String want;
                try (ResultSet rs = st.executeQuery("SELECT " + ts)) {
                    rs.next();
                    want = rs.getString(1);
                }
                long n = 0, sum = 0;
                boolean zones = true;
                try (Statement big = c.createStatement();
                        ResultSet rs = big.executeQuery("SELECT id, " + ts + " FROM big")) {
                    while (rs.next()) {
                        if (n == 0) {
                            st.execute("SET TIME ZONE 'UTC'");
                            try (ResultSet r = st.executeQuery("SELECT COUNT(*) FROM big")) {
                                eq(r.next() ? r.getLong(1) : -1, 100000L, "COUNT(*) while a large result is read");
                            }
                            st.execute("SET search_path TO public");
                            try (ResultSet r = st.executeQuery("SHOW TimeZone")) {
                                eq(r.next() ? r.getString(1) : null, "UTC", "SET while a large result is read");
                            }
                        }
                        n++;
                        sum += rs.getLong(1);
                        zones &= want.equals(rs.getString(2));
                    }
                }
                eq(n, 100000L, "rows of a large result in the session");
                eq(sum, 4999950000L, "sum of its ids");
                check(zones, "its rows have the session's time zone as of the query (" + want + ")");
            } finally {
                st.execute("DROP TABLE IF EXISTS " + schema + ".big");
                st.execute("DROP SCHEMA IF EXISTS " + schema);
                st.execute("SET search_path TO public");
                st.execute("RESET TIME ZONE");
            }
        }
    }
}
