import java.sql.DriverManager;

/** Creates/drops a fixture on a disposable backend for BI application tests. */
public class BiFixture {
    public static void main(String[] args) throws Exception {
        if (args.length != 2 || !(args[1].equals("create") || args[1].equals("drop"))) {
            throw new IllegalArgumentException("Usage: BiFixture.java JDBC_URL create|drop");
        }
        try (var connection = DriverManager.getConnection(args[0]);
             var statement = connection.createStatement()) {
            statement.executeUpdate("DROP TABLE IF EXISTS bi_jdbc_validation");
            if (args[1].equals("create")) {
                statement.executeUpdate("CREATE TABLE bi_jdbc_validation (id BIGINT, category VARCHAR, amount DOUBLE)");
                int count = statement.executeUpdate("INSERT INTO bi_jdbc_validation VALUES "
                        + "(1, 'east', 10.5), (2, 'west', 20.0), (3, 'east', NULL)");
                if (count != 3) throw new AssertionError("Expected 3 inserted rows, got " + count);
            }
            System.out.println("PASS fixture " + args[1]);
        }
    }
}
