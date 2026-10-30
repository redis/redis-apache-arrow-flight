import java.sql.*;
import java.nio.file.*;
import java.util.*;

// JDBC measures executeQuery plus complete getObject materialization. JVM
// startup, connections, validation and output serialization are outside timing.
class JdbcBenchmark {
  static String json(Object value) {
    if (value == null) return "null";
    return "\"" + value.toString().replace("\\", "\\\\").replace("\"", "\\\"")
      .replace("\n", "\\n").replace("\r", "\\r").replace("\t", "\\t") + "\"";
  }
  public static void main(String[] args) throws Exception {
    Path root = Path.of(args[1]);
    int repeats = Integer.parseInt(args[2]);
    try (Connection conn = DriverManager.getConnection(args[0]);
         Statement statement = conn.createStatement()) {
      try (var warmup = statement.executeQuery("SELECT 1")) { while (warmup.next()) {} }
      var results = new ArrayList<String>();
      int query = 0;
      for (String line : Files.readAllLines(root.resolve("queries.tsv"))) {
        String[] fields = line.split("\t", 2);
        try {
          for (int repeat = -1; repeat < repeats; repeat++) {
            var rows = new ArrayList<Object[]>();
            long start = System.nanoTime();
            try (ResultSet rs = statement.executeQuery(fields[1])) {
              int columns = rs.getMetaData().getColumnCount();
              while (rs.next()) {
                Object[] row = new Object[columns];
                for (int c = 0; c < columns; c++) row[c] = rs.getObject(c + 1);
                rows.add(row);
              }
            }
            double seconds = (System.nanoTime() - start) / 1e9;
            if (repeat >= 0) {
              results.add("{\"label\":" + json(fields[0]) + ",\"repeat\":" + repeat
                + ",\"rows\":" + rows.size() + ",\"seconds\":" + seconds + "}");
              Files.writeString(root.resolve("jdbc-results.json"), "[\n" + String.join(",\n", results) + "\n]\n");
              // Retain every trial's fetched values for validation outside timing.
              StringBuilder values = new StringBuilder("[\n");
              for (int r = 0; r < rows.size(); r++) {
                if (r > 0) values.append(",\n");
                values.append("[");
                for (int c = 0; c < rows.get(r).length; c++) {
                  if (c > 0) values.append(",");
                  values.append(json(rows.get(r)[c]));
                }
                values.append("]");
              }
              values.append("\n]\n");
              Files.writeString(root.resolve("jdbc-q" + query + "-r" + repeat + ".json"), values);
            }
            System.out.printf("jdbc %s %d %.4f rows=%d%n", fields[0], repeat, seconds, rows.size());
          }
        } catch (Exception e) {
          results.add("{\"label\":" + json(fields[0]) + ",\"error\":" + json(e.toString()) + "}");
          Files.writeString(root.resolve("jdbc-results.json"), "[\n" + String.join(",\n", results) + "\n]\n");
          System.out.println("FAILED " + fields[0] + " " + e);
        }
        query++;
      }
    }
  }
}
