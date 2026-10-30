# Rscript examples/r/flight_sql.R grpc://localhost:8815 /path/to/libadbc_driver_flightsql.so
# Requires adbcdrivermanager and nanoarrow. The library must match R's OS/architecture.
library(adbcdrivermanager)
args <- commandArgs(trailingOnly = TRUE)
stopifnot(length(args) == 2L)

main <- function(uri, driver_path) {
  driver <- adbc_driver(driver_path, entrypoint = "AdbcDriverFlightSQLInit")
  db <- adbc_database_init(driver, uri = uri)
  on.exit(adbc_database_release(db), add = TRUE, after = FALSE)
  con <- adbc_connection_init(db, adbc.connection.autocommit = "true")
  on.exit(adbc_connection_release(con), add = TRUE, after = FALSE)
  query <- function(sql) {
    stmt <- adbc_statement_init(con)
    on.exit(adbc_statement_release(stmt))
    adbc_statement_set_sql_query(stmt, sql)
    stream <- nanoarrow::nanoarrow_allocate_array_stream()
    adbc_statement_execute_query(stmt, stream)
    as.data.frame(stream)
  }
  name <- paste0("it_flight_r_", Sys.getpid(), "_", sample.int(1e8, 1))
  query(paste0("CREATE TABLE ", name, " (id INTEGER, label TEXT, amount DOUBLE)"))
  on.exit(query(paste("DROP TABLE IF EXISTS", name)), add = TRUE, after = FALSE)
  query(paste0("INSERT INTO ", name, " VALUES (1, 'one', 1.5), (2, NULL, 2.5), (3, 'three', 3.5)"))
  rows <- query(paste("SELECT * FROM", name, "ORDER BY id"))
  stopifnot(identical(as.integer(rows$id), 1:3),
            identical(rows$label, c("one", NA_character_, "three")),
            identical(rows$amount, c(1.5, 2.5, 3.5)))
  stopifnot(nrow(query(paste("SELECT id FROM", name, "WHERE id < 0"))) == 0L)
  stopifnot(as.numeric(query(paste("SELECT SUM(amount) AS total FROM", name))$total) == 7.5)
  cat("PASS R Flight SQL: DDL, writes, query, nulls, empty result, aggregate\n")
  print(sessionInfo())
}
main(args[[1]], args[[2]])
