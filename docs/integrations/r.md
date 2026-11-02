# R

[All integration guides](README.md). The validated route uses
`adbcdrivermanager` and a Flight SQL shared library, with nanoarrow converting
the query stream to a data frame. DBI/adbi is untested.

## Install the R packages and driver

Start a server using the [shared prerequisites](README.md). Install the packages:

```r
install.packages(c("adbcdrivermanager", "nanoarrow"),
                 repos = c("https://apache.r-universe.dev", "https://cloud.r-project.org"))
```

The [recorded run](../client-compatibility.md#flight-client-validation-on-2026-10-29)
used development builds of these packages and Flight SQL driver 1.12.0. Package
installation above follows the repositories' available versions; it does not
reproduce those development builds exactly.

Obtain the [Flight SQL shared driver](https://arrow.apache.org/adbc/current/driver/flight_sql.html)
for R's OS and architecture. On Linux the library is
`libadbc_driver_flightsql.so`; use the corresponding library for your platform.
The recorded Linux arm64 test sourced it from the Python Flight SQL 1.12.0
wheel. Python does not execute the R queries. The tested runtime could not
install `adbcflightsql`, so these instructions use the explicit library path.

## Run the read/write example

From the repository root, with R and the library installed:

```sh
Rscript examples/r/flight_sql.R grpc://localhost:8815 /path/to/libadbc_driver_flightsql.so
# Equivalent Make target:
make r-smoke FLIGHT_URI=grpc://localhost:8815 FLIGHT_SQL_DRIVER=/path/to/libadbc_driver_flightsql.so
```

The [script](../../examples/r/flight_sql.R) creates a unique table, inserts three
rows, checks ordered results, NULLs, an empty result and a SUM of `7.5`, then
drops its fixture. Expect a `PASS R Flight SQL` line and session information.

Its connection setup is:

```r
library(adbcdrivermanager)
driver <- adbc_driver("/path/to/libadbc_driver_flightsql.so",
                      entrypoint = "AdbcDriverFlightSQLInit")
db <- adbc_database_init(driver, uri = "grpc://localhost:8815")
con <- adbc_connection_init(db, adbc.connection.autocommit = "true")
# Create statements and consume streams as in examples/r/flight_sql.R.
adbc_connection_release(con)
adbc_database_release(db)
```

Keep autocommit enabled. If loading fails, check library architecture, OS,
dependencies and the `AdbcDriverFlightSQLInit` entry point. The recorded checks
use a local unauthenticated endpoint; TLS/authentication remains outside that scope.
