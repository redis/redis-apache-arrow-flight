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

The [November 3 run](../validation/2026-11-03-flight-integrations/README.md)
used R 4.6.2, adbcdrivermanager 0.25.0, nanoarrow 0.9.0 and the Flight SQL
1.13.0 wheel's shared driver. Package installation above follows the repositories' available versions; it does not
reproduce those development builds exactly.

Obtain the [Flight SQL shared driver](https://arrow.apache.org/adbc/current/driver/flight_sql.html)
for R's OS and architecture. On Linux the library is
`libadbc_driver_flightsql.so`; use the corresponding library for your platform.
The older Linux arm64 test sourced it from a Python Flight SQL 1.12.0
wheel; the November 3 run used 1.13.0. Python does not execute the R queries.
The tested runtime could not install `adbcflightsql`, so these instructions use the explicit library path.

To obtain the shared library from the recorded Python driver wheel:

```sh
python3.12 -m venv .venv-r-driver
. .venv-r-driver/bin/activate
python -m pip install adbc-driver-flightsql==1.13.0
python -c 'import adbc_driver_flightsql; print(adbc_driver_flightsql._driver_path())'
```

Use the printed absolute path below and keep that environment installed.
The wheel helper is the path used by the repository harness, not an R API.

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

The [live guide recipe run](../validation/2026-11-03-guide-recipes/README.md) also passed this exact R
harness against the retained Enterprise-backed Flight endpoint. Its scope is
R driver-manager statements and stream conversion, not DBI/adbi.
