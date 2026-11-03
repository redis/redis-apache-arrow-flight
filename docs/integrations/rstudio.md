# RStudio

[All integration guides](README.md). RStudio runs the R ADBC driver-manager
example; it does not provide a separate Flight SQL database connector.

## Install

Install [R](https://cran.r-project.org/) and [RStudio Desktop](https://posit.co/download/rstudio-desktop/).
Follow the [R package and shared driver installation](r.md#install-the-r-packages-and-driver)
for `adbcdrivermanager`, `nanoarrow` and a Flight SQL driver library matching R's
OS and architecture. Start the [server](README.md) against a disposable backend.

## Read, write and verify

Open this repository as the RStudio working directory. In the **R Console**, run:

```r
source("examples/r/flight_sql.R")
main("grpc://localhost:8815", "/absolute/path/to/libadbc_driver_flightsql.so")
```

Replace the driver path with the library you installed. Sourcing defines the
script's `main` function; calling it in the console runs the read/write checks.

The [R script](../../examples/r/flight_sql.R) creates a unique table, inserts
three rows through R ADBC, reads the values back (including NULL and an empty
result), checks the aggregate and drops its fixture. Expect `PASS R Flight SQL`
and printed session information. It uses autocommit and requires no shared
sample table. Inspect the script for the complete statement and stream code.

## Validation and limits

The [November 3 RStudio transcript](../validation/2026-11-03-guide-recipes/rstudio.log)
records native R Console execution of this harness, including writes and
read-back. DBI/adbi and RStudio's Connections pane remain untested. The live run used the retained Enterprise-backed endpoint; it does not
qualify a new public source fingerprint or test Flight client TLS/auth.
