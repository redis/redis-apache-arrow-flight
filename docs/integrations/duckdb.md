# DuckDB

[All integration guides](README.md). Fetch Arrow data from Flight, register it
in a local DuckDB connection, and query it there. This path transfers data
before DuckDB executes SQL; it does not require a DuckDB Flight extension.

## Install and query a SQL result

Follow the [Python environment and sample table setup](README.md#optional-sample-sql-table),
which includes DuckDB. Run:

```python
import duckdb
import adbc_driver_flightsql.dbapi as sql

with sql.connect("grpc://localhost:8815", autocommit=True) as conn, conn.cursor() as cur:
    cur.execute("SELECT id, label, amount FROM flight_tool_sample ORDER BY id")
    table = cur.fetch_arrow_table()

with duckdb.connect() as local:
    local.register("redis_rows", table)
    result = local.execute("SELECT COUNT(*), SUM(amount) FROM redis_rows").fetchone()
    assert result == (3, 7.5)
    print(result)
```

Apply remote filters and select only needed columns in the ADBC query to limit
the transfer. `fetch_arrow_table()` materializes that result in client memory.
Subsequent DuckDB SQL runs locally; it does not update Redis.

## Query a named dataset

Download a dataset using the [Python Flight example](python.md#upload-and-download-a-dataset),
reading every returned endpoint. Inside a DuckDB connection, call
`local.register("features", received)` and query `features`. You can join the
registered data to local DuckDB tables or files.

## Validation and cleanup

Run `make clients-smoke` against a disposable server. Recorded coverage includes
local aggregates over Flight SQL results and ordered/NULL results over a native
Flight dataset; see [versions and limits](../client-compatibility.md#flight-client-validation-on-2026-10-29).
No direct DuckDB Flight connector or write-back path was tested. Remove the
sample table using the [shared cleanup](README.md#optional-sample-sql-table).
