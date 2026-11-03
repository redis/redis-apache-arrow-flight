# Polars

[All integration guides](README.md). Use an ADBC Flight SQL connection for
SQL reads, or `pl.from_arrow()` for downloaded Arrow data.

## Install and query

Follow the [Python environment and sample table setup](README.md#optional-sample-sql-table),
which includes Polars. Then run:

```python
import polars as pl
import adbc_driver_flightsql.dbapi as sql

with sql.connect("grpc://localhost:8815", autocommit=True) as conn:
    query = "SELECT * FROM flight_tool_sample ORDER BY id"
    frame = pl.read_database(query, conn)
    print(frame)
    assert frame["id"].to_list() == [1, 2, 3]
    assert frame["label"].null_count() == 1
    for batch in pl.read_database(query, conn, iter_batches=True, batch_size=2):
        print(batch)
```

Keep the connection open while consuming the batch iterator. Batch sizes
depend on driver behavior; do not assume each batch has exactly two rows.

## Arrow results and writes

For a native dataset, follow the [Python download example](python.md#upload-and-download-a-dataset)
and use `pl.from_arrow(received)`. SQL tables and named datasets are separate.
For parameterized SQL, bind with an ADBC cursor before converting its Arrow
result. For ingestion, pass `frame.to_arrow()` to `cur.adbc_ingest` with a new
table name and `mode="create"`; Polars write APIs have not been validated here.

## Validation and cleanup

Run `make clients-smoke` against a disposable server. The
[recorded Polars checks](../client-compatibility.md#flight-client-validation-on-2026-10-29)
cover ordered reads, NULLs, empty results and batch iteration. They do not
establish arbitrary Polars API compatibility. Use the
[shared fixture cleanup](README.md#optional-sample-sql-table) when done.

## Write and verify a SQL table

Run in the environment installed above. This example writes the tool's Arrow
output through ADBC, reads the persisted values back and drops its unique table.
It is an illustrative recipe added after the recorded validation, not a new
live test result. It does not establish tool-native writer API support.

```python
import uuid
import pyarrow as pa
import adbc_driver_flightsql.dbapi as sql

import polars as pl
output = pl.DataFrame({"id": [1, 2], "amount": [2.0, 4.0]}).to_arrow()
name = "guide_" + uuid.uuid4().hex
with sql.connect("grpc://localhost:8815", autocommit=True) as conn, conn.cursor() as cur:
    try:
        cur.adbc_ingest(name, output, mode="create")
        cur.execute(f'SELECT * FROM "{name}" ORDER BY id')
        assert cur.fetch_arrow_table().equals(output)
    finally:
        cur.execute(f'DROP TABLE IF EXISTS "{name}"')
        cur.fetch_arrow_table()
```
