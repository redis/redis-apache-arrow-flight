# pandas

[All integration guides](README.md). pandas reads SQL through an ADBC Flight
SQL connection, or converts an Arrow table downloaded through either API.

## Install and read

Follow the [Python environment and sample table setup](README.md#optional-sample-sql-table).
It includes pandas and the Flight SQL driver. With the server running:

```python
import pandas as pd
import adbc_driver_flightsql.dbapi as sql

with sql.connect("grpc://localhost:8815", autocommit=True) as conn:
    frame = pd.read_sql("SELECT * FROM flight_tool_sample ORDER BY id", conn)
    print(frame)
    assert frame.id.tolist() == [1, 2, 3]
    assert pd.isna(frame.loc[1, "label"])
```

## Bind parameters through ADBC

The recorded pandas ADBC path raises `NotImplementedError` for
`pd.read_sql(..., params=...)`. Bind through the cursor, then convert:

```python
with sql.connect("grpc://localhost:8815", autocommit=True) as conn, conn.cursor() as cur:
    cur.execute("SELECT * FROM flight_tool_sample WHERE id >= ? ORDER BY id", (2,))
    selected = cur.fetch_arrow_table().to_pandas()
    assert selected.id.tolist() == [2, 3]
```

Use the same `table.to_pandas()` conversion for a native Flight dataset
downloaded with the [Python guide](python.md#upload-and-download-a-dataset).
For writes, convert with `pyarrow.Table.from_pandas(frame, preserve_index=False)`
and ingest through ADBC into a new table; pandas `to_sql` is outside the
recorded coverage.

## Validation and cleanup

Run `make clients-smoke` from the repository root against a disposable server
(`FLIGHT_URI=grpc://localhost:8815` overrides the address). The
[recorded checks](../client-compatibility.md#flight-client-validation-on-2026-10-29)
cover reads, NULLs, empty results and the parameter workaround. Remove the
shared table with the [fixture cleanup](README.md#optional-sample-sql-table).
