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

## Write and verify a SQL table

Run in the environment installed above. This example writes the tool's Arrow
output through ADBC, reads the persisted values back and drops its unique table.
It is an illustrative recipe added after the recorded validation, not a new
live test result. It does not establish tool-native writer API support.

```python
import uuid
import pyarrow as pa
import adbc_driver_flightsql.dbapi as sql

import pandas as pd
output = pa.Table.from_pandas(pd.DataFrame({"id": [1, 2], "amount": [2.0, 4.0]}),
                            preserve_index=False)
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
