# Dagster

[All integration guides](README.md). This guide reads SQL rows through Flight
SQL and writes a separate output table, then verifies it through a new SELECT.

## Install

Start the [server and sample table](README.md#optional-sample-sql-table).
From the repository root, create a separate environment:

```sh
python3.12 -m venv .venv-dagster
. .venv-dagster/bin/activate
python -m pip install -r requirements/integrations/core.txt
```

The profile pins the November 3 dependency versions. The examples default to
`grpc://localhost:8815` without client TLS/auth; use `FLIGHT_URI` for another
reachable local development endpoint. Use the [connection reference](../client-guide.md)
for secure deployment configuration.

## Read, write and verify

Save as `dagster_example.py` and run `python dagster_example.py`.

```python
import os
import uuid
import pyarrow as pa
import adbc_driver_flightsql
from dagster import asset, materialize
from dagster_adbc import ADBCResource

@asset
def flight_copy(flight: ADBCResource) -> int:
    name = "guide_" + uuid.uuid4().hex
    with flight.get_connection() as conn, conn.cursor() as cur:
        cur.execute("SELECT id, label, amount FROM flight_tool_sample ORDER BY id")
        source = cur.fetch_arrow_table()
        output = pa.table({"id": source["id"],
                           "amount": [value * 2 for value in source["amount"].to_pylist()]})
        try:
            cur.adbc_ingest(name, output, mode="create")
            cur.execute(f'SELECT * FROM "{name}" ORDER BY id')
            assert cur.fetch_arrow_table().equals(output)
        finally:
            cur.execute(f'DROP TABLE IF EXISTS "{name}"')
            cur.fetch_arrow_table()
    return output.num_rows

if __name__ == "__main__":
    resource = ADBCResource(driver=adbc_driver_flightsql._driver_path(),
        uri=os.environ.get("FLIGHT_URI", "grpc://localhost:8815"), autocommit=True)
    result = materialize([flight_copy], resources={"flight": resource})
    assert result.success
    assert result.output_for_node("flight_copy") == 3
```

The driver path uses the same wheel helper as the pinned integration tests.
This is local materialization with a task-local ADBC resource; production
resources, retries and scheduling need separate validation.

## Validation and cleanup

The [November 3 results](../validation/2026-11-03-flight-integrations/README.md)
record the historical workflow for this tool. The new read/write recipe above
has not been run against a live endpoint; fixture writes through another
client and local artifact saves do not prove this tool's database write support.
SQL uses autocommit without transactions. The example drops its own unique
output table; remove the shared input with the
[fixture cleanup](README.md#optional-sample-sql-table) after finishing the guides.
The historical run does not establish Enterprise, TLS/auth or distributed
execution qualification.
