# Metaflow

[All integration guides](README.md). This guide reads SQL rows through Flight
SQL and writes a separate output table, then verifies it through a new SELECT.

## Install

Start the [server and sample table](README.md#optional-sample-sql-table).
From the repository root, create a separate environment:

```sh
python3.12 -m venv .venv-metaflow
. .venv-metaflow/bin/activate
python -m pip install -r requirements/integrations/orchestrators.txt
```

The profile pins the November 3 dependency versions. The examples default to
`grpc://localhost:8815` without client TLS/auth; use `FLIGHT_URI` for another
reachable local development endpoint. Use the [connection reference](../client-guide.md)
for secure deployment configuration.

## Read, write and verify

Save as `metaflow_example.py`, set `METAFLOW_DEFAULT_DATASTORE=local` and
`METAFLOW_DEFAULT_METADATA=local`, then run `python metaflow_example.py run`.
If the local runtime cannot identify a user, set `METAFLOW_USER` to your username.

```python
from metaflow import FlowSpec, step

class FlightFlow(FlowSpec):
    @step
    def start(self):
        import os
        import uuid
        import pyarrow as pa
        import adbc_driver_flightsql.dbapi as sql

        uri = os.environ.get("FLIGHT_URI", "grpc://localhost:8815")
        with sql.connect(uri, autocommit=True) as conn, conn.cursor() as cur:
            cur.execute("SELECT id, label, amount FROM flight_tool_sample ORDER BY id")
            source = cur.fetch_arrow_table()
        output = pa.table({"id": source["id"],
                           "amount": [value * 2 for value in source["amount"].to_pylist()]})

        # Write the tool's output through Flight SQL, then verify the stored values.
        name = "guide_" + uuid.uuid4().hex
        with sql.connect(uri, autocommit=True) as conn, conn.cursor() as cur:
            try:
                cur.adbc_ingest(name, output, mode="create")
                cur.execute(f'SELECT * FROM "{name}" ORDER BY id')
                received = cur.fetch_arrow_table()
                assert received.equals(output)
                print(received)
            finally:
                cur.execute(f'DROP TABLE IF EXISTS "{name}"')
                cur.fetch_arrow_table()
        self.count = output.num_rows
        self.next(self.end)

    @step
    def end(self):
        assert self.count == 3
        print("Flight read/write verified")

if __name__ == "__main__":
    FlightFlow()
```

Use the local datastore and metadata service for this example; cloud execution
and retry guarantees were not tested. Local `.metaflow` artifacts remain on
disk. The connection is opened in the step, not serialized across steps.

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
