# Python / PyArrow

[All integration guides](README.md). Use ADBC Flight SQL for SQL tables and
`pyarrow.flight` for named datasets. Both paths have recorded passing smoke
checks; see [versions and scope](../client-compatibility.md).

## Install and query SQL

Follow the [prerequisites and sample table setup](README.md), which installs
the recorded Python client versions. Run in that environment:

```python
import adbc_driver_flightsql.dbapi as sql

with sql.connect("grpc://localhost:8815", autocommit=True) as conn, conn.cursor() as cur:
    cur.execute("SELECT id, label, amount FROM flight_tool_sample WHERE id >= ? ORDER BY id", (2,))
    print(cur.fetch_arrow_table())
```

Expect ids 2 and 3, including a NULL label for id 2. For writes, execute DDL or
DML separately, consume the response, and read back the affected rows. For bulk
ingestion use `cur.adbc_ingest(name, arrow_table, mode="create")`, as in the
sample setup. Transactions are unsupported; keep `autocommit=True`.

## Upload and download a dataset

This example uses a unique dataset name, reads every endpoint and removes its
dataset afterward. It does not create a SQL table.

```python
import uuid
import pyarrow as pa
import pyarrow.flight as flight

client = flight.connect("grpc://localhost:8815")
name = "guide_" + uuid.uuid4().hex
desc = flight.FlightDescriptor.for_path(name)
try:
    source = pa.table({"id": [1, 2], "embedding": [[0.1, 0.2], [0.3, 0.4]]})
    writer, metadata = client.do_put(desc, source.schema)
    with writer:
        writer.write_table(source)
        writer.done_writing()
        print(metadata.read())  # Consume the upload acknowledgement.
    info = client.get_flight_info(desc)
    received = pa.concat_tables([
        client.do_get(ep.ticket).read_all() for ep in info.endpoints
    ])
    assert received.equals(source)
finally:
    try:
        list(client.do_action(flight.Action("drop_dataset", name.encode())))
    finally:
        client.close()
```

For discovery, projection, batch selection and storage details, see the
[dataset reference](../data-access.md#datasets). To turn a dataset into SQL
rows, follow the [dataset-to-SQL workflow](../dataset-sql-workflow.md).

## Reproduce recorded checks

Against a disposable running server, from the repository root:

```sh
python examples/python/quickstart.py --uri grpc://localhost:8815
python examples/python/dataset_to_sql.py --uri grpc://localhost:8815
```

The scripts create and clean up fixtures; they also accept `--user` and
`--password`. See the [ADBC Flight SQL driver documentation](https://arrow.apache.org/adbc/current/driver/flight_sql.html)
for driver options and the [full client examples](../client-guide.md#examples).
