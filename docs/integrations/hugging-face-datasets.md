# Hugging Face Datasets

[All integration guides](README.md). Download SQL rows into a local Dataset, filter locally and write the selected rows to a new SQL table. This is an Arrow bridge, not a packaged Redis connector.

## Install

Start with the [server and sample table](README.md#optional-sample-sql-table).
Run from the repository root, using Python 3.12 in a separate environment:

```sh
python3.12 -m venv .venv-hugging-face-datasets
. .venv-hugging-face-datasets/bin/activate
python -m pip install -r requirements/integrations/workflows.txt
```

The profile pins the versions used in the November 3 run. It may install other
tools in the same profile. Use a client-reachable `FLIGHT_URI` if the endpoint
is elsewhere; the default below is a local unauthenticated development server.

## Read, write and verify

Save the following as `hugging_face_datasets_example.py` and run it with
`python hugging_face_datasets_example.py` in the active environment.
The code reads `flight_tool_sample`, writes the tool's output to a unique SQL
table, verifies it with a fresh ordered SELECT and drops only that output table.
Keep the shared input table until you finish the guides.

```python
import os
import uuid
import pyarrow as pa
import adbc_driver_flightsql.dbapi as sql

uri = os.environ.get("FLIGHT_URI", "grpc://localhost:8815")
with sql.connect(uri, autocommit=True) as conn, conn.cursor() as cur:
    cur.execute("SELECT id, label, amount FROM flight_tool_sample ORDER BY id")
    source = cur.fetch_arrow_table()

from datasets import Dataset
local = Dataset(source)
filtered = local.filter(lambda row: row["id"] >= 2, load_from_cache_file=False)
output = pa.Table.from_pylist(filtered.to_list(), schema=source.schema)

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
```

## Validation and limits

The [live guide recipe run](../validation/2026-11-03-guide-recipes/README.md) passed this example's remote
read, output write, fresh ordered read-back and cleanup. The report records
runtime versions and the precise scope. Writes use Arrow ingestion through
Flight SQL; native writer API support is not implied.

Datasets 5.1.0 has a known default empty save/reload failure. If persisting an
empty local Dataset, use `dataset.save_to_disk(path, num_shards=1)`. See
[issue #57](https://github.com/redis/redis-apache-arrow-flight/issues/57).

SQL uses autocommit. Client results are materialized in memory; these examples
do not establish distributed execution, Enterprise qualification or TLS/auth.
Remove the shared input with the [fixture cleanup](README.md#optional-sample-sql-table).
