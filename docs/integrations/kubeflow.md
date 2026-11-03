# Kubeflow Pipelines

[All integration guides](README.md). This guide reads SQL rows through Flight
SQL and writes a separate output table, then verifies it through a new SELECT.

## Install

Start the [server and sample table](README.md#optional-sample-sql-table).
From the repository root, create a separate environment:

```sh
python3.12 -m venv .venv-kubeflow
. .venv-kubeflow/bin/activate
python -m pip install -r requirements/integrations/orchestrators.txt
```

The profile pins the November 3 dependency versions. The examples default to
`grpc://localhost:8815` without client TLS/auth; use `FLIGHT_URI` for another
reachable local development endpoint. Use the [connection reference](../client-guide.md)
for secure deployment configuration.

## Read, write and verify

Save as `kubeflow_example.py` and run `python kubeflow_example.py`.

```python
from kfp import dsl, local

@dsl.component(base_image="python:3.12", install_kfp_package=False)
def copy_rows() -> int:
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
    return output.num_rows

if __name__ == "__main__":
    local.init(runner=local.SubprocessRunner(use_venv=False), pipeline_root="./flight-kfp-output")
    task = copy_rows()
    assert task.output == 3
```

This uses the local SubprocessRunner and the active virtual environment.
It does not install packages into a cluster component image or validate a
Kubernetes deployment. Keep the environment bin directory on PATH. Local
pipeline artifacts remain in `flight-kfp-output`; the SQL output is removed.

## Validation and cleanup

The [live guide recipe run](../validation/2026-11-03-guide-recipes/README.md) passed this example's read,
write, fresh read-back and cleanup. The report records runtime versions and
local execution limits.
SQL uses autocommit without transactions. The example drops its own unique
output table; remove the shared input with the
[fixture cleanup](README.md#optional-sample-sql-table) after finishing the guides.
The historical run does not establish Enterprise, TLS/auth or distributed
execution qualification.
