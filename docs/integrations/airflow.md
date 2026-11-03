# Apache Airflow

[All integration guides](README.md). This guide reads SQL rows through Flight
SQL and writes a separate output table, then verifies it through a new SELECT.

## Install

Start the [server and sample table](README.md#optional-sample-sql-table).
From the repository root, create a separate environment:

```sh
python3.14 -m venv .venv-airflow
. .venv-airflow/bin/activate
python -m pip install -r requirements/integrations/airflow.txt
export AIRFLOW_HOME="$(mktemp -d)"
export AIRFLOW__CORE__LOAD_EXAMPLES=False
airflow db migrate
```

The profile pins the November 3 dependency versions. The examples default to
`grpc://localhost:8815` without client TLS/auth; use `FLIGHT_URI` for another
reachable local development endpoint. Use the [connection reference](../client-guide.md)
for secure deployment configuration.

## Read, write and verify

Save as `airflow_example.py` and run `python airflow_example.py`.

```python
from airflow.sdk import dag, task
import pendulum

@dag(schedule=None, start_date=pendulum.datetime(2026, 1, 1, tz="UTC"), catchup=False)
def flight_workflow():
    @task
    def copy_rows():
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

    copy_rows()

if __name__ == "__main__":
    result = flight_workflow().test()
    assert result.state == "success", result.state
```

Use the Python version supported by the pinned Airflow package (the recorded
Airflow run used Python 3.14). Create an isolated local metadata database; run
`airflow db migrate` before executing the example. `dag.test()` runs locally,
without a deployed scheduler or a generic Flight provider hook.

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
