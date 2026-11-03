# XGBoost

[All integration guides](README.md). Read training input through Flight SQL and persist local CPU predictions through ADBC. Model artifacts remain local unless you store them separately.

## Install

Start with the [server and sample table](README.md#optional-sample-sql-table).
Run from the repository root, using Python 3.12 in a separate environment:

```sh
python3.12 -m venv .venv-xgboost
. .venv-xgboost/bin/activate
python -m pip install -r requirements/integrations/models.txt
```

The profile pins the versions used in the November 3 run. It may install other
tools in the same profile. Use a client-reachable `FLIGHT_URI` if the endpoint
is elsewhere; the default below is a local unauthenticated development server.

## Read, write and verify

Save the following as `xgboost_example.py` and run it with
`python xgboost_example.py` in the active environment.
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

import xgboost as xgb
x = source["amount"].to_numpy().reshape(-1, 1)
y = source["id"].to_numpy().astype(float)
data = xgb.DMatrix(x, label=y)
model = xgb.train({"objective": "reg:squarederror", "max_depth": 2,
                   "nthread": 1, "seed": 0}, data, num_boost_round=4)
output = pa.table({"id": source["id"], "prediction": model.predict(data)})

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

The [November 3 evidence](../validation/2026-11-03-flight-integrations/README.md)
records the tested workflow for this tool; consult the
[tool report](../validation/2026-11-03-flight-integrations/reports/summary.md)
for its scope. The read/write recipe above is documentation added after that
run and has not been executed against a live endpoint. Historical model reads
or local artifact saves do not establish database write coverage. Writes here
use Arrow ingestion through Flight SQL; this is not a tool-native Redis writer.

Use a separate interpreter for each model example. The recorded combined
XGBoost/PyTorch runtime aborted; the runner isolates native ML runtimes.

SQL uses autocommit. Client results are materialized in memory; these examples
do not establish distributed execution, Enterprise qualification or TLS/auth.
Remove the shared input with the [fixture cleanup](README.md#optional-sample-sql-table).
