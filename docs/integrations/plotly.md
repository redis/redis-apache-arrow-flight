# Plotly

[All integration guides](README.md). Plot remote SQL results locally and write the plotted point values back through ADBC. The self-contained HTML file stays on disk.

## Install

Start with the [server and sample table](README.md#optional-sample-sql-table).
Run from the repository root, using Python 3.12 in a separate environment:

```sh
python3.12 -m venv .venv-plotly
. .venv-plotly/bin/activate
python -m pip install -r requirements/integrations/models.txt
```

The profile pins the versions used in the November 3 run. It may install other
tools in the same profile. Use a client-reachable `FLIGHT_URI` if the endpoint
is elsewhere; the default below is a local unauthenticated development server.

## Read, write and verify

Save the following as `plotly_example.py` and run it with
`python plotly_example.py` in the active environment.
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

import plotly.graph_objects as go
figure = go.Figure(go.Scatter(x=source["id"].to_pylist(),
                              y=source["amount"].to_pylist(), mode="lines+markers"))
figure.write_html("flight-plot.html", include_plotlyjs=True)
# Persist the plotted points, not the HTML artifact.
output = pa.table({"id": list(figure.data[0].x), "amount": list(figure.data[0].y)})

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

Use a separate interpreter for each model example. The recorded combined
XGBoost/PyTorch runtime aborted; the runner isolates native ML runtimes.

SQL uses autocommit. Client results are materialized in memory; these examples
do not establish distributed execution, Enterprise qualification or TLS/auth.
Remove the shared input with the [fixture cleanup](README.md#optional-sample-sql-table).
