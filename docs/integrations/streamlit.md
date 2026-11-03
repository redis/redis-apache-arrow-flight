# Streamlit

[All integration guides](README.md). Read through Flight SQL in a Streamlit app and use a button to write transformed rows via ADBC. The historical automated route did not validate browser rendering.

## Install

Start with the [server and sample table](README.md#optional-sample-sql-table).
Run from the repository root, using Python 3.12 in a separate environment:

```sh
python3.12 -m venv .venv-streamlit
. .venv-streamlit/bin/activate
python -m pip install -r requirements/integrations/workflows.txt
```

The profile pins the versions used in the November 3 run. It may install other
tools in the same profile. Use a client-reachable `FLIGHT_URI` if the endpoint
is elsewhere; the default below is a local unauthenticated development server.

## Read, write and verify

Save as `flight_app.py` and run `streamlit run flight_app.py`. Open the local
URL printed by Streamlit and click the write button.
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

import streamlit as st
st.title("Flight SQL read and write")
st.dataframe(source.to_pandas())
# Each button click creates, verifies and drops a unique output fixture.
if not st.button("Write doubled amounts and verify"):
    st.stop()
output = pa.table({"id": source["id"],
                   "amount": [value * 2 for value in source["amount"].to_pylist()]})
st.write("Writing a temporary SQL table; see the terminal for read-back.")

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

SQL uses autocommit. Client results are materialized in memory; these examples
do not establish distributed execution, Enterprise qualification or TLS/auth.
Remove the shared input with the [fixture cleanup](README.md#optional-sample-sql-table).
