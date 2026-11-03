# Quarto

[All integration guides](README.md). Query Flight SQL in a Python document cell
and write a derived SQL table through ADBC during rendering.

## Install

Install [Quarto](https://quarto.org/docs/get-started/) or use the bundled Quarto
in RStudio. Start the [server and sample table](README.md#optional-sample-sql-table),
then from the repository root:

```sh
python3.12 -m venv .venv-quarto
. .venv-quarto/bin/activate
python -m pip install -r requirements/integrations/documents.txt
export QUARTO_PYTHON="$(command -v python)"
```

## Read, write and verify

Save this as `flight.qmd`. The Python cell reads the input, writes doubled
amounts into a unique table, verifies stored values and drops its output.
Rendering executes the write each time; this example intentionally cleans it up.

````markdown
---
title: Flight SQL read and write
format: html
jupyter: python3
---

```{python}
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
```
````

Run `quarto render flight.qmd` in the active environment and inspect `flight.html`.
Use `FLIGHT_URI` to override the local unauthenticated endpoint. The HTML remains
on disk; remove the shared input with the [fixture cleanup](README.md#optional-sample-sql-table).

## Validation and limits

The [live guide run](../validation/2026-11-03-guide-recipes/README.md) passed remote reads,
ADBC ingestion, exact read-back, cleanup and HTML rendering with the executed
Python cell. The document profile explicitly includes PyYAML. This uses ADBC
in a document cell, not a packaged Quarto database connector. It does not
establish production deployment or client TLS/authentication coverage.
