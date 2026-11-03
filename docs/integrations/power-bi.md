# Power BI

[All integration guides](README.md). This project ships no Power BI connector,
and no Power BI application integration has been validated. The available path
requires developing a custom ADBC-based Power Query connector.

## Develop a connector

Follow Microsoft's [ADBC connector development guide](https://learn.microsoft.com/en-us/power-query/adbc),
which uses a Flight SQL sample. Its driver configuration and `Adbc.Connection`
pattern are a starting point; the sample's SQL generation and metadata behavior
must be adapted to this server.

1. Start the Flight server and create the [sample SQL table](README.md#optional-sample-sql-table).
2. Set up the Power Query SDK and Flight SQL driver configuration following
   Microsoft's sample. Check the driver availability for the actual Power BI runtime.
3. Map host, port, encryption and authentication inputs to the Flight SQL
   connection properties. The local endpoint is `grpc://localhost:8815`.
4. Adapt catalog/schema/table navigation for `redis` / `public`, Arrow-to-Power
   Query type mappings and generated SQL to the [server's SQL reference](../sql-reference.md).
5. Build and load the custom connector using Microsoft's development workflow,
   then test it in Power BI Desktop.

These steps describe connector work still required, rather than an existing
plug-in installation. PostgreSQL and generic Redis connectors do not connect
to the Flight endpoint.

## Acceptance checks

Check connection, navigation, column types, ordered reads, NULLs and empty
results first. The sample table has three rows and `SUM(amount) = 7.5`.
Then test query folding for filters, projections and aggregates, refresh, and
the intended deployment/gateway runtime. Record the connector, driver and
Power BI versions with results before claiming compatibility.

SQL is autocommit without transactions. Named Arrow datasets are separate from
SQL tables and need a native Flight client or explicit dataset-to-SQL import.
Use the [fixture cleanup](README.md#optional-sample-sql-table) when done.

## Write workflow and limits

Power BI has no validated Flight SQL write-back route in this repository.
Use an external ADBC ingestion job, then refresh the configured read integration.
This writes through Python, not through Power BI. Install the
[Python client environment](README.md#optional-sample-sql-table) and create the
shared sample first. Ensure id `99` is unused in that disposable sample, then run:

```python
import adbc_driver_flightsql.dbapi as sql
with sql.connect("grpc://localhost:8815", autocommit=True) as conn, conn.cursor() as cur:
    cur.execute("SELECT id FROM flight_tool_sample WHERE id = 99")
    assert cur.fetch_arrow_table().num_rows == 0, "choose a different unused id"
    cur.execute("INSERT INTO flight_tool_sample (id, label, amount) VALUES (99, 'guide', 9.5)")
    cur.fetch_arrow_table()
    cur.execute("SELECT label, amount FROM flight_tool_sample WHERE id = 99")
    assert cur.fetch_arrow_table().to_pylist() == [{"label": "guide", "amount": 9.5}]
```

Refresh the Power BI read preview after configuring its connection; expect the
new row `99 / guide / 9.5`. Power BI still requires the connector development
and acceptance work above before this read can be attempted.
Do not use preview or refresh queries as DML editors. After the read check,
remove only the row you inserted through the same Python environment:

```python
import adbc_driver_flightsql.dbapi as sql
with sql.connect("grpc://localhost:8815", autocommit=True) as conn, conn.cursor() as cur:
    cur.execute("DELETE FROM flight_tool_sample WHERE id = 99")
    cur.fetch_arrow_table()
    cur.execute("SELECT id FROM flight_tool_sample WHERE id = 99")
    assert cur.fetch_arrow_table().num_rows == 0
```

These are illustrative external ingestion steps, not a recorded application
write pass. This documentation update has not rerun them live.
