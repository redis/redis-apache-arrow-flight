# Data tool integration guides

[Back to README data tools](../../README.md#data-tools).

Start a [local server](../local-development.md) or
[install a release](../../README.md#install-a-release). SQL needs Redis with
Query Engine (Search); 4× QPF is recommended where supported. SQL uses the
Redis ADBC driver's [data model](../../README.md#choose-an-interface), and runs
in autocommit mode without transactions. Named Arrow datasets are separate
from SQL tables.

Local examples assume an unauthenticated endpoint without TLS:

```text
Flight / ADBC: grpc://localhost:8815
JDBC: jdbc:arrow-flight-sql://localhost:8815/?useEncryption=false
```

Use an address reachable from the client. Inside a Docker Desktop container,
`host.docker.internal` reaches a Flight server on the host; `localhost` refers
to the container itself. For authenticated or TLS deployments, follow the
[connection options](../client-guide.md#jdbc-dbeaver-and-others) and
[server configuration](../server-guide.md#deployment-and-operations).

## Guides

| Integration | Guide |
| --- | --- |
| Python / PyArrow | [SQL and native datasets](python.md) |
| Go | [Flight SQL and native Flight](go.md) |
| Java | [JDBC application](java.md) |
| DBeaver | [Driver setup and manual tests](../dbeaver.md) |
| Other JDBC applications | [Custom driver configuration](jdbc-applications.md) |
| pandas | [SQL reads and parameter binding](pandas.md) |
| Polars | [SQL reads and batch iteration](polars.md) |
| DuckDB | [Local SQL over downloaded Arrow](duckdb.md) |
| R | [ADBC driver manager](r.md) |
| Apache Superset | [Version-specific connection guidance](../superset.md) |
| Tableau | [JDBC setup and read preview](tableau.md) |
| Metabase | [Community plugin and Actions](metabase.md) |
| Power BI | [Custom connector development path](power-bi.md) |
| Redis Insight | [Inspect the backing Redis database](redis-insight.md) |

## Optional sample SQL table

The pandas, Polars, DuckDB, Go, Java, JDBC and BI guides use
`flight_tool_sample`. Run these commands from the repository root against a
disposable backend. The table name must be unused; `mode="create"` refuses
to replace an existing table.

```sh
python -m venv .venv-tools
. .venv-tools/bin/activate
python -m pip install -r examples/python/requirements-clients.txt
python - <<'PY'
import adbc_driver_flightsql.dbapi as sql
import pyarrow as pa

with sql.connect("grpc://localhost:8815", autocommit=True) as conn, conn.cursor() as cur:
    cur.adbc_ingest("flight_tool_sample", pa.table({
        "id": [1, 2, 3],
        "label": ["one", None, "three"],
        "amount": [1.5, 2.5, 3.5],
    }), mode="create")
PY
```

Keep the table while following the guides. An ordered SELECT should return
three rows, with a NULL label on row 2; `SUM(amount)` is `7.5`. When done, drop
only the fixture you created:

```sh
python - <<'PY'
import adbc_driver_flightsql.dbapi as sql
with sql.connect("grpc://localhost:8815", autocommit=True) as conn, conn.cursor() as cur:
    cur.execute("DROP TABLE flight_tool_sample")
    cur.fetch_arrow_table()
PY
```

These guides describe setup and expected checks. Historical passing results,
versions and application limits remain in the [compatibility matrix](../client-compatibility.md).
Creating a fixture through Python does not establish write support in another tool.
