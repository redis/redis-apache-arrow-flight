# SQLAlchemy

[All integration guides](README.md). This guide reads SQL rows through Flight
SQL and writes a separate output table, then verifies it through a new SELECT.

## Install

Start the [server and sample table](README.md#optional-sample-sql-table).
From the repository root, create a separate environment:

```sh
python3.12 -m venv .venv-sqlalchemy
. .venv-sqlalchemy/bin/activate
python -m pip install -r requirements/integrations/core.txt
```

The profile pins the November 3 dependency versions. The examples default to
`grpc://localhost:8815` without client TLS/auth; use `FLIGHT_URI` for another
reachable local development endpoint. Use the [connection reference](../client-guide.md)
for secure deployment configuration.

## Read, write and verify

Save as `sqlalchemy_example.py` and run `python sqlalchemy_example.py`.

```python
import os
import uuid
import sqlalchemy as sa
uri = os.environ.get("FLIGHT_URI", "grpc://localhost:8815")
engine = sa.create_engine("adbc+flightsql://" + uri.removeprefix("grpc://") + "?tls=false",
                          connect_args={"autocommit": True})
name = "guide_" + uuid.uuid4().hex
try:
    with engine.connect() as conn:
        source = conn.exec_driver_sql(
            "SELECT id, label, amount FROM flight_tool_sample ORDER BY id").mappings().all()
        try:
            conn.exec_driver_sql(f'CREATE TABLE "{name}" (id BIGINT NOT NULL, label VARCHAR, amount DOUBLE)').close()
            for row in source:
                conn.exec_driver_sql(f'INSERT INTO "{name}" (id, label, amount) VALUES (?, ?, ?)',
                                     (row["id"], row["label"], row["amount"] * 2)).close()
            received = conn.exec_driver_sql(f'SELECT * FROM "{name}" ORDER BY id').mappings().all()
            assert [row["amount"] for row in received] == [3.0, 5.0, 7.0]
            print(received)
        finally:
            conn.exec_driver_sql(f'DROP TABLE IF EXISTS "{name}"').close()
finally:
    engine.dispose()
```

Use DBAPI `connect_args={"autocommit": True}`. The adapter 0.3.0 SQLAlchemy
`isolation_level="AUTOCOMMIT"` setter is unimplemented. Do not rely on rollback.
This is the Flight SQL dialect, separate from the direct Redis ADBC dialect.

## Validation and cleanup

The [live guide recipe run](../validation/2026-11-03-guide-recipes/README.md) passed this example's read,
write, fresh read-back and cleanup. The report records runtime versions and
local execution limits.
SQL uses autocommit without transactions. The example drops its own unique
output table; remove the shared input with the
[fixture cleanup](README.md#optional-sample-sql-table) after finishing the guides.
The historical run does not establish Enterprise, TLS/auth or distributed
execution qualification.
