"""Rerun the already exercised Python Flight SQL routes."""

def test_pyarrow(uri, workflow_rows):
    import adbc_driver_flightsql.dbapi as sql
    import pyarrow as pa
    name, expected = workflow_rows
    with sql.connect(uri, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(f'SELECT * FROM "{name}" ORDER BY id')
        batches = list(cur.fetch_record_batch())
        assert pa.Table.from_batches(batches).equals(expected)
        cur.execute(f'SELECT * FROM "{name}" WHERE id < 0')
        empty = cur.fetch_arrow_table()
        assert empty.num_rows == 0 and empty.schema == expected.schema


def test_pandas(uri, workflow_rows):
    import pandas as pd
    import adbc_driver_flightsql.dbapi as sql
    with sql.connect(uri, autocommit=True) as conn:
        actual = pd.read_sql(f'SELECT * FROM "{workflow_rows[0]}" ORDER BY id', conn)
    pd.testing.assert_frame_equal(actual, workflow_rows[1].to_pandas())


def test_polars(uri, workflow_rows):
    import polars as pl
    import adbc_driver_flightsql.dbapi as sql
    with sql.connect(uri, autocommit=True) as conn:
        actual = pl.read_database(f'SELECT * FROM "{workflow_rows[0]}" ORDER BY id', conn)
    assert actual.equals(pl.from_arrow(workflow_rows[1]))
