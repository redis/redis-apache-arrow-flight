"""All fixtures use Flight SQL; a native Redis driver is never loaded."""
import os
from uuid import uuid4

import pytest
import pyarrow as pa
import adbc_driver_flightsql.dbapi as dbapi


@pytest.fixture
def uri():
    value = os.environ.get("FLIGHT_URI")
    if not value:
        pytest.fail("FLIGHT_URI must name a disposable Flight SQL server")
    return value


@pytest.fixture
def connection(uri):
    with dbapi.connect(uri, autocommit=True) as conn:
        yield conn


@pytest.fixture
def workflow_rows(connection):
    name = "it_flight_sweep_" + uuid4().hex
    expected = pa.table({"id": [1, 2, 3], "label": ["alpha", None, "雪"],
                         "amount": [2.5, None, 7.5], "active": [True, None, False]})
    with connection.cursor() as cur:
        try:
            cur.adbc_ingest(name, expected, mode="create")
            cur.execute(f'SELECT * FROM "{name}" ORDER BY id')
            assert cur.fetch_arrow_table().equals(expected)
            yield name, expected
        finally:
            cur.execute(f'DROP TABLE IF EXISTS "{name}"')
            cur.fetch_arrow_table()
