"""Issue #65 transport qualification using Python ADBC Flight SQL only.

Run against a disposable, freshly qualified enterprise gateway and Redis catalog.
Required environment: FLIGHT_URI (grpc+tls://), FLIGHT_USERNAME, FLIGHT_PASSWORD.
Optional: FLIGHT_TLS_ROOT_CERTS (PEM file); FLIGHT_SNAPSHOT_LONG_TESTS=1 (expiry).
No native Redis ADBC library is loaded by this test.
"""
import importlib.util
import json
import os
from pathlib import Path
import time
from uuid import uuid4

import adbc_driver_flightsql.dbapi as dbapi
from adbc_driver_manager import Error as AdbcError
import pytest

_spec = importlib.util.spec_from_file_location(
    "shared_snapshot_example",
    Path(__file__).resolve().parents[2] / "examples/python/shared_snapshot.py",
)
_example = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_example)
shared_snapshot = _example.shared_snapshot
SESSION, HEADER = _example.SESSION, _example.HEADER


@pytest.fixture
def snapshot_connect():
    uri = os.environ.get("FLIGHT_URI", "")
    username = os.environ.get("FLIGHT_USERNAME", "")
    password = os.environ.get("FLIGHT_PASSWORD", "")
    if not uri.startswith("grpc+tls://") or not username or not password:
        pytest.fail("shared snapshot qualification requires TLS Flight URI and credentials")
    kwargs = {
        "username": username, "password": password,
        "adbc.flight.sql.rpc.with_cookie_middleware": "true",
    }
    if os.environ.get("FLIGHT_TLS_ROOT_CERTS"):
        kwargs["adbc.flight.sql.client_option.tls_root_certs"] = Path(
            os.environ["FLIGHT_TLS_ROOT_CERTS"]
        ).read_text()
    return lambda: dbapi.connect(uri, db_kwargs=kwargs, autocommit=True)


def execute(conn, sql):
    with conn.cursor() as cur:
        cur.execute(sql)
        return cur.fetch_arrow_table()


def objects(conn):
    with conn.adbc_get_objects() as reader:
        table = reader.read_all()
    # Metadata collection ordering is unspecified. Preserve all duplicate entries
    # and column ordinals while canonicalizing unordered catalog/schema/table lists.
    records = table.to_pylist()
    key = lambda row: json.dumps(row, sort_keys=True)
    for catalog in records:
        schemas = catalog.get("catalog_db_schemas") or []
        for schema in schemas:
            tables = schema.get("db_schema_tables") or []
            tables.sort(key=key)
        schemas.sort(key=key)
    records.sort(key=key)
    return table.schema, records


def test_two_tables_metadata_and_publications(snapshot_connect):
    suffix = uuid4().hex
    a, b, view = ("it_flight_snapshot_" + kind + suffix for kind in ("a", "b", "v"))
    with snapshot_connect() as writer, snapshot_connect() as reader:
        try:
            execute(writer, f'CREATE TABLE {a}(id INTEGER, v BIGINT)')
            execute(writer, f'CREATE TABLE {b}(id INTEGER, v BIGINT)')
            for name in (a, b):
                execute(writer, f'INSERT INTO {name} VALUES (1,9007199254740993),(1,9007199254740993),(2,NULL)')
            execute(writer, f'CREATE VIEW {view} AS SELECT id,v FROM {b}')
            expected = {name: execute(writer, f'SELECT * FROM {name} ORDER BY id,v NULLS LAST') for name in (a, b, view)}
            with shared_snapshot(reader) as acquired:
                schemas = {name: reader.adbc_get_table_schema(name) for name in (a, b, view)}
                before_schema, before_objects = objects(reader)
                assert execute(reader, f'SELECT * FROM {a} ORDER BY id,v NULLS LAST').equals(expected[a], check_metadata=True)
                # Native compaction is exercised by repeated supported publications.
                for _ in range(9):
                    execute(writer, f'UPDATE {b} SET v=v+1')
                execute(writer, f'ALTER TABLE {b} ADD COLUMN newer INTEGER')
                execute(writer, f'DELETE FROM {b} WHERE id=2')
                execute(writer, f'TRUNCATE TABLE {a}')
                execute(writer, f'DROP VIEW {view}')
                execute(writer, f'DROP TABLE {b}')
                for name in (a, b, view):
                    assert reader.adbc_get_table_schema(name).equals(schemas[name], check_metadata=True)
                    actual = execute(reader, f'SELECT * FROM {name} ORDER BY id,v NULLS LAST')
                    assert actual.equals(expected[name], check_metadata=True)
                    assert actual.num_rows == 3  # duplicate multiplicity is part of equality
                after_schema, after_objects = objects(reader)
                assert after_schema.equals(before_schema, check_metadata=True)
                assert after_objects == before_objects
                status = json.loads(reader.adbc_connection.get_option(SESSION + "redis.read_snapshot_status"))
                assert status == acquired
            with pytest.raises(AdbcError):
                execute(reader, f'SELECT * FROM {a}')  # no live reads after release
        finally:
            for sql in (f'DROP VIEW IF EXISTS {view}', f'DROP TABLE IF EXISTS {b}', f'DROP TABLE IF EXISTS {a}'):
                execute(writer, sql)


def test_handle_requires_own_session(snapshot_connect):
    with snapshot_connect() as owner, snapshot_connect() as stranger:
        with shared_snapshot(owner) as acquired:
            stranger.adbc_connection.set_options(**{HEADER: acquired["handle"]})
            with pytest.raises(AdbcError):
                execute(stranger, 'SELECT 1')
            # A diagnostic native generation is never accepted as the handle.
            owner.adbc_connection.set_options(**{HEADER: acquired["generation"]})
            with pytest.raises(AdbcError):
                execute(owner, 'SELECT 1')
            owner.adbc_connection.set_options(**{HEADER: acquired["handle"]})


def test_failure_releases_attempt(snapshot_connect):
    with snapshot_connect() as conn:
        with pytest.raises(RuntimeError, match="abandoned sync"):
            with shared_snapshot(conn):
                raise RuntimeError("abandoned sync")
        status = json.loads(conn.adbc_connection.get_option(SESSION + "redis.read_snapshot_status"))
        assert status["state"] == "released"
        with pytest.raises(AdbcError):
            execute(conn, 'SELECT 1')


def test_expiry_never_falls_back(snapshot_connect):
    if os.environ.get("FLIGHT_SNAPSHOT_LONG_TESTS") != "1":
        pytest.skip("set FLIGHT_SNAPSHOT_LONG_TESTS=1 for the fixed native two-minute minimum")
    with snapshot_connect() as conn:
        with shared_snapshot(conn, retention_ms=120000):
            time.sleep(121)
            with pytest.raises(AdbcError):
                execute(conn, 'SELECT 1')
            with pytest.raises(AdbcError):
                execute(conn, 'SELECT 1')
