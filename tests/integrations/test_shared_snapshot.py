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
            # The C++ Flight SQL driver appends nonempty header values; clear
            # the prior value before replacing it or requests carry duplicates.
            owner.adbc_connection.set_options(**{HEADER: ""})
            owner.adbc_connection.set_options(**{HEADER: acquired["generation"]})
            with pytest.raises(AdbcError):
                execute(owner, 'SELECT 1')
            owner.adbc_connection.set_options(**{HEADER: ""})
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


@pytest.fixture
def snapshot_admin():
    """Explicit opt-in: an isolated catalog, including destructive lease faults."""
    if os.environ.get("FLIGHT_SNAPSHOT_FAULT_INJECTION") != "1":
        pytest.skip("set FLIGHT_SNAPSHOT_FAULT_INJECTION=1 on an isolated test database")
    import redis
    from urllib.parse import urlsplit
    uri = os.environ.get("REDIS_URI", "")
    parsed = urlsplit(uri)
    if parsed.scheme != "rediss" or not parsed.username or not parsed.password:
        pytest.fail("snapshot fault qualification requires authenticated verified Redis TLS")
    client = redis.Redis.from_url(
        uri, ssl_ca_certs=os.environ.get("REDIS_CA_FILE"),
        ssl_cert_reqs="required", ssl_check_hostname=True,
        decode_responses=True, socket_timeout=5,
    )
    try:
        assert client.ping()
        assert client.hget("adbc:{meta}:snapshot:catalog", "format") == "17"
        yield client
    finally:
        client.close()


_NATIVE_CATALOG = "adbc:{meta}:snapshot:catalog"


def native_pins(admin):
    return {field for field in admin.hkeys(_NATIVE_CATALOG) if field.startswith("p:")}


def acquired_pin(admin, before):
    added = native_pins(admin) - before
    assert len(added) == 1, "expected one newly acquired native pin"
    return added.pop()


def native_table_descriptor(admin, name):
    # Read-only protocol-17 inspector, matching internal/integrationtest. Resolving
    # root/bucket/owner atomically avoids combining different catalog generations.
    field = "table:public:" + name
    slot = 2166136261
    for byte in field.encode():
        slot = ((slot ^ byte) * 16777619) & 0xffffffff
    raw = admin.eval("""
local prefix='adbc:{meta}:snapshot:17:'
if redis.call('HGET',KEYS[1],'format')~='17' then return redis.error_reply('unsupported native format') end
local root=redis.call('HGET',KEYS[1],'root')
if not root then return redis.error_reply('missing native root') end
local bucket=redis.call('HGET',prefix..'metadata:root:'..root,ARGV[1])
if not bucket then return false end
local owner=redis.call('HGET',prefix..'metadata:bucket:'..bucket..':'..ARGV[1],ARGV[2])
if not owner then return false end
return redis.call('GET',prefix..'workspace:'..owner..':'..ARGV[2])
""", 1, _NATIVE_CATALOG, f"{slot & 255:02x}", field)
    assert raw, "managed table descriptor missing"
    descriptor = json.loads(raw)
    assert descriptor["name"] == name and descriptor["schema"] == "public"
    return descriptor


def test_native_lease_loss_never_falls_back(snapshot_connect, snapshot_admin):
    with snapshot_connect() as conn:
        before = native_pins(snapshot_admin)
        with shared_snapshot(conn):
            pin = acquired_pin(snapshot_admin, before)
            # Deliberate fault injection touches only this attempt's exact pin.
            assert snapshot_admin.hdel(_NATIVE_CATALOG, pin) == 1
            for _ in range(2):
                with pytest.raises(AdbcError):
                    execute(conn, "SELECT 1")
            status = json.loads(conn.adbc_connection.get_option(SESSION + "redis.read_snapshot_status"))
            assert status["state"] == "failed"


def test_snapshot_survives_observed_compaction(snapshot_connect, snapshot_admin):
    name = "it_flight_compact_" + uuid4().hex
    with snapshot_connect() as writer, snapshot_connect() as reader:
        try:
            execute(writer, f"CREATE TABLE {name}(id BIGINT, v BIGINT)")
            execute(writer, f"INSERT INTO {name} SELECT i,10 FROM generate_series(1,2001) AS g(i)")
            expected = execute(writer, f"SELECT * FROM {name} ORDER BY id,v NULLS LAST")
            with shared_snapshot(reader):
                schema = reader.adbc_get_table_schema(name)
                initial = native_table_descriptor(snapshot_admin, name)["index_name"]
                for i in range(12):
                    execute(writer, f"UPDATE {name} SET v={i+11} WHERE __rowid=1")
                deadline = time.monotonic() + 30
                while True:
                    descriptor = native_table_descriptor(snapshot_admin, name)
                    indexes = [descriptor["index_name"]] + [
                        segment["index"] for segment in descriptor.get("snapshot_segments", [])
                    ]
                    if any(index != initial and index.endswith("_compacted") for index in indexes):
                        break
                    assert time.monotonic() < deadline, "no installed compaction observed"
                    time.sleep(0.1)
                assert reader.adbc_get_table_schema(name).equals(schema, check_metadata=True)
                assert execute(reader, f"SELECT * FROM {name} ORDER BY id,v NULLS LAST").equals(expected, check_metadata=True)
                assert execute(writer, f"SELECT v FROM {name} WHERE id=1").column(0).to_pylist() == [22]
        finally:
            execute(writer, f"DROP TABLE IF EXISTS {name}")


def test_gateway_kill_restart_loses_session_and_expires_pin(snapshot_connect, snapshot_admin):
    import re
    import subprocess
    if os.environ.get("FLIGHT_SNAPSHOT_LONG_TESTS") != "1":
        pytest.skip("set FLIGHT_SNAPSHOT_LONG_TESTS=1 for natural native lease expiry")
    container = os.environ.get("FLIGHT_SNAPSHOT_GATEWAY_CONTAINER", "")
    if not re.fullmatch(r"[a-zA-Z0-9][a-zA-Z0-9_.-]*", container):
        pytest.fail("set FLIGHT_SNAPSHOT_GATEWAY_CONTAINER to the disposable gateway container")
    with snapshot_connect() as conn:
        before = native_pins(snapshot_admin)
        with pytest.raises(AdbcError, match="session is unknown|shared read snapshot unavailable"):
            with shared_snapshot(conn):
                pin = acquired_pin(snapshot_admin, before)
                subprocess.run(["docker", "kill", "--signal", "KILL", container], check=True, capture_output=True)
                subprocess.run(["docker", "start", container], check=True, capture_output=True)
                deadline = time.monotonic() + 20
                while True:
                    try:
                        with snapshot_connect() as fresh:
                            assert execute(fresh, "SELECT 1").num_rows == 1
                        break
                    except AdbcError:
                        assert time.monotonic() < deadline, "restarted gateway did not become ready"
                        time.sleep(0.1)
                # Keep the gateway bearer-token secret stable across restart: this
                # must reject lost snapshot/session state, not merely a rotated token.
                execute(conn, "SELECT 1")
        # Process death cannot close native connections. Verify actual expiry and
        # physical sweep, rather than deleting the pin or accelerating Redis time.
        deadline = time.monotonic() + 135
        while snapshot_admin.hexists(_NATIVE_CATALOG, pin):
            assert time.monotonic() < deadline, "dead gateway's native pin was not collected"
            with snapshot_connect() as fresh:
                execute(fresh, "SELECT 1")
            time.sleep(0.2)
