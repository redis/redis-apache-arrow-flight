"""Issue #61: independent standard clients over authenticated verified TLS.

Run explicitly with make test-companion-contract against disposable Software.
Only Apache's Flight SQL/Flight clients are loaded; never the native Redis driver.
"""
from contextlib import contextmanager
import os
from pathlib import Path
from uuid import uuid4

import adbc_driver_flightsql.dbapi as sql
from adbc_driver_manager import NotSupportedError
import pyarrow as pa
import pyarrow.flight as flight
import pytest


@pytest.fixture
def clients():
    uri = os.environ.get('FLIGHT_URI', '')
    username, password = (os.environ.get(k, '') for k in ('FLIGHT_USERNAME', 'FLIGHT_PASSWORD'))
    if not uri.startswith('grpc+tls://') or not username or not password:
        pytest.fail('companion qualification requires verified TLS Flight and credentials')
    roots = Path(os.environ['FLIGHT_TLS_ROOT_CERTS']).read_bytes() if os.environ.get('FLIGHT_TLS_ROOT_CERTS') else None
    kwargs = {'username': username, 'password': password,
              'adbc.flight.sql.rpc.with_cookie_middleware': 'true'}
    if roots:
        kwargs['adbc.flight.sql.client_option.tls_root_certs'] = roots.decode()

    @contextmanager
    def connect():
        with sql.connect(uri, db_kwargs=kwargs, autocommit=True) as conn:
            yield conn

    @contextmanager
    def rpc():
        client = flight.connect(uri, tls_root_certs=roots)
        try:
            token = client.authenticate_basic_token(username, password)
            yield client, flight.FlightCallOptions(headers=[token])
        finally:
            client.close()

    return connect, rpc


def catalog_schema(schema):
    names = {pa.int64(): b'BIGINT', pa.string(): b'VARCHAR',
             pa.float64(): b'DOUBLE PRECISION', pa.bool_(): b'BOOLEAN'}
    return pa.schema([f.with_metadata({b'ARROW:FLIGHT:SQL:TYPE_NAME': names[f.type]})
                      for f in schema])


@pytest.fixture
def managed_table(clients):
    connect, _ = clients
    name = 'it_flight_contract_' + uuid4().hex
    expected = pa.table({'id': pa.array([1, 1, 9007199254740993], pa.int64()),
                         'label': ['雪', '雪', None],
                         'amount': pa.array([2.5, 2.5, None], pa.float64()),
                         'active': [True, True, None]})
    expected = expected.cast(catalog_schema(expected.schema))
    with connect() as conn, conn.cursor() as cur:
        try:
            assert cur.adbc_ingest(name, expected, mode='create') == 3
            yield name, expected
        finally:
            cur.execute(f'DROP TABLE IF EXISTS "{name}"')
            cur.fetch_arrow_table()


def test_sql_metadata_binding_and_ingestion(clients, managed_table):
    connect, _ = clients
    name, expected = managed_table
    with connect() as conn, conn.cursor() as cur:
        assert conn.adbc_get_table_schema(name).equals(catalog_schema(expected.schema), check_metadata=True)
        with conn.adbc_get_objects(depth='all', table_name_filter=name) as reader:
            objects = reader.read_all().to_pylist()
        tables = [t for cat in objects for sch in cat['catalog_db_schemas'] or []
                  for t in sch['db_schema_tables'] or [] if t['table_name'] == name]
        assert len(tables) == 1 and tables[0]['table_type'] == 'TABLE'
        assert [c['column_name'] for c in tables[0]['table_columns']] == expected.column_names
        assert [c['ordinal_position'] for c in tables[0]['table_columns']] == [1, 2, 3, 4]
        cur.execute(f'SELECT * FROM "{name}" ORDER BY id')
        assert cur.fetch_arrow_table().equals(expected, check_metadata=True)
        cur.execute(f'SELECT * FROM "{name}" WHERE FALSE')
        assert cur.fetch_arrow_table().equals(expected.slice(0, 0), check_metadata=True)
        query = 'SELECT CAST($1 AS BIGINT) AS id, CAST($2 AS VARCHAR) AS label'
        cur.adbc_prepare(query)
        cur.execute(query, (9007199254740993, '雪'))
        bound = pa.table({'id': [9007199254740993], 'label': ['雪']})
        assert cur.fetch_arrow_table().equals(bound.cast(catalog_schema(bound.schema)), check_metadata=True)
        assert cur.adbc_ingest(name, expected.slice(2), mode='append') == 1
        cur.execute(f'SELECT * FROM "{name}" ORDER BY id')
        assert cur.fetch_arrow_table().equals(pa.concat_tables([expected, expected.slice(2)]), check_metadata=True)


def test_flight_driver_partitions_are_deferred_and_replayable(clients, managed_table):
    connect, _ = clients
    name, expected = managed_table
    with connect() as owner, owner.cursor() as cur:
        partitions, schema = cur.adbc_execute_partitions(f'SELECT * FROM public."{name}" ORDER BY id')
    assert partitions and schema.equals(catalog_schema(expected.schema), check_metadata=True)
    # These are the Flight SQL driver's ADBC descriptors, not native Redis ones.
    def read():
        with connect() as worker, worker.cursor() as cur:
            tables = []
            for partition in partitions:
                cur.adbc_read_partition(partition)
                tables.append(cur.fetch_arrow_table())
            return pa.concat_tables(tables)
    assert read().equals(expected, check_metadata=True)
    with connect() as publisher, publisher.cursor() as cur:
        assert cur.adbc_ingest(name, expected.slice(2), mode='append') == 1
    # Ordinary SQL tickets execute a new read at DoGet; they do not freeze rows.
    assert read().equals(pa.concat_tables([expected, expected.slice(2)]), check_metadata=True)


def test_native_flight_datasets_and_explicit_unsupported(clients):
    _, rpc = clients
    descriptor = flight.FlightDescriptor.for_path('it_contract', uuid4().hex)
    expected = pa.table({'id': [1, 1, 2], 'nested': [[1, None], [1, None], None]})
    with rpc() as (uploader, options):
        try:
            writer, results = uploader.do_put(descriptor, expected.schema, options)
            with writer:
                for batch in expected.to_batches(max_chunksize=1):
                    writer.write_batch(batch)
                writer.done_writing()
                assert results.read() is not None
            info = uploader.get_flight_info(descriptor, options)
            assert info.schema.equals(expected.schema, check_metadata=True)
            assert info.total_records == expected.num_rows
            with rpc() as (reader, reader_options):
                actual = pa.concat_tables([reader.do_get(e.ticket, reader_options).read_all() for e in info.endpoints])
            assert actual.equals(expected, check_metadata=True)
            with pytest.raises(pa.ArrowNotImplementedError, match='(?i)DoPut|not implemented|unsupported'):
                writer, reader = uploader.do_exchange(descriptor, options)
                writer.close()
                reader.read_all()
        finally:
            list(uploader.do_action(flight.Action('drop_dataset', b'/'.join(descriptor.path)), options))


def test_explicit_unsupported_transactions(clients):
    connect, _ = clients
    with connect() as conn:
        with pytest.raises(NotSupportedError):
            conn.adbc_connection.set_autocommit(False)


def test_query_ticket_expiry_is_terminal(clients):
    """Shorten only this test's newly issued lease, then observe real Redis expiry."""
    if os.environ.get('FLIGHT_SNAPSHOT_FAULT_INJECTION') != '1':
        pytest.skip('ticket expiry injection requires an explicitly disposable backend')
    import time
    from urllib.parse import urlsplit
    import redis
    from adbc_driver_manager import AdbcStatusCode, Error

    uri = os.environ.get('REDIS_URI', '')
    parsed = urlsplit(uri)
    if parsed.scheme != 'rediss' or not parsed.username or not parsed.password:
        pytest.fail('expiry inspection requires authenticated verified Redis TLS')
    admin = redis.Redis.from_url(uri, ssl_ca_certs=os.environ.get('REDIS_CA_FILE'),
                                ssl_cert_reqs='required', ssl_check_hostname=True,
                                socket_timeout=5)
    connect, _ = clients
    try:
        before = set(admin.scan_iter('arrowflight:query:*'))
        with connect() as owner, owner.cursor() as cur:
            partitions, _ = cur.adbc_execute_partitions('SELECT 1')
            added = set(admin.scan_iter('arrowflight:query:*')) - before
            assert len(added) == 1, 'expected one newly issued query control record'
            key = added.pop()
            try:
                assert 0 < admin.pttl(key) <= 600000  # configured ten-minute lease
                assert admin.pexpire(key, 100)
                deadline = time.monotonic() + 5
                while admin.exists(key):
                    assert time.monotonic() < deadline, 'query control lease did not expire'
                    time.sleep(0.02)
                with connect() as worker, worker.cursor() as read:
                    for _ in range(2):
                        with pytest.raises(Error) as failed:
                            read.adbc_read_partition(partitions[0])
                            read.fetch_arrow_table()
                        assert failed.value.status_code == AdbcStatusCode.NOT_FOUND
            finally:
                admin.delete(key)
    finally:
        admin.close()
