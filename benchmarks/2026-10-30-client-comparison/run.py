"""Adapt the Redis ADBC sample benchmark; run setup, python, or cleanup.

Clients use warmed persistent connections, one warm-up and five full fetches.
Validation and Redis INFO sampling are outside the execution/fetch timer.
"""
import argparse
import datetime as dt
import decimal
import hashlib
import importlib.metadata
import json
from pathlib import Path
import statistics
import sys
import time
import types
import uuid

import adbc_driver_flightsql.dbapi as adbc
import duckdb
import pyarrow as pa
import pyarrow.flight as flight
import redis

ROOT = Path(__file__).resolve().parent
# The vendored generator/query list import common, but this harness supplies
# Flight SQL connections instead of the original direct Redis ADBC connection.
sys.modules['common'] = types.SimpleNamespace(connect=None)
sys.path.insert(0, str(ROOT / 'upstream'))
import bench
import load_sample


def save(path, value):
    path.write_text(json.dumps(value, indent=2, default=str) + '\n')


def counts(raw):
    return {k: v['calls'] for k, v in raw.info('commandstats').items() if k != 'cmdstat_info'}


def canon(value):
    if isinstance(value, (float, decimal.Decimal)):
        return format(float(value), '.9g')
    if isinstance(value, (dt.datetime, dt.date)):
        return value.isoformat().replace('+00:00', '').replace('T', ' ')
    return None if value is None else str(value)


def fingerprint(table):
    rows = [json.dumps([canon(v) for v in row], separators=(',', ':'))
            for row in zip(*(c.to_pylist() for c in table.columns))]
    return hashlib.sha256('\n'.join(sorted(rows)).encode()).hexdigest()


def main():
    p = argparse.ArgumentParser()
    p.add_argument('phase', choices=['setup', 'python', 'cleanup'])
    p.add_argument('--uri', default='grpc://host.docker.internal:18825')
    p.add_argument('--redis-uri', default='redis://host.docker.internal:6379/0')
    p.add_argument('--rows', type=int, default=100000)
    p.add_argument('--repeat', type=int, default=5)
    a = p.parse_args()
    raw = redis.Redis.from_url(a.redis_uri, protocol=2)
    with adbc.connect(a.uri, autocommit=True) as conn, conn.cursor() as cur:
        def execute(sql):
            cur.execute(sql)
            return cur.fetch_arrow_table()
        if a.phase == 'setup':
            token = 'flightperf_' + uuid.uuid4().hex[:12]
            names = {n: token + '_' + n for n in ['sales', 'customers', 'bench_shipped']}
            state = {'names': names, 'dataset': token, 'rows': a.rows, 'repeat': a.repeat,
                     'versions': {n: importlib.metadata.version(n) for n in
                                  ['pyarrow', 'adbc-driver-flightsql', 'adbc-driver-manager', 'redis', 'duckdb']},
                     'redis_version': raw.info('server')['redis_version']}
            save(ROOT / 'fixture.json', state)
            execute(f"CREATE TABLE {names['customers']} (customer_id INTEGER NOT NULL, name VARCHAR, country VARCHAR)")
            execute(f"INSERT INTO {names['customers']} VALUES " + ','.join(
                f"({i}, 'Customer {i}', '{load_sample.COUNTRIES[i % 6]}')" for i in range(1, 51)))
            # Explicit DDL preserves the upstream benchmark's indexed columns;
            # notes is deliberately excluded from the Search index.
            execute(f"CREATE TABLE {names['sales']} (order_id BIGINT, customer_id INTEGER, country VARCHAR, product VARCHAR, quantity INTEGER, unit_price DECIMAL(10,2), discount DOUBLE, ordered_at TIMESTAMP, status VARCHAR, notes VARCHAR NOINDEX)")
            fixture = load_sample.sales_table(a.rows)
            start = time.perf_counter()
            cur.adbc_ingest(names['sales'], fixture, mode='append')
            state['adbc_ingest_seconds'] = time.perf_counter() - start
            execute(f"CREATE VIEW {names['bench_shipped']} AS SELECT order_id,country,quantity FROM {names['sales']} WHERE status='shipped'")
            fc = flight.connect(a.uri)
            writer, ack = fc.do_put(flight.FlightDescriptor.for_path(token), fixture.schema)
            writer.write_table(fixture, max_chunksize=10000)
            writer.done_writing()
            ack.read()
            writer.close()
            fc.close()
            # Persist the exact source for independent result checking.
            with pa.OSFile(str(ROOT / 'source.arrow'), 'wb') as sink:
                with pa.ipc.new_file(sink, fixture.schema) as writer:
                    writer.write_table(fixture)
            queries = []
            import re
            for label, sql in bench.QUERIES:
                sql = sql.format(tenth=a.rows // 10)
                for original, replacement in names.items():
                    sql = re.sub(r'\b' + original + r'\b', replacement, sql)
                queries.append({'label': label, 'sql': sql})
            state['queries'] = queries
            save(ROOT / 'fixture.json', state)
            (ROOT / 'queries.tsv').write_text(''.join(f"{q['label']}\t{q['sql']}\n" for q in queries))
            print('setup complete', names, flush=True)
            return
        state = json.loads((ROOT / 'fixture.json').read_text())
        names = state['names']
        if a.phase == 'cleanup':
            execute('DROP VIEW IF EXISTS ' + names['bench_shipped'])
            for name in ['sales', 'customers']:
                execute('DROP TABLE IF EXISTS ' + names[name])
            fc = flight.connect(a.uri)
            list(fc.do_action(flight.Action('drop_dataset', state['dataset'].encode())))
            fc.close()
            print('removed only benchmark fixtures', flush=True)
            return
        fixture = pa.ipc.open_file(ROOT / 'source.arrow').read_all()
        oracle = duckdb.connect()
        oracle.register('source_sales', fixture)
        oracle.execute('CREATE VIEW sales AS SELECT *, row_number() OVER () AS __rowid FROM source_sales')
        oracle.execute("CREATE TABLE customers AS SELECT i::INTEGER customer_id, 'Customer ' || i name, CASE i%6 WHEN 0 THEN 'USA' WHEN 1 THEN 'CAN' WHEN 2 THEN 'MEX' WHEN 3 THEN 'GBR' WHEN 4 THEN 'DEU' ELSE 'JPN' END country FROM range(1,51) t(i)")
        oracle.execute("CREATE VIEW bench_shipped AS SELECT order_id,country,quantity FROM sales WHERE status='shipped'")
        report = {'fixture': state, 'results': [], 'validation': []}
        def timed(label, kind, function, validator):
            for repeat in range(-1, state['repeat']):
                before = counts(raw)
                start = time.perf_counter()
                table = function()
                elapsed = time.perf_counter() - start
                after = counts(raw)
                validator(table)
                if repeat >= 0:
                    report['results'].append({'label': label, 'client': kind, 'repeat': repeat,
                        'seconds': elapsed, 'rows': table.num_rows, 'arrow_bytes': table.nbytes,
                        'commands': {k: after.get(k, 0)-before.get(k, 0) for k in after if after.get(k, 0) != before.get(k, 0)}})
                    save(ROOT / 'python-results.json', report)
            print(kind, label, statistics.median(r['seconds'] for r in report['results'] if r['label'] == label and r['client'] == kind), flush=True)
        for i, query in enumerate(state['queries']):
            sql = bench.QUERIES[i][1].format(tenth=state['rows'] // 10)
            # Original query's * excludes the synthetic hidden row identifier.
            sql = sql.replace('SELECT * FROM sales', 'SELECT ' + ','.join(fixture.column_names) + ' FROM sales')
            expected = oracle.execute(sql).to_arrow_table()
            expected_fp = fingerprint(expected)
            def validate(table):
                assert table.num_rows == expected.num_rows, (query['label'], table.num_rows, expected.num_rows)
                if i == 1:  # LIMIT cuts an unordered quantity tie; any valid 10 qualify.
                    records = table.to_pylist()
                    by_id = {r['order_id']: r for r in fixture.to_pylist()}
                    threshold = min(expected.column(2).to_pylist())
                    assert len({r['order_id'] for r in records}) == 10
                    for r in records:
                        src = by_id[r['order_id']]
                        assert src['country'] == 'JPN' and src['status'] == 'shipped'
                        assert r['quantity'] >= threshold and r['quantity'] == src['quantity'] and r['product'] == src['product']
                else:
                    assert fingerprint(table) == expected_fp, ('oracle mismatch', query['label'], table.to_pylist()[:3], expected.to_pylist()[:3])
            try:
                timed(query['label'], 'adbc', lambda: execute(query['sql']), validate)
                report['validation'].append({'label': query['label'], 'status': 'passed', 'oracle': 'DuckDB source fixture', 'rows': expected.num_rows})
            except Exception as e:
                report['validation'].append({'label': query['label'], 'status': 'failed', 'error': str(e)})
                print('FAILED', query['label'], str(e), flush=True)
            save(ROOT / 'python-results.json', report)
        fc = flight.connect(a.uri)
        list(fc.list_actions())
        def native():
            info = fc.get_flight_info(flight.FlightDescriptor.for_path(state['dataset']))
            return pa.concat_tables([fc.do_get(e.ticket).read_all() for e in info.endpoints])
        source_fp = fingerprint(fixture)
        def validate_scan(table):
            assert table.num_rows == fixture.num_rows and fingerprint(table) == source_fp
        timed('full scan, all columns', 'native_flight_dataset', native, validate_scan)
        metadata = json.loads(raw.get('adbc:{meta}:table:public:' + names['sales']))
        prefix = metadata.get('key_prefix') or 'public:' + names['sales'] + ':'
        print('metadata', metadata, 'sample', raw.hgetall(next(raw.scan_iter(match=prefix+'*', _type='hash'))), flush=True)
        def resp():
            rows, seen = [], set()
            cursor = 0
            while True:
                cursor, keys = raw.scan(cursor, match=prefix+'*', count=1000, _type='hash')
                keys = [k for k in keys if k not in seen]
                seen.update(keys)
                pipe = raw.pipeline(transaction=False)
                for key in keys:
                    pipe.hmget(key, fixture.column_names)
                rows.extend(pipe.execute())
                if cursor == 0:
                    break
            columns = []
            for index, field in enumerate(fixture.schema):
                values = []
                for row in rows:
                    v = row[index]
                    if v is not None:
                        v = v.decode()
                        if pa.types.is_integer(field.type) or pa.types.is_timestamp(field.type): v = int(v)
                        elif pa.types.is_floating(field.type): v = float(v)
                        elif pa.types.is_decimal(field.type): v = decimal.Decimal(v)
                    values.append(v)
                columns.append(pa.array(values, type=field.type))
            return pa.Table.from_arrays(columns, schema=fixture.schema)
        timed('full scan, all columns', 'redis_py_scan_hmget_arrow', resp, validate_scan)
        fc.close()
        save(ROOT / 'python-results.json', report)


if __name__ == '__main__':
    main()
