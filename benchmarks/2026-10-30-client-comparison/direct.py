"""Run the same SQL suite directly through the pinned Redis ADBC shared driver.

Restores the archived source fixture into its original, previously cleaned-up
names, validates every fetched result, and removes only these fixtures.
"""
import argparse
import hashlib
import json
from pathlib import Path
import statistics
import tarfile
import time

import adbc_driver_manager.dbapi
import duckdb
import pyarrow as pa
import redis
from run import ROOT, bench, counts, fingerprint, load_sample, save


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--driver', required=True)
    parser.add_argument('--redis-uri', default='redis://host.docker.internal:6379/0')
    args = parser.parse_args()
    state = json.loads((ROOT / 'fixture.json').read_text())
    names = state['names']
    with tarfile.open(ROOT / 'raw-values.tar.gz') as archive:
        source = archive.extractfile('source.arrow').read()
    manifest = json.loads((ROOT / 'raw-values-manifest.json').read_text())
    assert hashlib.sha256(source).hexdigest() == manifest['source.arrow']
    fixture = pa.ipc.open_file(pa.BufferReader(source)).read_all()
    raw = redis.Redis.from_url(args.redis_uri, protocol=2)
    report = {'client': 'direct_redis_adbc', 'driver_sha256': hashlib.sha256(Path(args.driver).read_bytes()).hexdigest(),
              'redis_adbc_revision': '2f053d3affad', 'rows': fixture.num_rows,
              'method': 'One warm-up and five timed execute plus fetch_arrow_table trials; validation and INFO outside timer',
              'results': [], 'validation': []}
    oracle = duckdb.connect()
    oracle.register('source_sales', fixture)
    oracle.execute('CREATE VIEW sales AS SELECT *, row_number() OVER () AS __rowid FROM source_sales')
    oracle.execute("CREATE TABLE customers AS SELECT i::INTEGER customer_id, 'Customer ' || i name, CASE i%6 WHEN 0 THEN 'USA' WHEN 1 THEN 'CAN' WHEN 2 THEN 'MEX' WHEN 3 THEN 'GBR' WHEN 4 THEN 'DEU' ELSE 'JPN' END country FROM range(1,51) t(i)")
    oracle.execute("CREATE VIEW bench_shipped AS SELECT order_id,country,quantity FROM sales WHERE status='shipped'")
    # Refuse to modify a fixture name if another process has restored it.
    for name in names.values():
        assert raw.get('adbc:{meta}:table:public:' + name) is None, name
        assert raw.get('adbc:{meta}:view:public:' + name) is None, name
    created = []
    with adbc_driver_manager.dbapi.connect(driver=args.driver, db_kwargs={'uri': args.redis_uri}, autocommit=True) as conn, conn.cursor() as cur:
        def execute(sql):
            cur.execute(sql)
            return cur.fetch_arrow_table()
        try:
            execute(f"CREATE TABLE {names['customers']} (customer_id INTEGER NOT NULL, name VARCHAR, country VARCHAR)")
            created.append(('TABLE', names['customers']))
            execute(f"INSERT INTO {names['customers']} VALUES " + ','.join(
                f"({i}, 'Customer {i}', '{load_sample.COUNTRIES[i % 6]}')" for i in range(1, 51)))
            execute(f"CREATE TABLE {names['sales']} (order_id BIGINT, customer_id INTEGER, country VARCHAR, product VARCHAR, quantity INTEGER, unit_price DECIMAL(10,2), discount DOUBLE, ordered_at TIMESTAMP, status VARCHAR, notes VARCHAR NOINDEX)")
            created.append(('TABLE', names['sales']))
            cur.adbc_ingest(names['sales'], fixture, mode='append')
            execute(f"CREATE VIEW {names['bench_shipped']} AS SELECT order_id,country,quantity FROM {names['sales']} WHERE status='shipped'")
            created.append(('VIEW', names['bench_shipped']))
            for i, query in enumerate(state['queries']):
                sql = bench.QUERIES[i][1].format(tenth=state['rows'] // 10)
                sql = sql.replace('SELECT * FROM sales', 'SELECT ' + ','.join(fixture.column_names) + ' FROM sales')
                expected = oracle.sql(sql).to_arrow_table()
                expected_fp = fingerprint(expected)
                times = []
                for repeat in range(-1, state['repeat']):
                    before = counts(raw)
                    start = time.perf_counter()
                    actual = execute(query['sql'])
                    elapsed = time.perf_counter() - start
                    after = counts(raw)
                    assert actual.num_rows == expected.num_rows, query['label']
                    if i == 1:
                        by_id = {r['order_id']: r for r in fixture.to_pylist()}
                        rows = actual.to_pylist()
                        assert len({r['order_id'] for r in rows}) == 10
                        threshold = min(expected.column(2).to_pylist())
                        for row in rows:
                            src = by_id[row['order_id']]
                            assert src['country'] == 'JPN' and src['status'] == 'shipped'
                            assert row['quantity'] >= threshold and row['quantity'] == src['quantity'] and row['product'] == src['product']
                    else:
                        assert fingerprint(actual) == expected_fp, ('oracle mismatch', query['label'])
                    if repeat >= 0:
                        times.append(elapsed)
                        report['results'].append({'client': 'direct_redis_adbc', 'label': query['label'], 'repeat': repeat,
                            'rows': actual.num_rows, 'seconds': elapsed, 'arrow_bytes': actual.nbytes,
                            'commands': {k: after.get(k, 0)-before.get(k, 0) for k in after if after.get(k, 0) != before.get(k, 0)}})
                        report['validation'].append({'label': query['label'], 'repeat': repeat, 'status': 'passed'})
                        save(ROOT / 'direct-results.json', report)
                print('direct_redis_adbc', query['label'], statistics.median(times), flush=True)
        finally:
            for kind, name in reversed(created):
                execute(f'DROP {kind} IF EXISTS {name}')
            remaining = list(raw.scan_iter(match='*' + state['dataset'] + '*'))
            report['cleanup'] = {'remaining_fixture_keys': len(remaining)}
            save(ROOT / 'direct-results.json', report)
            assert not remaining, remaining
            print('direct-driver fixture cleanup verified', flush=True)


if __name__ == '__main__':
    main()
