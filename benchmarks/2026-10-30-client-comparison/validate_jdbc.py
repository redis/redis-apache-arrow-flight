"""Validate every JDBC trial against the source fixture using DuckDB."""
import datetime
import json
import pyarrow as pa
import duckdb
from run import ROOT, bench, canon, save

state = json.loads((ROOT / 'fixture.json').read_text())
fixture = pa.ipc.open_file(ROOT / 'source.arrow').read_all()
db = duckdb.connect()
db.register('source_sales', fixture)
db.execute('CREATE VIEW sales AS SELECT *, row_number() OVER () AS __rowid FROM source_sales')
db.execute("CREATE TABLE customers AS SELECT i::INTEGER customer_id, 'Customer ' || i name, CASE i%6 WHEN 0 THEN 'USA' WHEN 1 THEN 'CAN' WHEN 2 THEN 'MEX' WHEN 3 THEN 'GBR' WHEN 4 THEN 'DEU' ELSE 'JPN' END country FROM range(1,51) t(i)")
db.execute("CREATE VIEW bench_shipped AS SELECT order_id,country,quantity FROM sales WHERE status='shipped'")
validation = []
for i, (label, sql) in enumerate(bench.QUERIES):
    sql = sql.format(tenth=state['rows'] // 10)
    sql = sql.replace('SELECT * FROM sales', 'SELECT ' + ','.join(fixture.column_names) + ' FROM sales')
    expected = db.sql(sql).to_arrow_table()
    def normalized(rows):
        result = []
        for row in rows:
            values = []
            for value, field in zip(row, expected.schema):
                if value is None:
                    values.append(None)
                elif pa.types.is_integer(field.type) or pa.types.is_floating(field.type) or pa.types.is_decimal(field.type):
                    values.append(format(float(value), '.9g'))
                elif pa.types.is_timestamp(field.type) or pa.types.is_date(field.type):
                    value = datetime.datetime.fromisoformat(str(value))
                    values.append(value.replace(tzinfo=None).isoformat())
                else:
                    values.append(str(value))
            result.append(json.dumps(values))
        return sorted(result)
    wanted = normalized(zip(*(c.to_pylist() for c in expected.columns)))
    for repeat in range(state['repeat']):
        try:
            actual = json.loads((ROOT / f'jdbc-q{i}-r{repeat}.json').read_text())
            assert len(actual) == expected.num_rows, 'row count mismatch'
            if i == 1:
                source = {r['order_id']: r for r in fixture.to_pylist()}
                threshold = min(expected.column(2).to_pylist())
                assert len({int(r[0]) for r in actual}) == 10
                for row in actual:
                    r = source[int(row[0])]
                    assert r['country'] == 'JPN' and r['status'] == 'shipped'
                    assert int(row[2]) >= threshold and int(row[2]) == r['quantity'] and row[1] == r['product']
            else:
                assert normalized(actual) == wanted, 'value mismatch'
            result = {'label': label, 'repeat': repeat, 'status': 'passed', 'rows': len(actual)}
        except Exception as e:
            result = {'label': label, 'repeat': repeat, 'status': 'failed', 'error': str(e)}
        validation.append(result)
save(ROOT / 'jdbc-validation.json', validation)
print(f"JDBC oracle checks: {sum(v['status'] == 'passed' for v in validation)}/{len(validation)} passed")
for v in validation:
    if v['status'] != 'passed': print(v)
