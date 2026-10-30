"""Produce summary JSON and Markdown tables from recorded trials."""
import json
import statistics
from run import ROOT, save

python = json.loads((ROOT / 'python-results.json').read_text())
jdbc = json.loads((ROOT / 'jdbc-results.json').read_text())
validation = json.loads((ROOT / 'jdbc-validation.json').read_text())
direct = json.loads((ROOT / 'direct-results.json').read_text())
assert len(python['results']) == 110
assert len(python['validation']) == 20 and all(v['status'] == 'passed' for v in python['validation'])
assert len(jdbc) == 100 and all('error' not in v for v in jdbc)
assert len(validation) == 100 and all(v['status'] == 'passed' for v in validation)
assert len(direct['results']) == 100
assert len(direct['validation']) == 100 and all(v['status'] == 'passed' for v in direct['validation'])
assert direct['cleanup']['remaining_fixture_keys'] == 0
groups = {}
for row in python['results'] + [dict(r, client='jdbc') for r in jdbc] + direct['results']:
    groups.setdefault((row['client'], row['label']), []).append(row)
summary = []
for (client, label), rows in groups.items():
    assert len(rows) == 5
    times = [r['seconds'] for r in rows]
    median = statistics.median(times)
    summary.append({'client': client, 'label': label, 'rows': rows[0]['rows'],
                    'min_ms': min(times) * 1000, 'median_ms': median * 1000,
                    'max_ms': max(times) * 1000, 'rows_per_second': rows[0]['rows'] / median})
save(ROOT / 'summary.json', summary)
by_key = {(r['client'], r['label']): r for r in summary}
lines = ['| Path | Rows | Median ms | Min–max ms | Rows/s |', '|---|---:|---:|---:|---:|']
for client in ['direct_redis_adbc', 'adbc', 'jdbc', 'native_flight_dataset', 'redis_py_scan_hmget_arrow']:
    r = by_key[client, 'full scan, all columns']
    lines.append(f"| {client} | {r['rows']:,} | {r['median_ms']:,.2f} | {r['min_ms']:,.2f}–{r['max_ms']:,.2f} | {r['rows_per_second']:,.0f} |")
lines += ['', '| Query | Rows | Direct Redis ADBC ms | Flight SQL ADBC ms | Flight SQL JDBC ms | Flight ADBC / direct |', '|---|---:|---:|---:|---:|---:|']
for q in python['fixture']['queries']:
    d, a, j = [by_key[c, q['label']] for c in ['direct_redis_adbc', 'adbc', 'jdbc']]
    lines.append(f"| {q['label']} | {a['rows']:,} | {d['median_ms']:,.2f} | {a['median_ms']:,.2f} | {j['median_ms']:,.2f} | {a['median_ms']/d['median_ms']:.2f}× |")
(ROOT / 'tables.md').write_text('\n'.join(lines) + '\n')
print('\n'.join(lines))
