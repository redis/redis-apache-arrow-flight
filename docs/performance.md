# Performance

[Back to the README](../README.md).

The October 30, 2026 benchmark compares Flight SQL via ADBC, Flight SQL via JDBC,
native Flight RPC, redis-py, and direct Redis ADBC on the Redis ADBC driver's
sample dataset. All 310 timed trials completed and passed result validation.
These results describe local, warm, single-client runs of the recorded server
revision and environment.

## Full-data read comparison

Each path reads the same 100,000 sales rows and all 10 columns. Times include
execution and complete result consumption. The table reports five timed
repetitions after one warm-up, with connection setup excluded.

| Path | Rows | Median ms | Min–max ms | Rows/s |
|---|---:|---:|---:|---:|
| Direct Redis ADBC | 100,000 | 792.38 | 789.37–809.98 | 126,202 |
| Flight SQL via ADBC | 100,000 | 896.32 | 886.10–918.58 | 111,567 |
| Flight SQL via JDBC | 100,000 | 940.25 | 936.82–961.03 | 106,355 |
| Native Flight RPC (stored dataset) | 100,000 | 37.81 | 28.90–46.56 | 2,644,864 |
| redis-py + Arrow conversion | 100,000 | 3,611.29 | 3,556.56–3,657.23 | 27,691 |

Flight SQL ADBC and JDBC full-scan medians were within 5% of each other.
Direct Redis ADBC read the complete SQL table in 792.38 ms; Flight SQL via ADBC
took 1.13× that time in this run. ADBC returned an Arrow table; JDBC materialized
every cell through `ResultSet.getObject`.
Native Flight retrieved a separately uploaded Arrow dataset through
`GetFlightInfo` and all `DoGet` endpoints, using 10,000-row batches. Its stored
Arrow IPC path avoids SQL execution and Redis HASH decoding. The native Flight
result therefore reflects a different storage and execution path as well as
transport overhead.

The redis-py baseline used RESP2 `SCAN` with a COUNT hint of 1,000 and one
`HMGET` pipeline per page, decoded all ten fields, and built a typed Arrow table. It used the
pure Python RESP parser without hiredis. SCAN traversed the shared database
keyspace with the benchmark prefix filter and de-duplicated keys. Its timings
include client decoding and Arrow construction. Dataset upload and SQL table
loading are excluded from every read timing.

## SQL query comparison

Direct Redis ADBC, Flight SQL ADBC and Flight SQL JDBC ran all 20 queries from
the Redis ADBC driver's
[sample benchmark](../benchmarks/2026-10-30-client-comparison/upstream/bench.py).
The generator uses seed 42, 100,000 sales rows, 10 columns and 50 customers.
SQL preserves the upstream queries with unique fixture names; nine sales
columns are indexed and `notes` uses `NOINDEX`. The labels below are retained
from the upstream benchmark. Native Flight datasets and redis-py do not execute
this SQL suite; their comparison above covers complete data reads.

| Query | Rows | Direct Redis ADBC ms | Flight SQL ADBC ms | Flight SQL JDBC ms | Flight ADBC / direct |
|---|---:|---:|---:|---:|---:|
| point lookup by __rowid | 1 | 6.02 | 10.58 | 15.04 | 1.76× |
| indexed filter + top 10 | 10 | 53.02 | 62.07 | 59.18 | 1.17× |
| indexed range COUNT | 1 | 7.60 | 14.15 | 10.36 | 1.86× |
| LIKE prefix COUNT | 1 | 46.87 | 45.91 | 40.28 | 0.98× |
| interval range COUNT | 1 | 4.69 | 12.52 | 6.35 | 2.67× |
| GROUP BY, index reduce | 6 | 266.81 | 258.05 | 254.07 | 0.97× |
| GROUP BY, driver (AVG double) | 6 | 561.15 | 543.15 | 540.04 | 0.97× |
| GROUP BY expression, driver | 5 | 645.74 | 617.75 | 617.34 | 0.96× |
| GROUP BY DATE_TRUNC, driver | 12 | 682.02 | 595.00 | 631.27 | 0.87× |
| non-indexed filter (scan) | 1 | 131.46 | 96.03 | 92.49 | 0.73× |
| fetch 10% of rows | 10,000 | 75.06 | 83.49 | 93.19 | 1.11× |
| full scan, all columns | 100,000 | 792.38 | 896.32 | 940.25 | 1.13× |
| selective join (index lookup) | 8 | 29.58 | 39.22 | 37.02 | 1.33× |
| unfiltered join (hash join) | 6 | 443.12 | 441.31 | 450.45 | 1.00× |
| correlated subquery (50 rows) | 50 | 41.88 | 46.77 | 48.97 | 1.12× |
| IN (SELECT …), half the rows | 1 | 915.42 | 841.07 | 841.15 | 0.92× |
| correlated EXISTS (semi-join) | 1 | 1,111.00 | 1,083.69 | 1,054.75 | 0.98× |
| correlated EXISTS, no inner rows | 1 | 11.88 | 15.68 | 13.72 | 1.32× |
| UNION of two filters | 1,750 | 98.92 | 92.59 | 88.70 | 0.94× |
| view with pushed filter | 1 | 12.78 | 27.03 | 24.48 | 2.11× |

The ratio is Flight SQL ADBC time divided by direct Redis ADBC time. Above 1
means the Flight SQL path took longer; below 1 means it took less time. The
client suites ran sequentially, so ratios also reflect host load and runtime
warm-up. Small-query differences should be interpreted
with the raw min/max timings and this limitation.

## Recorded environment

| Component | Configuration |
|---|---|
| Flight server | Community build, commit `4c0eba6e73777ebf3fb25f33512207a2460f09cc` |
| Redis ADBC module | v0.0.18, revision `2f053d3affad` |
| Redis | Redis Software 8.6.2, one shard, 1 GiB database, no replication or persistence |
| Query Performance Factor | Active at 4×; verified through the Software REST API before timing |
| Python | 3.14.10, PyArrow 26.0.0, Flight SQL/ADBC manager 1.12.0, redis-py 8.1.0 |
| Direct Redis ADBC | v0.0.18, same `2f053d3affad` source revision, compiled with Go 1.26.10 for Linux arm64 |
| Java | Flight SQL JDBC 19.0.0, Temurin 25.0.4.1 |
| Host | macOS 26.7.2 arm64; Docker Desktop with 18 CPUs and 25,421,033,472 bytes RAM |
| Placement | Python/Java clients and Redis in Linux arm64 containers; Flight server native on macOS |
| Network | Clients reach Flight or Redis through Docker Desktop host forwarding |
| Timing | One client, warmed persistent connections, one warm-up plus five timed runs per case |

The direct-driver baseline was a follow-up run after the initial four paths.
It restored the exact archived source fixture, SQL and index definitions on the
same database, reverified active 4× QPF, and fetched Arrow results through the
ADBC driver manager without a Flight server. The direct shared library uses
Go 1.26.10 on Linux; the Flight server uses Go 1.27.2 on macOS. Runtime, placement
and host-load differences prevent attributing the whole timing difference to
Flight overhead. Redis command statistics are database-wide and can include
unrelated activity on this shared server.

The benchmark used the driver's sample performance suite. It did not run
ClickBench, concurrent clients, cold-cache tests or a remote network workload.
The laptop and containers share resources with other applications. The results
do not establish production capacity or memory usage.

## Validation, raw results and reproduction

DuckDB 2.0.0 independently computed SQL references from the generated fixture.
Every Flight SQL ADBC and direct Redis ADBC warm-up and timed result, all 100
timed JDBC results, and all native Flight and redis-py full scans passed value
and row-count checks. Unordered
results preserve multiplicity; numbers are normalized to nine significant
digits. Top-10 ties accept any valid set of ten unique rows at the correct
threshold. These checks validate result contents rather than ORDER BY output
order. Validation, logging and Redis INFO sampling occur outside the timers.

The [benchmark report and reproduction commands](../benchmarks/2026-10-30-client-comparison/README.md)
include the harness, exact SQL, dependency pins, source hashes and cleanup record.
The [summary JSON](../benchmarks/2026-10-30-client-comparison/summary.json)
contains min/median/max timings for every case. Individual trials are available
in the [Python results](../benchmarks/2026-10-30-client-comparison/python-results.json),
[JDBC results](../benchmarks/2026-10-30-client-comparison/jdbc-results.json),
and [direct Redis ADBC results](../benchmarks/2026-10-30-client-comparison/direct-results.json).
The [environment record](../benchmarks/2026-10-30-client-comparison/environment.json)
identifies the tested binary and effective QPF configuration. Source data and
JDBC result values are retained in a compressed archive with SHA256 hashes.
All benchmark tables, the view, dataset and temporary processes were removed;
existing Redis data was preserved.
