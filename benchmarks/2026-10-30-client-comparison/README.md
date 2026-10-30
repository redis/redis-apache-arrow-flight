# Flight client performance comparison — October 30, 2026

Executed the Redis ADBC driver's sample benchmark: seed 42, 100,000 sales rows,
10 columns, 50 customers, all 20 SQL queries, one warm-up and five timed runs per
case. Redis Software 8.6.2, one shard, verified active 4× Query Performance Factor.
All 310 timed trials completed; all fetched results passed validation.

## Full-data read

Times include execution and complete client fetch. Direct Redis ADBC and Flight
SQL ADBC produce Arrow tables;
JDBC materializes every cell with `getObject`; redis-py performs RESP2 SCAN with
a COUNT hint of 1,000 plus per-page HMGET pipelines and constructs a typed Arrow table. Native Flight reads
a separately uploaded, equivalent Arrow dataset (10,000-row batches), including
GetFlightInfo and all DoGet endpoints.

Native Flight's stored IPC path avoids SQL planning and Redis HASH decoding, so
its speedup measures the different storage/execution path as well as transport.
Native Flight datasets and redis-py do not execute the SQL query suite.
The Redis baseline is the full scan, not an FT.SEARCH/FT.AGGREGATE implementation
of each query. Dataset upload and table ingestion are excluded from read timing.

| Path | Rows | Median ms | Min–max ms | Rows/s |
|---|---:|---:|---:|---:|
| direct_redis_adbc | 100,000 | 792.38 | 789.37–809.98 | 126,202 |
| adbc | 100,000 | 896.32 | 886.10–918.58 | 111,567 |
| jdbc | 100,000 | 940.25 | 936.82–961.03 | 106,355 |
| native_flight_dataset | 100,000 | 37.81 | 28.90–46.56 | 2,644,864 |
| redis_py_scan_hmget_arrow | 100,000 | 3,611.29 | 3,556.56–3,657.23 | 27,691 |

## SQL suite

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



## Method and limits

The original generator and query list are copied unchanged under `upstream/`,
including their license. Source checkout and SHA256 hashes are in
[environment.json](environment.json). The Flight server was built from clean
commit `4c0eba6e73777ebf3fb25f33512207a2460f09cc`; its pinned Redis ADBC module is
`2f053d3affad` (v0.0.18). Explicit DDL preserves the original nine indexed columns
and NOINDEX notes; unique fixture names replace sales/customers/bench_shipped.
The hidden row ID is one-based. All SQL otherwise comes from the driver suite.

Python 3.14.10, PyArrow 26.0.0, ADBC manager/Flight SQL 1.12.0, redis-py 8.1.0;
Flight SQL JDBC 19.0.0 with Temurin 25.0.4.1. Python and Java run sequentially in
Linux arm64 containers. Flight runs natively on macOS 26.7.2 arm64. Both Flight
and direct Redis paths cross Docker Desktop networking. Docker has 18 CPUs and
25,421,033,472 bytes RAM; the Redis database limit is 1 GiB with no replication or
persistence. These are local, warm, single-client measurements on a shared host;
there is no cold-cache, WAN, concurrency, production throughput or memory claim.
Client suites run sequentially rather than interleaved, so time-dependent host
load and JVM warm-up can affect ratios, especially short queries. Redis-py's
pure Python RESP parser is used; hiredis is not installed. SCAN traverses the
existing shared keyspace and filters to fixture keys, then de-duplicates keys.
Connection setup, result validation, logging and Redis INFO sampling are outside
the timers. JDBC includes per-cell Java object materialization while ADBC uses
Arrow buffers, so this is an application-facing comparison, not pure wire cost.

## Correctness and evidence

DuckDB 2.0.0 computes all SQL references independently from the generated source
fixture. Every Flight SQL ADBC and direct Redis ADBC warm-up/timed result, and all 100
timed JDBC results, are checked
for row counts and values. Numeric comparison uses nine significant digits;
unordered results preserve multiplicity. The top-10 tie case accepts any ten
unique qualifying rows at the correct threshold and verifies projected values.
The validator compares contents, not ORDER BY output order. Each native Flight
and redis-py scan is compared with the source fixture using the same normalized
content fingerprint. Neither results validation nor source generation is timed.

- [All Python trial timings and command counts](python-results.json)
- [All JDBC trial timings](jdbc-results.json)
- [All direct Redis ADBC trial timings, validation and cleanup](direct-results.json)
- [JDBC per-trial validation](jdbc-validation.json)
- [Summary with min/median/max](summary.json)
- [Exact fixture names and SQL](fixture.json)
- [Versions, build identity and QPF readback](environment.json)
- [Compressed source fixture and every JDBC trial's values](raw-values.tar.gz)
- [Raw value SHA256 manifest](raw-values-manifest.json)
- [Verified cleanup](cleanup.json)

The direct-driver baseline was measured in a later sequential follow-up using
Redis ADBC v0.0.18 at the exact same source revision (`2f053d3affad`). It restored
the archived source fixture into the same unique SQL names, with the same DDL,
queries, warm-up and repeat counts, and verified QPF again before timing. The
Linux arm64 shared driver was built with Go 1.26.10; Flight used Go 1.27.2 on
macOS. It uses the same Python version and Arrow dependencies but bypasses the
Flight server. These runtime, network and host-load differences mean the ratios
do not isolate pure Flight overhead. Database-wide Redis command counts may
include unrelated activity on this shared database. Direct-driver results contain
all 100 successful timed checks and verified fixture cleanup.

Both SQL tables, the view, the Flight dataset and its retained sequence counter
were removed. Existing Redis data and containers were preserved. No ClickBench
run is claimed: this uses the driver's sample performance suite, which supports
both Flight SQL clients and a full-scan comparison with the other two paths.

## Reproduce

Use a Redis Software database with active 4× QPF, and start a server built from
this repository against it. Install `requirements.txt` into Python 3.14 and obtain
the verified Flight SQL JDBC 19.0.0 jar. From the repository root on a host with
Python and Java 25 installed (adjust the two addresses for containers):

```sh
python -m pip install -r benchmarks/2026-10-30-client-comparison/requirements.txt
python benchmarks/2026-10-30-client-comparison/run.py setup --uri grpc://localhost:18825 --redis-uri redis://localhost:6379/0
python benchmarks/2026-10-30-client-comparison/run.py python --uri grpc://localhost:18825 --redis-uri redis://localhost:6379/0
java --add-opens=java.base/java.nio=ALL-UNNAMED --sun-misc-unsafe-memory-access=allow -cp /path/to/flight-sql-jdbc-driver-19.0.0.jar benchmarks/2026-10-30-client-comparison/JdbcBenchmark.java 'jdbc:arrow-flight-sql://localhost:18825/?useEncryption=false' benchmarks/2026-10-30-client-comparison 5
python benchmarks/2026-10-30-client-comparison/validate_jdbc.py
python benchmarks/2026-10-30-client-comparison/summarize.py
python benchmarks/2026-10-30-client-comparison/run.py cleanup --uri grpc://localhost:18825 --redis-uri redis://localhost:6379/0
```

Setup creates new unique names and writes fixture state; complete cleanup before
another setup. The scripts overwrite their result files, so copy this directory
to preserve a previous run. The recorded raw source/JDBC files were compressed
after validation; to revalidate the saved run, first extract `raw-values.tar.gz`
inside this directory. The dataset deletion API intentionally retains a sequence
counter; `cleanup.json` records the exact benchmark-owned counter removed after
this run, once no readers remained.


## Direct Redis ADBC reproduction

Build the `2f053d3affad` Redis ADBC source revision's `go/pkg` as a shared library
on Linux arm64 with Go 1.26.10, matching the recorded baseline:

```sh
# From the Redis ADBC driver's go directory at revision 2f053d3affad:
CGO_ENABLED=1 go build -buildvcs=false -buildmode=c-shared -tags driverlib -ldflags '-s -w -X github.com/adbc-drivers/driverbase-go/driverbase.infoDriverVersion=v0.0.18' -o /tmp/libadbc_driver_redis.so ./pkg
```

After the original benchmark fixtures have been cleaned up, run this from the
Flight repository root in the Python environment with `requirements.txt`
installed. The script reads the source fixture directly from the retained
archive, verifies its hash, restores only the unique fixture names, runs and
validates all queries, then removes its tables/view in a finally block.

```sh
python benchmarks/2026-10-30-client-comparison/direct.py --driver /tmp/libadbc_driver_redis.so --redis-uri redis://localhost:6379/0
python benchmarks/2026-10-30-client-comparison/summarize.py
```
