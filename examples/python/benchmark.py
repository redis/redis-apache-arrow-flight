# /// script
# requires-python = ">=3.11"
# dependencies = ["adbc-driver-flightsql==1.12.0", "adbc-driver-manager==1.12.0", "pyarrow==26.0.0", "redis==8.1.0", "psutil==7.2.2"]
# ///
"""Synthetic local endpoint benchmark. Mutates uniquely named disposable fixtures.

Measures warmed-connection time to first batch, total transfer, Arrow payload
bytes and sampled process RSS. Read the methodology before interpreting results:
client/server share one host; Redis/Docker memory and network emulation are not
included. A redis-py SCAN+HMGET+Arrow conversion baseline compares unordered full
scans only, not SQL operators. RSS sampled every 20ms can miss short peaks.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor
import hashlib
import importlib.metadata
import json
import os
from pathlib import Path
import platform
import subprocess
import threading
import time
import uuid

import adbc_driver_flightsql.dbapi as sql
import adbc_driver_manager
import psutil
import pyarrow as pa
import pyarrow.flight as flight
import redis


class MemorySamples:
    def __init__(self, server_pid):
        self.processes = {"client": psutil.Process()}
        if server_pid:
            self.processes["server"] = psutil.Process(server_pid)
        self.baseline = {}
        self.peak = {}
        self.stop = threading.Event()

    def sample(self):
        for name, process in self.processes.items():
            rss = process.memory_info().rss
            self.baseline.setdefault(name, rss)
            self.peak[name] = max(self.peak.get(name, 0), rss)

    def __enter__(self):
        self.sample()
        def collect():
            while not self.stop.wait(0.02):
                self.sample()
        self.thread = threading.Thread(target=collect, daemon=True)
        self.thread.start()
        return self

    def __exit__(self, *_):
        self.sample()
        self.stop.set()
        self.thread.join()

    def result(self):
        return {key: {"baseline_rss_bytes": value, "sampled_peak_rss_bytes": self.peak[key]}
                for key, value in self.baseline.items()}


def source_state():
    root = Path(__file__).resolve().parents[2]
    files = subprocess.check_output(["git", "ls-files", "--cached", "--others", "--exclude-standard"], cwd=root, text=True).splitlines()
    manifest = {name: hashlib.sha256((root / name).read_bytes()).hexdigest()
                for name in sorted(files) if (root / name).is_file() and not name.startswith("benchmarks/")}
    return {"commit": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=root, text=True).strip(),
            "dirty": bool(subprocess.check_output(["git", "status", "--porcelain"], cwd=root)),
            "source_manifest_sha256": hashlib.sha256(json.dumps(manifest, sort_keys=True).encode()).hexdigest(),
            "files": manifest}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--uri", default="grpc://localhost:18815")
    parser.add_argument("--redis-uri", default="redis://localhost:16379/0")
    parser.add_argument("--rows", type=int, default=10_000)
    parser.add_argument("--width", type=int, default=64)
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--concurrency", type=int, default=16)
    parser.add_argument("--server-pid", type=int)
    parser.add_argument("--output", required=True)
    parser.add_argument("--cases", help="optional comma-separated case names; concurrent scans still run")
    parser.add_argument("--policy-label",choices=["buffer","reject","unspecified"],default="unspecified",
                        help="record the policy used to start the server; this does not change server configuration")
    args = parser.parse_args()
    if min(args.rows, args.width, args.repeats, args.concurrency) < 1:
        parser.error("rows, width, repeats and concurrency must be positive")
    suffix = uuid.uuid4().hex
    table, dimension = "flight_bench_" + suffix, "flight_dim_" + suffix
    dataset = "benchmark/" + suffix
    raw = redis.Redis.from_url(args.redis_uri)
    client = flight.connect(args.uri)
    report = {"methodology": __doc__, "source": source_state(),
              "rows": args.rows, "label_width": args.width, "repeats": args.repeats,
              "concurrency": args.concurrency, "stream_overflow_policy": args.policy_label,
              "machine": {"platform": platform.platform(), "cpu_count": os.cpu_count(),
                          "total_ram_bytes": psutil.virtual_memory().total},
              "clients": {name: importlib.metadata.version(name) for name in
                          ["pyarrow", "adbc-driver-flightsql", "adbc-driver-manager", "redis", "psutil"]},
              "redis_version": raw.info("server")["redis_version"], "results": []}
    if args.server_pid:
        executable=psutil.Process(args.server_pid).exe()
        report["server_binary_sha256"]=hashlib.sha256(Path(executable).read_bytes()).hexdigest()
        report["server_version"]=subprocess.check_output([executable,"-version"],text=True).strip()
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)

    def measure(kind, statement=None):
        count, payload, checksum, first = 0, 0, 0, None
        def consume(batch):
            nonlocal count, payload, checksum, first
            if first is None:
                first = time.perf_counter() - start
            count += batch.num_rows
            payload += batch.nbytes
            if batch.num_columns and batch.schema.field(0).name == "id":
                checksum += sum(batch.column(0).to_pylist())
        # Open and warm each connection before timing execution/transfer.
        if kind == "sql":
            with sql.connect(args.uri, autocommit=True) as conn, conn.cursor() as cur:
                cur.execute("SELECT 1"); cur.fetchall()
                start = time.perf_counter()
                cur.execute(statement)
                reader = cur.fetch_record_batch()
                try:
                    for batch in reader:
                        consume(batch)
                finally:
                    reader.close()
                elapsed = time.perf_counter() - start
        elif kind == "flight":
            connection = flight.connect(args.uri)
            connection.list_actions()  # establish the channel
            start = time.perf_counter()
            info = connection.get_flight_info(flight.FlightDescriptor.for_path(*dataset.split("/")))
            for endpoint in info.endpoints:
                for chunk in connection.do_get(endpoint.ticket):
                    consume(chunk.data)
            elapsed = time.perf_counter() - start
            connection.close()
        else:
            connection = redis.Redis.from_url(args.redis_uri)
            connection.ping()
            metadata = json.loads(connection.get("adbc:{meta}:table:public:" + table))
            prefix = metadata.get("key_prefix") or "public:" + table + ":"
            start = time.perf_counter()
            seen, page = set(), []
            def read_page(keys):
                pipeline = connection.pipeline(transaction=False)
                for key in keys:
                    pipeline.hmget(key, ["id", "g", "value", "label"])
                rows = pipeline.execute()
                batch = pa.record_batch([
                    pa.array([int(row[0]) for row in rows], pa.int64()),
                    pa.array([int(row[1]) for row in rows], pa.int32()),
                    pa.array([float(row[2]) for row in rows], pa.float64()),
                    pa.array([row[3].decode() for row in rows], pa.string()),
                ], names=["id", "g", "value", "label"])
                consume(batch)
            for key in connection.scan_iter(match=prefix + "*", count=1000, _type="hash"):
                if key in seen:
                    continue
                seen.add(key); page.append(key)
                if len(page) == 1000:
                    read_page(page); page = []
            if page:
                read_page(page)
            elapsed = time.perf_counter() - start
            connection.close()
        if kind != "sql" or statement.startswith("SELECT id,g,value,label FROM"):
            assert count == args.rows and checksum == args.rows * (args.rows - 1) // 2, (kind, count, checksum)
        return {"rows": count, "arrow_payload_bytes": payload, "first_batch_seconds": first,
                "total_seconds": elapsed, "rows_per_second": count / elapsed,
                "arrow_mib_per_second": payload / elapsed / (1024 * 1024)}

    with sql.connect(args.uri, autocommit=True) as conn, conn.cursor() as cur:
        try:
            report["endpoint_info"]=conn.adbc_get_info()
            fixture = pa.table({"id": pa.array(range(args.rows), pa.int64()),
                                "g": pa.array([i % 100 for i in range(args.rows)], pa.int32()),
                                "value": pa.array([i / 10 for i in range(args.rows)], pa.float64()),
                                "label": pa.array([str(i).zfill(args.width) for i in range(args.rows)])})
            cur.adbc_ingest(table, fixture, mode="create")
            cur.adbc_ingest(dimension, pa.table({"g": pa.array(range(100), pa.int32())}), mode="create")
            writer, result = client.do_put(flight.FlightDescriptor.for_path(*dataset.split("/")), fixture.schema)
            writer.write_table(fixture, max_chunksize=10_000)
            writer.done_writing(); result.read(); writer.close()
            del fixture
            scan = "SELECT id,g,value,label FROM " + table
            cases = [("flight_full_scan", "flight", None), ("sql_full_scan", "sql", scan),
                     ("resp_scan_hmget_arrow", "redis", None),
                     ("sql_grouped_aggregate", "sql", "SELECT g,COUNT(*) AS n,SUM(value) AS total FROM " + table + " GROUP BY g"),
                     ("sql_join", "sql", "SELECT a.id,a.label FROM " + table + " a JOIN " + dimension + " b ON a.g=b.g"),
                     ("sql_residual_sort", "sql", scan + " ORDER BY value*1.1 DESC"),
                     ("sql_distinct", "sql", "SELECT DISTINCT g FROM " + table),
                     ("sql_window", "sql", "SELECT id,ROW_NUMBER() OVER (ORDER BY id) AS rn FROM " + table)]
            if args.cases:
                selected=set(args.cases.split(","))
                if selected-{case[0] for case in cases}: raise ValueError("unknown benchmark case")
                cases=[case for case in cases if case[0] in selected]
            report["cases"]=[case[0] for case in cases]
            for name, kind, statement in cases:
                for repeat in range(args.repeats):
                    with MemorySamples(args.server_pid) as memory:
                        result = measure(kind, statement)
                    expected = min(args.rows,100) if name in ["sql_grouped_aggregate","sql_distinct"] else args.rows
                    assert result["rows"] == expected, (name,result["rows"],expected)
                    report["results"].append({"case": name, "repeat": repeat, **result, "memory": memory.result()})
                    output.write_text(json.dumps(report, indent=2))
                    print(name, repeat, round(result["total_seconds"], 4), flush=True)
            with MemorySamples(args.server_pid) as memory:
                with ThreadPoolExecutor(max_workers=args.concurrency) as pool:
                    # Barrier makes capacity pressure reproducible instead of
                    # letting early workers finish before later ones connect.
                    gate = threading.Barrier(args.concurrency)
                    def concurrent_scan(_):
                        gate.wait()
                        try:
                            return measure("sql", scan)
                        except (adbc_driver_manager.Error, OSError) as error:
                            if "resourceexhausted" not in str(error).lower().replace("_", "").replace(" ", ""): raise
                            return {"error": "RESOURCE_EXHAUSTED", "message": str(error)}
                    started = time.perf_counter()
                    results = list(pool.map(concurrent_scan, range(args.concurrency)))
                    elapsed = time.perf_counter() - started
            report["concurrent_scans"] = {"seconds": elapsed, "clients": results, "memory": memory.result()}
        finally:
            for name in [table, dimension]:
                cur.execute("DROP TABLE IF EXISTS " + name); cur.fetch_arrow_table()
            for _ in client.do_action(flight.Action("drop_dataset", dataset.encode())):
                pass
            client.close(); raw.close()
            output.write_text(json.dumps(report, indent=2))
    print("saved", output, flush=True)


if __name__ == "__main__":
    main()
