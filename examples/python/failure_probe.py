# /// script
# requires-python = ">=3.11"
# dependencies = ["adbc-driver-flightsql==1.12.0", "adbc-driver-manager==1.12.0", "pyarrow==26.0.0"]
# ///
"""Pause a disposable Redis container, verify timeout/recovery and old-reader compatibility.

The caller must explicitly name a container it owns. Never use a shared or
production backend. All fixtures have unique names and are removed on exit.
The rollback endpoint is a separately started pre-change server on the same DB.
"""
import argparse
import json
from pathlib import Path
import subprocess
import time
import uuid

import adbc_driver_flightsql.dbapi as sql
import pyarrow as pa
import pyarrow.flight as flight


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--uri", default="grpc://localhost:18815")
    parser.add_argument("--redis-container", required=True)
    parser.add_argument("--rollback-uri", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    suffix = uuid.uuid4().hex
    table, dataset = "flight_failure_" + suffix, "failure/" + suffix
    desc = flight.FlightDescriptor.for_path(*dataset.split("/"))
    fixture = pa.table({"id": pa.array([1, 2, 3], pa.int64())})
    client = flight.connect(args.uri)
    report = {"fixture_rows": 3, "redis_container": args.redis_container, "checks": []}
    paused = False
    with sql.connect(args.uri, autocommit=True) as conn, conn.cursor() as cur:
        try:
            cur.adbc_ingest(table, fixture, mode="create")
            writer, result = client.do_put(desc, fixture.schema)
            writer.write_table(fixture); writer.done_writing(); result.read(); writer.close()
            subprocess.run(["docker", "pause", args.redis_container], check=True, stdout=subprocess.DEVNULL)
            paused = True
            started = time.perf_counter()
            try:
                client.get_flight_info(desc, flight.FlightCallOptions(timeout=1))
            except flight.FlightTimedOutError as error:
                report["checks"].append({"check": "paused_redis_deadline", "passed": True,
                                         "seconds": time.perf_counter() - started, "error_type": type(error).__name__})
            else:
                raise AssertionError("a paused backend did not produce the expected deadline error")
            finally:
                subprocess.run(["docker", "unpause", args.redis_container], check=True, stdout=subprocess.DEVNULL)
                paused = False
            info = client.get_flight_info(desc, flight.FlightCallOptions(timeout=5))
            recovered = pa.concat_tables([client.do_get(endpoint.ticket).read_all() for endpoint in info.endpoints])
            assert recovered.equals(fixture)
            cur.execute("SELECT COUNT(*) FROM " + table)
            assert cur.fetchone()[0] == 3
            report["checks"].append({"check": "dataset_and_sql_recover_after_redis_unpause", "passed": True})
            rollback = flight.connect(args.rollback_uri)
            info = rollback.get_flight_info(desc)
            data = pa.concat_tables([rollback.do_get(endpoint.ticket).read_all() for endpoint in info.endpoints])
            assert data.equals(fixture)
            rollback.close()
            with sql.connect(args.rollback_uri, autocommit=True) as older, older.cursor() as cursor:
                version=older.adbc_get_info()["vendor_version"]
                assert version=="rollback-4120689",version
                cursor.execute("SELECT COUNT(*) FROM " + table)
                assert cursor.fetchone()[0] == 3
            report["checks"].append({"check": "pre_change_server_reads_new_dataset_and_sql_table", "passed": True,
                                     "baseline_server_revision": "4120689", "reported_server_version":version})
            report["limitations"] = ["Local disposable backend only; not production failover evidence.",
                "No storage format changed in this work. Discard outstanding query tickets and cookie sessions during rollback.",
                "Older servers do not enforce new cancellation records; do not mix them with cancellable-ticket readers."]
        finally:
            if paused:
                subprocess.run(["docker", "unpause", args.redis_container], check=True, stdout=subprocess.DEVNULL)
            cur.execute("DROP TABLE IF EXISTS " + table); cur.fetch_arrow_table()
            for _ in client.do_action(flight.Action("drop_dataset", dataset.encode())):
                pass
            client.close()
            output = Path(args.output); output.parent.mkdir(parents=True, exist_ok=True)
            output.write_text(json.dumps(report, indent=2))
    print("failure/recovery and rollback data checks passed")


if __name__ == "__main__":
    main()
