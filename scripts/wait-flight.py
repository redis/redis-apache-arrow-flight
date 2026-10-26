"""Wait for an actual SQL round trip; community health UNKNOWN is not readiness."""
import argparse
import time

import adbc_driver_flightsql.dbapi as sql
import adbc_driver_manager


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--uri", default="grpc://localhost:8815")
    parser.add_argument("--timeout", type=float, default=30)
    args = parser.parse_args()
    deadline = time.monotonic() + args.timeout
    while True:
        try:
            with sql.connect(args.uri, autocommit=True, db_kwargs={
                "adbc.flight.sql.rpc.timeout_seconds.connect": "2",
                "adbc.flight.sql.rpc.timeout_seconds.query": "2",
                "adbc.flight.sql.rpc.timeout_seconds.fetch": "2",
            }) as conn, conn.cursor() as cur:
                cur.execute("SELECT 1")
                if cur.fetchall() != [(1,)]:
                    raise RuntimeError("unexpected readiness query result")
            print("Flight SQL is ready")
            return
        except adbc_driver_manager.Error:
            if time.monotonic() >= deadline:
                raise
            time.sleep(0.5)


if __name__ == "__main__":
    main()
