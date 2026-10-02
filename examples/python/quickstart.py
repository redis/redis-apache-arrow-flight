# /// script
# requires-python = ">=3.10"
# dependencies = [
#   "adbc-driver-flightsql>=1.7",
#   "adbc-driver-manager>=1.7",
#   "pyarrow>=18",
#   "pandas>=2",
# ]
# ///
"""A tour of the Redis Arrow Flight server from Python.

1. SQL over Flight SQL, with the ADBC Flight SQL driver: bulk ingest, queries
   with parameters, and catalog metadata. The server runs them on Redis with
   the Redis ADBC driver (one HASH per row, a RediSearch index per table).
2. Datasets over plain Flight, with pyarrow.flight: upload a table with any
   Arrow types, read it back, list and drop it. The server stores it in Redis
   as Arrow IPC.

Run it against a server (see the README):

    uv run examples/python/quickstart.py
    uv run examples/python/quickstart.py --user alice --password secret  # --auth=redis-acl
"""

import argparse
import json

import adbc_driver_flightsql.dbapi as flight_sql
import pyarrow as pa
import pyarrow.flight as flight


def run(cur, sql: str, parameters=None) -> None:
    """Runs a statement that returns no rows, and reads its (empty) result,
    so the driver doesn't cancel the stream."""
    cur.execute(sql, parameters)
    cur.fetch_arrow_table()


def sql_tour(uri: str, user: str | None, password: str | None) -> None:
    db_kwargs = {"username": user, "password": password} if user else {}
    # The Redis driver runs each statement in autocommit mode.
    with flight_sql.connect(uri, db_kwargs=db_kwargs, autocommit=True) as conn, conn.cursor() as cur:
        orders = pa.table(
            {
                "order_id": pa.array(range(1, 1001), pa.int64()),
                "country": pa.array(["JPN", "USA", "DEU", "BRA"] * 250),
                "quantity": pa.array([i % 20 + 1 for i in range(1000)], pa.int32()),
                "price": pa.array([round(5 + (i * 7) % 95 + 0.99, 2) for i in range(1000)]),
            }
        )
        n = cur.adbc_ingest("flight_demo_orders", orders, mode="replace")
        print(f"ingested {n} rows into flight_demo_orders")

        cur.execute(
            "SELECT country, COUNT(*) AS orders, SUM(quantity) AS units"
            " FROM flight_demo_orders GROUP BY country ORDER BY units DESC"
        )
        print(cur.fetch_arrow_table().to_pandas())

        cur.execute(
            "SELECT order_id, country, quantity FROM flight_demo_orders"
            " WHERE quantity >= ? AND country = ? ORDER BY order_id LIMIT 3",
            parameters=(15, "JPN"),
        )
        print("big JPN orders:", cur.fetchall())

        run(cur, "UPDATE flight_demo_orders SET quantity = 0 WHERE order_id = 1")
        cur.execute("SELECT quantity FROM flight_demo_orders WHERE order_id = 1")
        print("order 1 quantity after UPDATE:", cur.fetchone()[0])

        print("table schema:", conn.adbc_get_table_schema("flight_demo_orders"))
        objects = conn.adbc_get_objects(depth="tables", table_name_filter="flight_demo%").read_all()
        tables = [
            t["table_name"]
            for cat in objects.to_pylist()
            for sch in cat["catalog_db_schemas"] or []
            for t in sch["db_schema_tables"] or []
        ]
        print("tables:", tables)

        run(cur, "DROP TABLE flight_demo_orders")


def dataset_tour(uri: str, user: str | None, password: str | None) -> None:
    client = flight.connect(uri)
    options = flight.FlightCallOptions()
    if user:
        token = client.authenticate_basic_token(user, password)
        options = flight.FlightCallOptions(headers=[token])

    features = pa.table(
        {
            "id": pa.array(range(10_000), pa.int64()),
            "embedding": pa.array([[i / 10, i / 100, i / 1000] for i in range(10_000)], pa.list_(pa.float32())),
            "label": pa.array(["cat", "dog"] * 5_000).dictionary_encode(),
        }
    )
    desc = flight.FlightDescriptor.for_path("demo", "features")
    writer, results = client.do_put(desc, features.schema, options)
    writer.write_table(features, max_chunksize=2_500)
    writer.done_writing()
    print("stored:", json.loads(results.read().to_pybytes()))
    writer.close()

    info = client.get_flight_info(desc, options)
    print(f"dataset demo/features: {info.total_records} rows, {info.total_bytes} bytes")
    back = client.do_get(info.endpoints[0].ticket, options).read_all()
    assert back.equals(features), "the dataset came back different"
    print("read back", back.num_rows, "rows; equal to what was stored")

    print("datasets:", ["/".join(p.decode() for p in f.descriptor.path) for f in client.list_flights(b"demo/", options)])
    for r in client.do_action(flight.Action("drop_dataset", b"demo/features"), options):
        print("drop:", json.loads(r.body.to_pybytes()))


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--uri", default="grpc://localhost:8815", help="server URI (grpc+tls:// for TLS)")
    p.add_argument("--user", help="Redis ACL user, when the server runs with --auth=redis-acl")
    p.add_argument("--password")
    args = p.parse_args()
    sql_tour(args.uri, args.user, args.password)
    dataset_tour(args.uri, args.user, args.password)


if __name__ == "__main__":
    main()
