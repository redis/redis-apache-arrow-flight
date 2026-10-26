# /// script
# requires-python = ">=3.11"
# dependencies = ["adbc-driver-flightsql==1.12.0", "adbc-driver-manager==1.12.0", "pyarrow==26.0.0"]
# ///
"""Explicit Flight dataset -> SQL ingestion, with unique fixtures and cleanup.

Run: uv run examples/python/dataset_to_sql.py --uri grpc://localhost:8815
This small example materializes a projection in the client. For large data,
feed endpoint record batches incrementally to ingestion instead of read_all().
"""
import argparse
import json
import uuid

import adbc_driver_flightsql.dbapi as sql
import adbc_driver_manager
import pyarrow as pa
import pyarrow.flight as flight


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--uri", default="grpc://localhost:8815")
    parser.add_argument("--user")
    parser.add_argument("--password")
    args = parser.parse_args()
    suffix = uuid.uuid4().hex
    dataset = "bridge/" + suffix
    target = "flight_bridge_" + suffix
    rejected = target + "_nested"
    client = flight.connect(args.uri)
    options = flight.FlightCallOptions()
    kwargs = {}
    if args.user:
        token = client.authenticate_basic_token(args.user, args.password)
        options = flight.FlightCallOptions(headers=[token])
        kwargs = {"username": args.user, "password": args.password}
    source = pa.table({
        "id": pa.array([1, 2, 3], pa.int64()),
        "label": pa.array(["one", None, "three"], pa.string()),
        "embedding": pa.array([[1.0], [2.0, 3.0], []], pa.list_(pa.float32())),
        # The pinned driver supports one-dimensional lists as SQL arrays, but
        # struct ingestion is still unsupported and must fail explicitly.
        "metadata": pa.array([{"source": "a"}, None, {"source": "c"}],
                             pa.struct([("source", pa.string())])),
    })
    desc = flight.FlightDescriptor.for_path(*dataset.split("/"))
    with sql.connect(args.uri, db_kwargs=kwargs, autocommit=True) as conn, conn.cursor() as cur:
        try:
            writer, result = client.do_put(desc, source.schema, options)
            writer.write_table(source, max_chunksize=2)
            writer.done_writing()
            assert json.loads(result.read().to_pybytes())["rows"] == 3
            writer.close()
            info = client.get_flight_info(desc, options)
            downloaded = pa.concat_tables([client.do_get(ep.ticket, options).read_all() for ep in info.endpoints])
            assert downloaded.equals(source)
            # Uploading is independent of SQL; ingestion of a nested type fails
            # explicitly for the struct rather than silently flattening it.
            try:
                cur.adbc_ingest(rejected, downloaded, mode="create")
            except (adbc_driver_manager.ProgrammingError, adbc_driver_manager.NotSupportedError) as error:
                assert "unsupported Arrow type" in str(error), str(error)
            else:
                raise AssertionError("nested SQL ingestion unexpectedly succeeded")
            projected = downloaded.select(["id", "label"])
            assert cur.adbc_ingest(target, projected, mode="create") == 3
            cur.execute("SELECT id, label FROM " + target + " ORDER BY id")
            assert cur.fetchall() == [(1, "one"), (2, None), (3, "three")]
            print("Flight dataset -> explicit scalar projection -> SQL ingestion/query: passed")
        finally:
            for table in [target, rejected]:
                cur.execute("DROP TABLE IF EXISTS " + table)
                cur.fetch_arrow_table()
            for _ in client.do_action(flight.Action("drop_dataset", dataset.encode()), options):
                pass
    client.close()


if __name__ == "__main__":
    main()
