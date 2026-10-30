"""Validate dataframe clients through Flight SQL and DuckDB through Flight RPC.

Requires a disposable server; installs are listed in requirements-clients.txt.
Run: python examples/python/client_compatibility.py --uri grpc://localhost:8815
"""
import argparse
import json
import uuid
from importlib.metadata import version

import adbc_driver_flightsql.dbapi as sql
import duckdb
import pandas as pd
import polars as pl
import pyarrow as pa
import pyarrow.flight as flight


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--uri", default="grpc://localhost:8815")
    args = parser.parse_args()
    name = "it_flight_clients_" + uuid.uuid4().hex
    source = pa.table({"id": [1, 2, 3], "label": ["one", None, "three"],
                       "amount": [1.5, 2.5, 3.5]})
    query = f"SELECT id, label, amount FROM {name} ORDER BY id"
    with sql.connect(args.uri, autocommit=True) as conn, conn.cursor() as cur:
        try:
            cur.adbc_ingest(name, source, mode="create")
            actual = pd.read_sql(query, conn)
            pd.testing.assert_frame_equal(actual, source.to_pandas())
            assert pd.read_sql(f"SELECT id FROM {name} WHERE id < 0", conn).empty
            print(f"PASS pandas {version('pandas')}: read_sql, nulls, empty results")
            try:
                selected = pd.read_sql(f"SELECT id FROM {name} WHERE id >= ? ORDER BY id",
                                       conn, params=(2,))
            except NotImplementedError as error:
                print(f"LIMIT pandas read_sql(params=...) with ADBC: {error}")
                cur.execute(f"SELECT id FROM {name} WHERE id >= ? ORDER BY id", (2,))
                assert cur.fetch_arrow_table().to_pandas().id.tolist() == [2, 3]
            else:
                assert selected.id.tolist() == [2, 3]

            actual = pl.read_database(query, conn)
            assert actual.equals(pl.from_arrow(source))
            assert pl.read_database(f"SELECT id FROM {name} WHERE id < 0", conn).is_empty()
            batches = list(pl.read_database(query, conn, iter_batches=True, batch_size=2))
            assert pl.concat(batches).equals(actual)
            print(f"PASS polars {version('polars')}: read_database, nulls, empty results, batches")

            cur.execute(query)
            with duckdb.connect() as local:
                local.register("flight_sql_result", cur.fetch_arrow_table())
                assert local.execute("SELECT count(*), sum(amount) FROM flight_sql_result").fetchone() == (3, 7.5)
            print(f"PASS duckdb {version('duckdb')}: Flight SQL Arrow result -> local SQL")
        finally:
            cur.execute(f"DROP TABLE IF EXISTS {name}")
            cur.fetch_arrow_table()

    client = flight.connect(args.uri)
    try:
        desc = flight.FlightDescriptor.for_path(name)
        writer, metadata = client.do_put(desc, source.schema)
        with writer:
            writer.write_table(source, max_chunksize=2)
            writer.done_writing()
            assert json.loads(metadata.read().to_pybytes())["rows"] == 3
        info = client.get_flight_info(desc)
        tables = [client.do_get(ep.ticket).read_all() for ep in info.endpoints]
        with duckdb.connect() as local:
            local.register("flight_dataset", pa.concat_tables(tables))
            assert local.sql("SELECT id, label FROM flight_dataset ORDER BY id").fetchall() == [
                (1, "one"), (2, None), (3, "three")]
        print("PASS duckdb: Flight RPC dataset -> Arrow -> local SQL")
    finally:
        try:
            list(client.do_action(flight.Action("drop_dataset", name.encode())))
        finally:
            client.close()


if __name__ == "__main__":
    main()
