"""Real Dagster assets and a SQLAlchemy Flight SQL dialect, using stored rows."""

def test_dagster_resource(uri, workflow_rows, tmp_path):
    from dagster import asset, materialize
    from dagster_adbc import ADBCResource
    import adbc_driver_flightsql
    name, expected = workflow_rows
    resource = ADBCResource(driver=adbc_driver_flightsql._driver_path(), uri=uri, autocommit=True)

    @asset
    def flight_rows(flight: ADBCResource) -> int:
        with flight.get_connection() as conn, conn.cursor() as cur:
            cur.execute(f'SELECT * FROM "{name}" ORDER BY id')
            assert cur.fetch_arrow_table().equals(expected)
            cur.execute(f'SELECT id FROM "{name}" WHERE id >= ?', (2,))
            assert sorted(cur.fetchall()) == [(2,), (3,)]
        return 3

    result = materialize([flight_rows], resources={"flight": resource})
    assert result.success
    assert result.output_for_node("flight_rows") == 3


def test_sqlalchemy_flightsql(uri, workflow_rows):
    import sqlalchemy as sa
    engine = sa.create_engine("adbc+flightsql://" + uri.removeprefix("grpc://") + "?tls=false",
                              connect_args={"autocommit": True})
    try:
        with engine.connect() as conn:
            rows = conn.exec_driver_sql(f'SELECT * FROM "{workflow_rows[0]}" ORDER BY id').mappings().all()
            assert [dict(row) for row in rows] == workflow_rows[1].to_pylist()
            assert conn.exec_driver_sql(f'SELECT id FROM "{workflow_rows[0]}" WHERE id < 0').all() == []
    finally:
        engine.dispose()
