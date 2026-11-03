"""Run an ADBC-backed Streamlit app through AppTest, including an input rerun."""

import textwrap

import pandas as pd
import pytest


def test_dataframe_and_filtered_rerun(workflow_rows, tmp_path, monkeypatch):
    pytest.importorskip("streamlit")
    from streamlit.testing.v1 import AppTest

    name, expected = workflow_rows
    monkeypatch.setenv("REDIS_ADBC_TEST_TABLE", name)
    app = tmp_path / "app.py"
    app.write_text(textwrap.dedent("""\
        import os
        import streamlit as st
        import adbc_driver_flightsql.dbapi as dbapi

        minimum = st.number_input("Minimum id", min_value=1, max_value=3, value=1)
        with dbapi.connect(os.environ["FLIGHT_URI"], autocommit=True) as conn, conn.cursor() as cur:
            name = os.environ["REDIS_ADBC_TEST_TABLE"]
            cur.execute(f'SELECT * FROM "{name}" WHERE id >= ? ORDER BY id', (minimum,))
            data = cur.fetch_arrow_table()
        st.dataframe(data)
        st.metric("Rows", data.num_rows)
        """))

    result = AppTest.from_file(str(app), default_timeout=30).run()
    assert not result.exception
    pd.testing.assert_frame_equal(result.dataframe[0].value, expected.to_pandas())
    assert result.metric[0].value == "3"

    result.number_input[0].set_value(2).run()
    assert not result.exception
    pd.testing.assert_frame_equal(result.dataframe[0].value, expected.slice(1).to_pandas())
    assert result.metric[0].value == "2"
