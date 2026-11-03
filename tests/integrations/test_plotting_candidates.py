"""Render and serialize figures using stored Redis rows fetched through ADBC."""

import json

import pytest
import adbc_driver_flightsql.dbapi as dbapi


def read_rows(uri, name):
    with dbapi.connect(uri, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(f'SELECT * FROM "{name}" ORDER BY id')
        return cur.fetch_arrow_table()


def test_plotly_html_and_json(uri, workflow_rows, tmp_path):
    pytest.importorskip("plotly")
    import plotly.graph_objects as go
    import plotly.io as pio

    name, expected = workflow_rows
    table = read_rows(uri, name)
    figure = go.Figure(go.Scatter(
        x=table["id"].to_pylist(), y=table["amount"].to_pylist(),
        customdata=table["label"].to_pylist(), mode="lines+markers",
    ))
    path = tmp_path / "plot.json"
    figure.write_json(path)
    restored = pio.from_json(path.read_text())
    trace = json.loads(restored.to_json())["data"][0]
    assert trace["x"] == expected["id"].to_pylist()
    assert trace["y"] == expected["amount"].to_pylist()
    assert trace["customdata"] == expected["label"].to_pylist()
    html = tmp_path / "plot.html"
    restored.write_html(html, include_plotlyjs=True, full_html=True)
    rendered = html.read_text()
    assert "Plotly.newPlot" in rendered
    assert '<script src="http' not in rendered


def test_matplotlib_agg_png(uri, workflow_rows, tmp_path):
    np = pytest.importorskip("numpy")
    pytest.importorskip("matplotlib")
    from matplotlib.backends.backend_agg import FigureCanvasAgg
    from matplotlib.figure import Figure
    from PIL import Image

    name, expected = workflow_rows
    table = read_rows(uri, name)
    figure = Figure(figsize=(4, 3), dpi=100)
    FigureCanvasAgg(figure)
    axes = figure.subplots()
    line, = axes.plot(table["id"].to_pylist(), table["amount"].to_pylist(), "o-")
    np.testing.assert_array_equal(line.get_xdata(), expected["id"].to_numpy())
    np.testing.assert_allclose(line.get_ydata(orig=False), [2.5, np.nan, 7.5], equal_nan=True)
    path = tmp_path / "plot.png"
    figure.savefig(path)
    with Image.open(path) as image:
        assert image.format == "PNG"
        assert image.size == (400, 300)
        assert len(image.convert("RGB").getcolors(maxcolors=400 * 300)) > 1
