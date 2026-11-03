"""Execute notebook cells in a separate kernel using the current interpreter."""

import json
import os
from pathlib import Path
import sys
import textwrap

import pytest


def test_notebook_queries_redis_in_separate_kernel(
    uri, workflow_rows, tmp_path, monkeypatch
):
    nbformat = pytest.importorskip("nbformat")
    pytest.importorskip("ipykernel")
    pytest.importorskip("nbclient")
    from nbclient import NotebookClient

    name, expected = workflow_rows
    kernel_dir = tmp_path / "jupyter" / "kernels" / "redis-adbc-test"
    kernel_dir.mkdir(parents=True)
    (kernel_dir / "kernel.json").write_text(
        json.dumps(
            {
                "argv": [sys.executable, "-m", "ipykernel_launcher", "-f", "{connection_file}"],
                "display_name": "Redis ADBC test",
                "language": "python",
            }
        )
    )
    monkeypatch.setenv("JUPYTER_PATH", str(tmp_path / "jupyter"))
    monkeypatch.setenv("JUPYTER_RUNTIME_DIR", str(tmp_path / "runtime"))
    monkeypatch.setenv("JUPYTER_CONFIG_DIR", str(tmp_path / "config"))
    monkeypatch.setenv("IPYTHONDIR", str(tmp_path / "ipython"))
    python_source = str(Path(__file__).resolve().parents[1])
    env = {
        **os.environ,
        "FLIGHT_URI": uri,
        "REDIS_ADBC_TEST_TABLE": name,
        "PYTHONPATH": python_source + os.pathsep + os.environ.get("PYTHONPATH", ""),
    }
    notebook = nbformat.v4.new_notebook(
        cells=[
            nbformat.v4.new_code_cell(textwrap.dedent("""\
                import os
                import adbc_driver_flightsql.dbapi as dbapi

                with dbapi.connect(os.environ["FLIGHT_URI"], autocommit=True) as conn, conn.cursor() as cur:
                    name = os.environ["REDIS_ADBC_TEST_TABLE"]
                    cur.execute(f'SELECT * FROM "{name}" ORDER BY id')
                    data = cur.fetch_arrow_table()
                """)),
            nbformat.v4.new_code_cell(textwrap.dedent("""\
                import json
                import pyarrow as pa

                assert data.schema == pa.schema([
                    ("id", pa.int64()), ("label", pa.string()),
                    ("amount", pa.float64()), ("active", pa.bool_()),
                ])
                print(json.dumps(data.to_pylist(), ensure_ascii=False))
                """)),
        ]
    )
    executed = NotebookClient(
        notebook, kernel_name="redis-adbc-test", timeout=60
    ).execute(cwd=str(tmp_path), env=env)
    path = tmp_path / "executed.ipynb"
    nbformat.write(executed, path)
    saved = nbformat.read(path, as_version=4)

    assert [cell.execution_count for cell in saved.cells] == [1, 2]
    output = "".join(o.get("text", "") for o in saved.cells[-1].outputs)
    assert json.loads(output) == expected.to_pylist()
