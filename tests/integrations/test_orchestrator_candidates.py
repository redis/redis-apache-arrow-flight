"""Execute local components/tasks/SDK syncs with task-local Flight SQL reads."""

import os
from pathlib import Path
import subprocess
import sys
import textwrap

import pytest


def test_kubeflow_local_component(uri, workflow_rows, tmp_path, monkeypatch):
    pytest.importorskip("kfp")
    from kfp import dsl, local

    monkeypatch.setenv("PATH", str(Path(sys.executable).parent) + os.pathsep + os.environ["PATH"])
    name, _ = workflow_rows
    local.init(runner=local.SubprocessRunner(use_venv=False), pipeline_root=str(tmp_path / "outputs"))

    @dsl.component(base_image="python:3.12", install_kfp_package=False)
    def read_redis(redis_uri: str, table_name: str) -> int:
        import adbc_driver_flightsql.dbapi as dbapi

        with dbapi.connect(redis_uri, autocommit=True) as conn, conn.cursor() as cur:
            cur.execute(f'SELECT * FROM "{table_name}" ORDER BY id')
            rows = cur.fetch_arrow_table().to_pylist()
        assert [r["label"] for r in rows] == ["alpha", None, "雪"]
        assert [r["amount"] for r in rows] == [2.5, None, 7.5]
        return sum(r["id"] for r in rows)

    task = read_redis(redis_uri=uri, table_name=name)
    assert task.output == 6


def test_metaflow_local_flow(uri, workflow_rows, tmp_path):
    pytest.importorskip("metaflow")
    name, _ = workflow_rows
    flow = tmp_path / "flow.py"
    flow.write_text(textwrap.dedent("""\
        import os
        from metaflow import FlowSpec, step

        class RedisADBCCandidateFlow(FlowSpec):
            @step
            def start(self):
                import adbc_driver_flightsql.dbapi as dbapi
                with dbapi.connect(os.environ["FLIGHT_URI"], autocommit=True) as conn, conn.cursor() as cur:
                    name = os.environ["REDIS_ADBC_TEST_TABLE"]
                    cur.execute(f'SELECT * FROM "{name}" ORDER BY id')
                    self.rows = cur.fetch_arrow_table().to_pylist()
                self.next(self.end)

            @step
            def end(self):
                assert [r["id"] for r in self.rows] == [1, 2, 3]
                assert [r["label"] for r in self.rows] == ["alpha", None, "雪"]
                assert [r["active"] for r in self.rows] == [True, None, False]
                print("REDIS_ADBC_METAFLOW_OK")

        if __name__ == "__main__":
            RedisADBCCandidateFlow()
        """))
    env = {
        **os.environ, "FLIGHT_URI": uri, "REDIS_ADBC_TEST_TABLE": name,
        "METAFLOW_USER": "redis_adbc_test", "METAFLOW_DEFAULT_DATASTORE": "local",
        "METAFLOW_DEFAULT_METADATA": "local", "METAFLOW_HOME": str(tmp_path / "config"),
        "METAFLOW_DATASTORE_SYSROOT_LOCAL": str(tmp_path / ".metaflow"),
    }
    result = subprocess.run(
        [sys.executable, str(flow), "--no-pylint", "run"], cwd=tmp_path, env=env,
        text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=180,
    )
    assert result.returncode == 0, result.stdout
    assert "REDIS_ADBC_METAFLOW_OK" in result.stdout
    assert (tmp_path / ".metaflow").exists()
