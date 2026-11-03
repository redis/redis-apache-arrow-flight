"""A real local TaskFlow DAG; no scheduler deployment is claimed."""
import os
import subprocess
import sys
import textwrap


def test_airflow_taskflow(uri, workflow_rows, tmp_path):
    name, _ = workflow_rows
    script = tmp_path / 'dag.py'
    script.write_text(textwrap.dedent('''\
        import os
        from airflow.sdk import dag, task
        import pendulum
        @dag(schedule=None, start_date=pendulum.datetime(2026,1,1,tz='UTC'), catchup=False)
        def flight_workflow():
            @task
            def read():
                import adbc_driver_flightsql.dbapi as sql
                with sql.connect(os.environ['FLIGHT_URI'], autocommit=True) as conn, conn.cursor() as cur:
                    name = os.environ['FLIGHT_TABLE']
                    cur.execute(f'SELECT * FROM "{name}" WHERE id >= ? ORDER BY id', (2,))
                    rows = cur.fetch_arrow_table().to_pylist()
                assert [r['label'] for r in rows] == [None, '雪']
                return rows
            @task
            def check(rows):
                assert [r['amount'] for r in rows] == [None, 7.5]
                print('FLIGHT_AIRFLOW_OK')
            check(read())
        dag_instance = flight_workflow()
        if __name__ == '__main__':
            result = dag_instance.test()
            assert result.state == 'success', result.state
            assert all(str(t.state) == 'success' for t in result.get_task_instances())
        '''))
    env = {**os.environ,'FLIGHT_URI':uri,'FLIGHT_TABLE':name,'AIRFLOW_HOME':str(tmp_path),
           'AIRFLOW__DATABASE__SQL_ALCHEMY_CONN':f'sqlite:///{tmp_path}/airflow.db',
           'AIRFLOW__CORE__LOAD_EXAMPLES':'False','AIRFLOW__CORE__DAGS_FOLDER':str(tmp_path)}
    for args in [['-m','airflow','db','migrate'],[str(script)]]:
        result = subprocess.run([sys.executable,*args],env=env,cwd=tmp_path,
                                capture_output=True,text=True,timeout=180)
        assert result.returncode == 0, result.stdout + result.stderr
    assert 'FLIGHT_AIRFLOW_OK' in result.stdout
