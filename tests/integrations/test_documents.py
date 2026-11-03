import os
from pathlib import Path
import subprocess
import sys
import textwrap


def test_quarto(uri, workflow_rows, tmp_path):
    name, _ = workflow_rows
    doc = tmp_path / 'flight.qmd'
    doc.write_text(textwrap.dedent('''\
        ---
        title: Flight SQL integration
        format: html
        jupyter: python3
        ---
        ```{python}
        import os
        import adbc_driver_flightsql.dbapi as sql
        with sql.connect(os.environ['FLIGHT_URI'], autocommit=True) as conn, conn.cursor() as cur:
            cur.execute('SELECT * FROM "' + os.environ['FLIGHT_TABLE'] + '" ORDER BY id')
            data = cur.fetch_arrow_table()
        assert data['label'].to_pylist() == ['alpha', None, '雪']
        assert data['amount'].to_pylist() == [2.5, None, 7.5]
        print('FLIGHT_ROWS_CHECKED=3')
        ```
        '''))
    env = {**os.environ, 'FLIGHT_URI':uri, 'FLIGHT_TABLE':name,
           'QUARTO_PYTHON':sys.executable, 'JUPYTER_RUNTIME_DIR': str(tmp_path/'runtime')}
    result = subprocess.run([os.environ.get('QUARTO','quarto'),'render',str(doc)], env=env,
                            capture_output=True,text=True,timeout=180)
    assert result.returncode == 0, result.stdout + result.stderr
    assert 'FLIGHT_ROWS_CHECKED=3' in doc.with_suffix('.html').read_text()


def test_r_driver_manager(uri):
    import adbc_driver_flightsql
    root = Path(__file__).resolve().parents[2]
    result = subprocess.run([os.environ.get('RSCRIPT','Rscript'), str(root/'examples/r/flight_sql.R'),
                             uri, adbc_driver_flightsql._driver_path()],
                            capture_output=True, text=True, timeout=120)
    assert result.returncode == 0, result.stdout + result.stderr
    assert 'PASS R Flight SQL' in result.stdout
    print(result.stdout)
