#!/usr/bin/env python3
"""Run all 27 inventoried Flight SQL routes with explicit application/manual gaps."""
import argparse
import datetime as dt
import importlib.metadata
import json
import os
from pathlib import Path
import subprocess
import sys
import time
import xml.etree.ElementTree as ET

ROOT = Path(__file__).resolve().parents[1]
TESTS = 'tests/integrations/'
PROFILES = {
    'core': ['test_ml_ingest.py', 'test_resources.py', 'test_clients.py'],
    'workflows': ['test_jupyter.py', 'test_hf_datasets.py', 'test_streamlit.py', 'test_scipy.py'],
    'models': ['test_model_candidates.py', 'test_plotting_candidates.py'],
    'orchestrators': ['test_orchestrator_candidates.py'],
    'spark': ['test_spark.py'],
    'airflow': ['test_airflow.py'],
    'documents': ['test_documents.py'],
}
# Tool entries, not test-case counts. The six pre-existing routes are labeled.
TOOLS = {
    'NumPy': ('core', 'test_numpy', False),
    'scikit-learn': ('core', 'test_scikit', False),
    'SQLAlchemy': ('core', 'test_sqlalchemy', False),
    'Dagster': ('core', 'test_dagster', False),
    'pandas': ('core', 'test_pandas', True),
    'Polars': ('core', 'test_polars', True),
    'PyArrow': ('core', 'test_pyarrow', True),
    'Jupyter / JupyterLab': ('workflows', 'test_notebook', False),
    'Hugging Face Datasets': ('workflows', 'test_arrow_dataset', False),
    'Streamlit': ('workflows', 'test_dataframe', False),
    'SciPy': ('workflows', 'test_sparse', False),
    'XGBoost': ('models', 'test_xgboost', False),
    'PyTorch': ('models', 'test_pytorch', False),
    'TensorFlow': ('models', 'test_tensorflow', False),
    'Keras': ('models', 'test_keras', False),
    'Hugging Face Transformers': ('models', 'test_transformers', False),
    'Plotly': ('models', 'test_plotly', False),
    'Matplotlib': ('models', 'test_matplotlib', False),
    'Kubeflow Pipelines': ('orchestrators', 'test_kubeflow', False),
    'Metaflow': ('orchestrators', 'test_metaflow', False),
    'Apache Spark': ('spark', 'test_spark', False),
    'Apache Airflow': ('airflow', 'test_airflow', False),
    'Quarto': ('documents', 'test_quarto', False),
    'R': ('documents', 'test_r_driver', True),
    'RStudio': ('manual', '', False),
    'Tableau': ('manual', '', True),
    'Metabase': ('manual', '', True),
}


def summarize(results, selected, runs, applications=None):
    applications = applications or {}
    tools = []
    for name, (profile, prefix, baseline) in TOOLS.items():
        row = {'tool': name, 'previously_exercised': baseline, 'profile': profile}
        xml = results / f'{profile}.xml'
        if profile == 'manual':
            row.update(status='manual_required', reason='Run the application acceptance procedure in the integration guide; historical evidence is not a rerun.')
            if name in applications:
                row.update(applications[name])
                row.pop('reason', None)
        elif profile not in selected:
            row.update(status='not_run', reason='Profile not selected')
        elif not xml.exists():
            row.update(status='blocked', reason='No JUnit report: inspect the profile log')
        else:
            cases = [c for c in ET.parse(xml).iter('testcase') if c.get('name', '').startswith(prefix)]
            failed = [c for c in cases if c.find('failure') is not None or c.find('error') is not None]
            passed = [c for c in cases if c.find('failure') is None and c.find('error') is None and c.find('skipped') is None]
            skipped = [c for c in cases if c.find('skipped') is not None]
            row.update(status='failed' if failed else 'passed' if passed else 'blocked',
                       passed=len(passed), failed=len(failed), skipped=len(skipped),
                       cases=[c.get('name') for c in cases], log=f'{profile}.log')
            if skipped:
                row['limitations'] = [c.find('skipped').get('message') for c in skipped]
            if not cases:
                row['reason'] = 'No matching test collected'
        tools.append(row)
    report = {'created_at': dt.datetime.now(dt.timezone.utc).isoformat(),
              'source_revision': subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=ROOT, text=True).strip(),
              'scope': '27 deck routes; custom pipelines and local workflows only where stated. No production certification.',
              'profiles': runs, 'tools': tools}
    (results/'summary.json').write_text(json.dumps(report, indent=2)+'\n')
    lines = ['# Flight SQL integration results', '', '| Tool | Prior route | Current result | Cases passed / failed / skipped |', '| --- | --- | --- | --- |']
    for t in tools:
        lines.append(f"| {t['tool']} | {'exercised' if t['previously_exercised'] else 'candidate'} | {t['status']} | {t.get('passed',0)} / {t.get('failed',0)} / {t.get('skipped',0)} |")
    (results/'summary.md').write_text('\n'.join(lines)+'\n')
    return report


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--uri', default=os.environ.get('FLIGHT_URI', 'grpc://localhost:8815'))
    p.add_argument('--env-root', type=Path, default=Path('/tmp/flight-integration-envs'))
    p.add_argument('--results', type=Path, required=True, help='New directory; stale reports are rejected')
    p.add_argument('--profiles', nargs='+', choices=list(PROFILES), default=list(PROFILES))
    p.add_argument('--setup', action='store_true', help='Create isolated uv environments from pinned requirements')
    p.add_argument('--uv', default='uv')
    p.add_argument('--application-results', type=Path, help='JSON receipts from fresh RStudio/Tableau/Metabase application checks')
    p.add_argument('--automated-only', action='store_true', help='Exit status reflects selected automated profiles; application gaps remain visible')
    args = p.parse_args()
    applications = {}
    if args.application_results:
        receipts = json.loads(args.application_results.read_text())
        if receipts['uri'] != args.uri:
            p.error('Application receipts must use the same Flight URI')
        observed = dt.datetime.fromisoformat(receipts['observed_at'])
        age = (dt.datetime.now(dt.timezone.utc) - observed).total_seconds()
        if not 0 <= age < 86400:
            p.error('Application receipts must be from the last 24 hours')
        for name, receipt in receipts['tools'].items():
            if name not in TOOLS or TOOLS[name][0] != 'manual' or receipt['status'] not in ('passed', 'failed', 'blocked'):
                p.error('Invalid application receipt')
            evidence = args.application_results.parent / receipt['evidence']
            if not evidence.is_file():
                p.error('Application receipt evidence is missing')
            applications[name] = {**receipt, 'observed_at': receipts['observed_at']}
    results = args.results.resolve()
    results.mkdir(parents=True, exist_ok=True)
    if any(results.iterdir()):
        p.error('--results must be empty to prevent stale evidence')
    env = {**os.environ, 'FLIGHT_URI':args.uri, 'OMP_NUM_THREADS':'1', 'TF_CPP_MIN_LOG_LEVEL':'2',
           'MPLCONFIGDIR':str(results/'matplotlib'), 'PYTEST_DISABLE_PLUGIN_AUTOLOAD':'1'}
    (results/'artifacts').mkdir()
    runs = {}
    for profile in args.profiles:
        python = args.env_root.resolve()/profile/'bin/python'
        started = time.monotonic()
        print(f'Running {profile}', flush=True)
        with (results/f'{profile}.log').open('w') as log:
            try:
                if args.setup:
                    subprocess.run([args.uv,'venv','--allow-existing','--python','3.14' if profile=='airflow' else '3.12',str(python.parent.parent)],stdout=log,stderr=subprocess.STDOUT,check=True,timeout=300)
                    subprocess.run([args.uv,'pip','install','--python',str(python),'-r',str(ROOT/'requirements/integrations'/f'{profile}.txt')],stdout=log,stderr=subprocess.STDOUT,check=True,timeout=900)
                versions = subprocess.check_output([str(python),'-c','import importlib.metadata as m,json,sys; print(json.dumps({"python":sys.version,"packages":{d.metadata["Name"]:d.version for d in m.distributions()}},indent=2))'],text=True)
                (results/f'{profile}-versions.json').write_text(versions)
                cmd = [str(python),'-m','pytest','-v','-rA',*['tests/integrations/'+t for t in PROFILES[profile]],
                       '--junitxml='+str(results/f'{profile}.xml'),'--basetemp='+str(results/'artifacts'/profile)]
                if profile == 'models':
                    # Native ML runtimes can abort when loaded together (macOS
                    # PyTorch after XGBoost training). Isolate each tool process.
                    names = ['xgboost', 'pytorch', 'tensorflow', 'keras', 'transformers', 'plotly', 'matplotlib']
                    statuses = []
                    combined = ET.Element('testsuites')
                    for name in names:
                        xml_path = results / f'models-{name}.xml'
                        child = cmd[:-2] + ['-k', name, '--junitxml='+str(xml_path),
                                          '--basetemp='+str(results/'artifacts'/f'models-{name}')]
                        code = subprocess.run(child,cwd=ROOT,env=env,stdout=log,stderr=subprocess.STDOUT,timeout=180).returncode
                        statuses.append(code)
                        if xml_path.exists():
                            combined.extend(list(ET.parse(xml_path).getroot()))
                    ET.ElementTree(combined).write(results/'models.xml',encoding='unicode')
                    status = 1 if any(statuses) else 0
                else:
                    status = subprocess.run(cmd,cwd=ROOT,env=env,stdout=log,stderr=subprocess.STDOUT,timeout=900).returncode
            except (OSError,subprocess.SubprocessError) as e:
                log.write(str(e)+'\n')
                status = 2
        runs[profile] = {'exit_code':status,'elapsed_seconds':round(time.monotonic()-started,3)}
        print(f'{profile}: exit {status}', flush=True)
    report = summarize(results, args.profiles, runs, applications)
    report['flight_uri'] = args.uri
    (results/'summary.json').write_text(json.dumps(report, indent=2)+'\n')
    if args.application_results:
        (results/'application-results.json').write_text(args.application_results.read_text())
        for receipt in applications.values():
            source = args.application_results.parent / receipt['evidence']
            (results/source.name).write_bytes(source.read_bytes())
    print(json.dumps({s:sum(t['status']==s for t in report['tools']) for s in ['passed','failed','blocked','manual_required','not_run']}))
    checked = [t for t in report['tools'] if not args.automated_only or t['profile'] in args.profiles]
    return 1 if any(t['status'] != 'passed' for t in checked) or any(r['exit_code'] for r in runs.values()) else 0


if __name__ == '__main__':
    sys.exit(main())
