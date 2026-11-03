"""The report must not turn absent, skipped or failed checks into passes."""
import importlib.util
from pathlib import Path
import tempfile
import unittest

spec = importlib.util.spec_from_file_location('runner', Path(__file__).resolve().parents[1]/'scripts/test-integrations.py')
runner = importlib.util.module_from_spec(spec)
spec.loader.exec_module(runner)


class ReportTests(unittest.TestCase):
    def test_missing_report_blocks_every_selected_tool(self):
        with tempfile.TemporaryDirectory() as directory:
            report = runner.summarize(Path(directory), ['core'], {'core': {'exit_code': 2}})
            rows = {t['tool']:t for t in report['tools']}
            self.assertEqual(rows['NumPy']['status'], 'blocked')
            self.assertEqual(rows['PyTorch']['status'], 'not_run')
            self.assertEqual(rows['RStudio']['status'], 'manual_required')
            self.assertEqual(len(rows), 27)
            self.assertEqual(sum(not t['previously_exercised'] for t in rows.values()), 21)

    def test_failed_skipped_uncollected_and_scoped_pass(self):
        with tempfile.TemporaryDirectory() as directory:
            directory = Path(directory)
            (directory/'core.xml').write_text('''<testsuites><testsuite>
              <testcase name="test_numpy"><failure message="wrong rows"/></testcase>
              <testcase name="test_scikit"><skipped message="missing dependency"/></testcase>
              <testcase name="test_pyarrow"/>
            </testsuite></testsuites>''')
            rows = {t['tool']:t for t in runner.summarize(directory, ['core'], {})['tools']}
            self.assertEqual(rows['NumPy']['status'], 'failed')
            self.assertEqual(rows['scikit-learn']['status'], 'blocked')
            self.assertEqual(rows['SQLAlchemy']['status'], 'blocked')
            self.assertEqual(rows['PyArrow']['status'], 'passed')
            self.assertEqual(rows['PyArrow']['passed'], 1)


if __name__ == '__main__':
    unittest.main()
