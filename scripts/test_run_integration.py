import importlib.util
from pathlib import Path
import subprocess
import signal
import sys
import tempfile
import time
import unittest

SCRIPT = Path(__file__).with_name("run-integration.py")
spec = importlib.util.spec_from_file_location("integration_runner", SCRIPT)
runner = importlib.util.module_from_spec(spec)
spec.loader.exec_module(runner)


class IntegrationRunnerTests(unittest.TestCase):
    def test_scope_ignores_credentials_but_separates_databases(self):
        a = runner.database_scope("rediss://alice:secret@host:6379/0?dial_timeout=1")
        for uri in ("rediss://bob:other@HOST/0", "rediss://host/", "rediss://host", "rediss://host/2?db=0"):
            self.assertEqual(a, runner.database_scope(uri))
        self.assertNotEqual(a, runner.database_scope("rediss://alice:secret@host/1"))
        self.assertNotIn("secret", a)
        with self.assertRaises(ValueError):
            runner.database_scope("redis://user:secret@host/0")

    def test_exit_status_is_preserved(self):
        with tempfile.TemporaryDirectory() as tmp:
            result = subprocess.run([sys.executable, str(SCRIPT), "--scope", "test",
                                     "--lock-dir", tmp, "--", sys.executable, "-c", "raise SystemExit(7)"],
                                    stdout=subprocess.DEVNULL)
            self.assertEqual(result.returncode, 7)

    def test_same_scope_serializes_and_different_scope_can_run(self):
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            code = "from pathlib import Path; import sys,time; p=Path(sys.argv[1]); p.write_text('entered');\nwhile not Path(sys.argv[2]).exists(): time.sleep(.01)"
            def start(name, scope):
                return subprocess.Popen([sys.executable, str(SCRIPT), "--scope", scope,
                    "--lock-dir", tmp, "--", sys.executable, "-c", code,
                    str(directory / name), str(directory / (name + ".release"))],
                    stdout=subprocess.PIPE, text=True)
            def entered(name):
                deadline = time.monotonic() + 5
                while not (directory / name).exists():
                    if time.monotonic() > deadline:
                        self.fail("validation process did not start")
                    time.sleep(.01)
            processes = []
            try:
                first = start("first", "same"); processes.append(first)
                entered("first")
                second = start("second", "same"); processes.append(second)
                self.assertIn("Waiting", second.stdout.readline())
                independent = start("independent", "different"); processes.append(independent)
                entered("independent")
                self.assertFalse((directory / "second").exists(), "same catalog commands overlapped")
                (directory / "first.release").touch()
                self.assertEqual(first.wait(timeout=5), 0)
                entered("second")
                for name in ("second", "independent"):
                    (directory / (name + ".release")).touch()
                for process in processes:
                    self.assertEqual(process.wait(timeout=5), 0)
            finally:
                for process in processes:
                    if process.poll() is None:
                        process.terminate(); process.wait(timeout=5)
                    process.stdout.close()

    def test_termination_waits_for_child_and_preserves_failure(self):
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            code = """from pathlib import Path
import signal,sys,time
root=Path(sys.argv[1])
def stop(signum, frame):
    (root/'signalled').touch()
    while not (root/'release').exists(): time.sleep(.01)
    raise SystemExit(0)
signal.signal(signal.SIGTERM, stop)
(root/'ready').touch()
while True: time.sleep(.05)
"""
            process = subprocess.Popen([sys.executable, str(SCRIPT), "--scope", "signal-test",
                "--lock-dir", tmp, "--", sys.executable, "-c", code, tmp], stdout=subprocess.DEVNULL)
            def wait_file(name):
                deadline = time.monotonic() + 5
                while not (directory/name).exists():
                    if time.monotonic() > deadline:
                        self.fail("child signal handling did not complete")
                    time.sleep(.01)
            try:
                wait_file("ready")
                process.terminate()
                wait_file("signalled")
                self.assertIsNone(process.poll(), "runner exited before child cleanup")
                (directory/"release").touch()
                self.assertEqual(process.wait(timeout=5), 128 + signal.SIGTERM)
            finally:
                (directory/"release").touch()
                if process.poll() is None:
                    process.terminate(); process.wait(timeout=5)
