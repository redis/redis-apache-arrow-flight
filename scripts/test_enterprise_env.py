"""Certificate readiness is bounded, idempotent, and preserves verified trust."""
import importlib.util
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

spec = importlib.util.spec_from_file_location('enterprise_env', Path(__file__).with_name('enterprise-env.py'))
env = importlib.util.module_from_spec(spec)
spec.loader.exec_module(env)


class CertificateReadiness(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)
        (self.root / 'proxy.crt').write_text('test-certificate')
        (self.root / 'proxy.key').write_text('test-key')
        state = patch.object(env, 'STATE', self.root)
        state.start()
        self.addCleanup(state.stop)
        self.api = Mock(context=SimpleNamespace(check_hostname=False))

    def test_transient_service_unavailable_retries_identical_put(self):
        self.api.request.side_effect = [RuntimeError('Management HTTP 503'), {}]
        with patch.object(env.time, 'sleep'):
            env.install_certificates(self.api)
        first, second = self.api.request.call_args_list
        self.assertEqual(first, second)
        self.assertEqual(first.args[0], '/cluster/certificates')
        self.assertEqual(first.args[2], 'PUT')
        self.assertEqual([x['name'] for x in first.args[1]['certificates']], ['proxy', 'api'])
        self.api.load_context.assert_called_once_with()
        self.assertTrue(self.api.context.check_hostname)

    def test_nontransient_failure_does_not_retry_or_advance_trust(self):
        self.api.request.side_effect = RuntimeError('Management HTTP 403')
        with self.assertRaisesRegex(RuntimeError, 'HTTP 403'):
            env.install_certificates(self.api)
        self.api.request.assert_called_once()
        self.api.load_context.assert_not_called()
        self.assertFalse(self.api.context.check_hostname)

    def test_unavailable_service_has_a_deadline(self):
        self.api.request.side_effect = RuntimeError('Management HTTP 503')
        with patch.object(env.time, 'monotonic', side_effect=[0, 0, 361]), patch.object(env.time, 'sleep'):
            with self.assertRaisesRegex(RuntimeError, 'Timed out waiting for cluster certificates'):
                env.install_certificates(self.api)
        self.assertEqual(self.api.request.call_count, 2)
        self.api.load_context.assert_not_called()
        self.assertFalse(self.api.context.check_hostname)


if __name__ == '__main__':
    unittest.main()
