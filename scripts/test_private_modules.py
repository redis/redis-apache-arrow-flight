"""Credential boundary checks; use synthetic tokens, never real credentials."""
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest

ROOT = Path(__file__).resolve().parent


class PrivateModulesTest(unittest.TestCase):
    def helper(self, action="get", host="github.com", protocol="https", **env):
        return subprocess.check_output(
            [str(ROOT / "github-credential.sh"), action],
            input=f"protocol={protocol}\nhost={host}\n\n",
            text=True,
            env={**os.environ, "REDIS_ADBC_TOKEN": "synthetic-test-token", **env},
        )

    def test_credentials_only_for_https_github_reads(self):
        self.assertIn("password=synthetic-test-token", self.helper())
        for args in [{"action": "store"}, {"action": "erase"},
                     {"host": "example.com"}, {"protocol": "http"}]:
            self.assertEqual(self.helper(**args), "")

    def test_secret_file_and_no_credential_persistence(self):
        with tempfile.TemporaryDirectory() as tmp:
            secret = Path(tmp) / "token"
            secret.write_text("synthetic-file-token\n")
            self.assertIn("password=synthetic-file-token", self.helper(REDIS_ADBC_TOKEN_FILE=str(secret)))
        env = {**os.environ, "REDIS_ADBC_TOKEN": "synthetic-test-token",
               "GIT_CONFIG_COUNT": "1", "GIT_CONFIG_KEY_0": "user.name",
               "GIT_CONFIG_VALUE_0": "preserved"}
        result = subprocess.check_output(
            [str(ROOT / "with-private-modules.sh"), "git", "config", "--list"],
            text=True, env=env)
        self.assertIn("user.name=preserved", result)
        self.assertNotIn("synthetic-test-token", result)

    def test_module_scope_preserves_public_checksum_validation(self):
        result = subprocess.check_output(
            [str(ROOT / "with-private-modules.sh"), "python3", "-c",
             "import json,os; print(json.dumps({k:os.environ.get(k) for k in ['GONOPROXY','GONOSUMDB','GOSUMDB']}))"],
            text=True, env={**os.environ, "GONOPROXY": "example.com/other", "GONOSUMDB": "example.com/other", "GOSUMDB": "sum.golang.org"})
        settings = json.loads(result)
        self.assertEqual(settings["GONOPROXY"], "example.com/other,github.com/alberttwong/redis-adbc-driver/go")
        self.assertEqual(settings["GONOSUMDB"], settings["GONOPROXY"])
        self.assertEqual(settings["GOSUMDB"], "sum.golang.org")


if __name__ == "__main__":
    unittest.main()
