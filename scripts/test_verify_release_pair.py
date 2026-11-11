import copy, importlib.util, pathlib, unittest

spec = importlib.util.spec_from_file_location('pair', pathlib.Path(__file__).with_name('verify-release-pair.py'))
pair = importlib.util.module_from_spec(spec)
spec.loader.exec_module(pair)


class PublicReleaseProvenance(unittest.TestCase):
    def setUp(self):
        self.release = dict(draft=False, tag_name='v0.2.0', prerelease=False)
        self.asset = dict(uploader={'login': 'release-coordinator[bot]'})
        self.pair = dict(schema_version=1, version='v0.2.0', public_repository=pair.REPOSITORY,
                         enterprise_repository='Redislabs-Solution-Architects/redis-apache-arrow-flight-enterprise',
                         public_sha='a' * 40, private_sha='b' * 40,
                         qualification=dict(run_id='123', manifest_sha256='c' * 64,
                           full_enterprise_suite='passed', live_sql_smoke='passed', tls_acl_verification='passed'))

    def validate(self):
        return pair.validate(self.release, self.asset, self.pair, 'a' * 40, 'v0.2.0', 'release-coordinator')

    def test_exact_published_pair_is_accepted(self):
        self.assertEqual(self.validate(), 'a' * 40)

    def test_other_uploader_or_missing_app_configuration_is_rejected(self):
        self.asset['uploader']['login'] = 'someone'
        with self.assertRaisesRegex(ValueError, 'approved GitHub App'):
            self.validate()
        with self.assertRaises(ValueError):
            pair.validate(self.release, self.asset, self.pair, 'a' * 40, 'v0.2.0', '')

    def test_draft_or_other_release_version_is_rejected(self):
        for key, value in [('draft', True), ('tag_name', 'v0.3.0'), ('prerelease', True)]:
            original = dict(self.release)
            self.release[key] = value
            with self.subTest(key=key), self.assertRaises(ValueError):
                self.validate()
            self.release = original

    def test_source_binding_and_passed_qualification_are_required(self):
        for key in ('schema_version', 'version', 'public_repository', 'enterprise_repository', 'public_sha', 'private_sha'):
            original = copy.deepcopy(self.pair)
            self.pair[key] = 'incorrect'
            with self.subTest(key=key), self.assertRaises(ValueError):
                self.validate()
            self.pair = original
        for key in self.pair['qualification']:
            original = copy.deepcopy(self.pair)
            self.pair['qualification'][key] = 'incorrect'
            with self.subTest(key=key), self.assertRaises(ValueError):
                self.validate()
            self.pair = original


if __name__ == '__main__':
    unittest.main()
