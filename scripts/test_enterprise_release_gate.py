import importlib.util
import pathlib
import unittest

spec=importlib.util.spec_from_file_location('gate',pathlib.Path(__file__).with_name('enterprise-release-gate.py'))
gate=importlib.util.module_from_spec(spec);spec.loader.exec_module(gate)

class ReleaseGate(unittest.TestCase):
    def test_missing_token_fails_closed(self):
        with self.assertRaisesRegex(RuntimeError,'fails closed'): gate.GitHub('')

    def test_requires_full_commit_sha(self):
        with self.assertRaisesRegex(RuntimeError,'immutable'): gate.qualify(None,'main')

    def test_binds_receipt_to_exact_run_and_both_revisions(self):
        values=dict(public_sha='a'*40,private_sha='b'*40,run_id='123',manifest_sha256='c'*64,
                    full_enterprise_suite='passed',live_sql_smoke='passed',tls_acl_verification='passed')
        gate.validate_receipt(values,'a'*40,'b'*40,123,'c'*64)
        for key in values:
            with self.subTest(key=key), self.assertRaisesRegex(RuntimeError,'does not match'):
                gate.validate_receipt(dict(values,**{key:'wrong'}),'a'*40,'b'*40,123,'c'*64)

    def test_failure_blocks_release(self):
        import base64
        class Failed:
            def request(self,path,body=None,binary=False):
                if path=='/commits/main':return {'sha':'b'*40}
                if path.startswith('/contents/'):return {'content':base64.b64encode(b'{"qualified_public_commit":"'+b'a'*40+b'"}').decode()}
                if body:
                    self.title='Enterprise qualification '+body['inputs']['public_sha']+' / '+body['inputs']['request_id']
                    return {}
                return {'workflow_runs':[{'display_title':self.title,'head_sha':'b'*40,
                    'event':'workflow_dispatch','status':'completed','conclusion':'failure',
                    'html_url':'https://github.com/private/run'}]}
        with self.assertRaisesRegex(RuntimeError,'qualification failed'):gate.qualify(Failed(),'a'*40)


class ReceiptFlow(unittest.TestCase):
    def fake(self, expired=False, stale=False):
        import base64,hashlib,io,json,zipfile
        manifest=json.dumps({'qualified_public_commit':'a'*40}).encode()
        class API:
            def request(self,path,body=None,binary=False):
                if path=='/commits/main':return {'sha':'b'*40}
                if path.startswith('/contents/'):return {'content':base64.b64encode(manifest).decode()}
                if body:
                    self.title='Enterprise qualification '+body['inputs']['public_sha']+' / '+body['inputs']['request_id']
                    return {}
                if '/runs?' in path:return {'workflow_runs':[{'display_title':self.title+(' stale' if stale else ''),
                    'head_sha':'b'*40,'event':'workflow_dispatch','status':'completed','conclusion':'success',
                    'id':7,'html_url':'https://github.com/private/run/7'}]}
                if path.endswith('/artifacts'):return {'artifacts':[{'name':'enterprise-qualification','expired':expired,'id':9}]}
                data={'public_sha':'a'*40,'private_sha':'b'*40,'run_id':'7',
                      'manifest_sha256':hashlib.sha256(manifest).hexdigest(),
                      'full_enterprise_suite':'passed','live_sql_smoke':'passed','tls_acl_verification':'passed'}
                out=io.BytesIO()
                with zipfile.ZipFile(out,'w') as archive:archive.writestr('qualification.json',json.dumps(data))
                return out.getvalue()
        return API()

    def test_success_requires_run_and_receipt(self):
        self.assertEqual(gate.qualify(self.fake(),'a'*40),'https://github.com/private/run/7')

    def test_expired_receipt_blocks_release(self):
        with self.assertRaisesRegex(RuntimeError,'unexpired'):gate.qualify(self.fake(expired=True),'a'*40)

    def test_old_run_cannot_satisfy_new_request(self):
        ticks=iter([0,0,30])
        with self.assertRaisesRegex(RuntimeError,'Timed out'):
            gate.qualify(self.fake(stale=True),'a'*40,timeout=10,sleep=lambda _:None,clock=lambda:next(ticks))

if __name__=='__main__':unittest.main()
