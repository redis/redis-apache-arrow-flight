#!/usr/bin/env python3
"""Dispatch and verify private Enterprise qualification for one exact public SHA."""
import argparse
import io
import json
import os
import re
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
import zipfile

REPOSITORY = 'Redislabs-Solution-Architects/redis-apache-arrow-flight-enterprise'
WORKFLOW = 'enterprise-qualification.yml'


class SafeRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        if urllib.parse.urlparse(newurl).scheme != 'https':
            raise RuntimeError('Refusing non-HTTPS artifact redirect')
        target = super().redirect_request(req, fp, code, msg, headers, newurl)
        if urllib.parse.urlparse(newurl).hostname != 'api.github.com':
            target.remove_header('Authorization')
        return target


class GitHub:
    def __init__(self, token):
        if not token:
            raise RuntimeError('ENTERPRISE_CI_TOKEN is required; release qualification fails closed')
        self.token = token
        self.opener = urllib.request.build_opener(SafeRedirect())

    def request(self, path, body=None, binary=False):
        request = urllib.request.Request('https://api.github.com/repos/' + REPOSITORY + path,
            data=None if body is None else json.dumps(body).encode(), headers={
                'Authorization': 'Bearer ' + self.token,
                'Accept': 'application/vnd.github+json', 'Content-Type': 'application/json',
                'X-GitHub-Api-Version': '2022-11-28'})
        try:
            with self.opener.open(request, timeout=30) as response:
                data = response.read()
                return data if binary else (json.loads(data) if data else {})
        except urllib.error.HTTPError as error:
            raise RuntimeError('Private qualification API failed: HTTP %d' % error.code) from None


def validate_receipt(receipt, public_sha, private_sha, run_id, manifest_sha):
    expected = {'public_sha': public_sha, 'private_sha': private_sha,
                'run_id': str(run_id), 'manifest_sha256': manifest_sha,
                'full_enterprise_suite': 'passed', 'live_sql_smoke': 'passed',
                'tls_acl_verification': 'passed'}
    if any(receipt.get(k) != value for k, value in expected.items()):
        raise RuntimeError('Enterprise qualification receipt does not match the release inputs or passing checks')


def qualify(api, public_sha, timeout=2700, sleep=time.sleep, clock=time.monotonic):
    if not re.fullmatch(r'[0-9a-f]{40}', public_sha):
        raise RuntimeError('Qualification requires a full immutable public commit SHA')
    private_sha = api.request('/commits/main')['sha']
    manifest = api.request('/contents/overlay.json?ref=' + private_sha)
    import base64
    import hashlib
    manifest_bytes = base64.b64decode(manifest['content'])
    manifest_sha = hashlib.sha256(manifest_bytes).hexdigest()
    if json.loads(manifest_bytes).get('qualified_public_commit') != public_sha:
        raise RuntimeError('Release commit is not qualified by the private composition manifest')
    request_id = str(uuid.uuid4())
    title = 'Enterprise qualification ' + public_sha + ' / ' + request_id
    api.request('/actions/workflows/' + WORKFLOW + '/dispatches', {
        'ref': 'main', 'inputs': {'public_sha': public_sha, 'request_id': request_id}})
    deadline = clock() + timeout
    while clock() < deadline:
        runs = api.request('/actions/workflows/' + WORKFLOW + '/runs?event=workflow_dispatch&per_page=100')['workflow_runs']
        matches = [run for run in runs if run.get('display_title') == title
                   and run.get('head_sha') == private_sha and run.get('event') == 'workflow_dispatch']
        if len(matches) > 1:
            raise RuntimeError('Ambiguous Enterprise qualification runs')
        if matches:
            run = matches[0]
            if run['status'] == 'completed':
                if run.get('conclusion') != 'success':
                    raise RuntimeError('Full Enterprise qualification failed: ' + run['html_url'])
                artifacts = api.request('/actions/runs/%d/artifacts' % run['id'])['artifacts']
                receipts = [a for a in artifacts if a['name'] == 'enterprise-qualification' and not a['expired']]
                if len(receipts) != 1:
                    raise RuntimeError('Expected one unexpired Enterprise qualification receipt')
                archive = api.request('/actions/artifacts/%d/zip' % receipts[0]['id'], binary=True)
                with zipfile.ZipFile(io.BytesIO(archive)) as bundle:
                    info = bundle.getinfo('qualification.json')
                    if info.file_size > 8192:
                        raise RuntimeError('Oversized Enterprise qualification receipt')
                    receipt = json.loads(bundle.read(info))
                validate_receipt(receipt, public_sha, private_sha, run['id'], manifest_sha)
                return run['html_url']
        sleep(15)
    raise RuntimeError('Timed out waiting for full Enterprise qualification; release blocked')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--sha', required=True)
    parser.add_argument('--timeout', type=int, default=2700)
    args = parser.parse_args()
    print('Verified full Enterprise qualification:', qualify(
        GitHub(os.environ.get('ENTERPRISE_CI_TOKEN')), args.sha, args.timeout))


if __name__ == '__main__':
    try:
        main()
    except (RuntimeError, OSError, ValueError, KeyError, zipfile.BadZipFile) as error:
        raise SystemExit(str(error))
