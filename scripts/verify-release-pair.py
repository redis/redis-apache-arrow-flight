#!/usr/bin/env python3
"""Validate a public release produced by the approved Enterprise coordinator."""
import argparse, json, os, re, urllib.request

REPOSITORY = 'redis/redis-apache-arrow-flight'


def validate(release, asset, pair, tag_commit, release_version, app_slug):
    if not re.fullmatch(r'v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(-[0-9A-Za-z-]+(\.[0-9A-Za-z-]+)*)?', release_version):
        raise ValueError('invalid release version')
    if not app_slug or asset.get('uploader', {}).get('login') != app_slug + '[bot]':
        raise ValueError('release provenance was not uploaded by the approved GitHub App')
    if release.get('draft') or release.get('tag_name') != release_version or release.get('prerelease') != ('-' in release_version):
        raise ValueError('release must be published with the requested version and prerelease status')
    if pair.get('schema_version') != 1 or pair.get('version') != release_version or pair.get('public_repository') != REPOSITORY:
        raise ValueError('release provenance does not match this repository and version')
    if pair.get('enterprise_repository') != 'Redislabs-Solution-Architects/redis-apache-arrow-flight-enterprise':
        raise ValueError('unexpected Enterprise coordinator repository')
    if not re.fullmatch('[0-9a-f]{40}', pair.get('public_sha', '')) or pair['public_sha'] != tag_commit:
        raise ValueError('public tag does not match the qualified commit')
    if not re.fullmatch('[0-9a-f]{40}', pair.get('private_sha', '')):
        raise ValueError('immutable Enterprise commit required')
    proof = pair.get('qualification', {})
    if not re.fullmatch('[0-9a-f]{64}', proof.get('manifest_sha256', '')) or not re.fullmatch('[1-9][0-9]*', str(proof.get('run_id', ''))):
        raise ValueError('qualification binding is missing')
    if any(proof.get(k) != 'passed' for k in ('full_enterprise_suite', 'live_sql_smoke', 'tls_acl_verification')):
        raise ValueError('Enterprise qualification has not passed')
    return pair['public_sha']


def request(path):
    req = urllib.request.Request('https://api.github.com/repos/' + REPOSITORY + path,
        headers={'Authorization': 'Bearer ' + os.environ['GH_TOKEN'], 'Accept': 'application/vnd.github+json'})
    with urllib.request.urlopen(req, timeout=30) as response:
        return json.load(response)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--version', required=True)
    args = parser.parse_args()
    if not re.fullmatch(r'v[0-9A-Za-z.-]+', args.version):
        raise ValueError('invalid release version')
    release = request('/releases/tags/' + args.version)
    assets = request('/releases/' + str(release['id']) + '/assets?per_page=100')
    found = [a for a in assets if a['name'] == 'release-pair.json']
    if len(found) != 1 or found[0]['size'] > 65536:
        raise ValueError('missing or invalid coordinated release provenance')
    # The release is public: download without forwarding any repository token.
    asset = found[0]
    url = asset['browser_download_url']
    if not url.startswith('https://github.com/' + REPOSITORY + '/releases/download/'):
        raise ValueError('unexpected release asset origin')
    with urllib.request.urlopen(url, timeout=30) as response:
        pair = json.loads(response.read(65537))
    target = request('/git/ref/tags/' + args.version)['object']
    for _ in range(8):
        if target['type'] != 'tag':
            break
        target = request('/git/tags/' + target['sha'])['object']
    if target['type'] != 'commit':
        raise ValueError('release tag does not resolve to a commit')
    sha = validate(release, asset, pair, target['sha'], args.version, os.environ.get('RELEASE_COORDINATOR_APP_SLUG', ''))
    with open(os.environ['GITHUB_OUTPUT'], 'a') as output:
        output.write('sha=' + sha + '\nversion=' + args.version + '\n')


if __name__ == '__main__':
    main()
