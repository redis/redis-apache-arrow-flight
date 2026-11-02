#!/usr/bin/env python3
"""Provision an isolated local Redis Enterprise TLS test environment.

Secrets stay in .local/enterprise (0700); this script never prints connection URIs.
Requires Docker Compose, Python 3.9+, OpenSSL, Go, and a qualified private Flight
composition. No global trust-store changes and no plaintext Redis fallback.
"""
import argparse
import base64
import hashlib
import json
import os
from pathlib import Path
import secrets
import socket
import ssl
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request

ROOT = Path(__file__).resolve().parents[1]
STATE = ROOT / '.local/enterprise'
RUNTIME = STATE / 'runtime'
API = 'https://localhost:29443/v1'


def run(args, **kwargs):
    return subprocess.run(args, check=True, **kwargs)


def private_file(path, content):
    # Set permissions before writing even on first creation.
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    os.fchmod(fd, 0o600)
    with os.fdopen(fd, 'w') as out:
        out.write(content)
    path.chmod(0o600)


def config():
    STATE.mkdir(parents=True, exist_ok=True, mode=0o700)
    STATE.chmod(0o700)
    path = STATE / 'credentials.json'
    if not path.exists():
        private_file(path, json.dumps({
            'admin_user': 'admin@flight-test.local',
            'admin_password': secrets.token_urlsafe(32),
            'username': 'flight_test', 'password': secrets.token_urlsafe(32),
            'default_password': secrets.token_urlsafe(32),
        }))
    path.chmod(0o600)
    return json.loads(path.read_text())


def compose_env():
    # Paths are configurable, not credentials. The builder checks compatibility.
    env = os.environ.copy()
    saved = STATE / 'paths.json'
    if saved.exists():
        for k,v in json.loads(saved.read_text()).items():
            env.setdefault(k,v)
    default = Path.home() / 'sandbox/work'
    env.setdefault('ENTERPRISE_FLIGHT_SOURCE', str(default / 'redis-apache-arrow-flight'))
    env.setdefault('ENTERPRISE_FLIGHT_COMPOSITION', str(default / 'redis-apache-arrow-flight-enterprise'))
    if 'ENTERPRISE_GO_MODULE_CACHE' not in env:
        env['ENTERPRISE_GO_MODULE_CACHE'] = subprocess.check_output(
            ['go', 'env', 'GOMODCACHE'], text=True).strip()
    return env


def dc(*args, **kwargs):
    return run(['docker', 'compose', '-f', str(ROOT / 'compose.enterprise.yaml'), *args],
               cwd=ROOT, env=compose_env(), **kwargs)


def retry(label, fn, timeout=360):
    deadline = time.monotonic() + timeout
    while True:
        try:
            result = fn()
            if result:
                return result
        except (urllib.error.URLError, ConnectionError, TimeoutError, OSError, subprocess.CalledProcessError):
            pass
        except RuntimeError as exc:
            if "HTTP 503" not in str(exc):
                raise
        if time.monotonic() > deadline:
            raise RuntimeError('Timed out waiting for ' + label)
        time.sleep(2)


def certificates():
    RUNTIME.mkdir(exist_ok=True)
    key = STATE / 'ca.key'
    ca = RUNTIME / 'ca.crt'
    marker = STATE / 'certificate-version'
    complete = all(p.is_file() for p in (key, ca, STATE / 'proxy.key', STATE / 'proxy.crt'))
    if complete and marker.exists() and marker.read_text() == '2':
        if subprocess.run(['openssl', 'x509', '-checkend', '86400', '-noout',
                           '-in', str(ca)], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL).returncode == 0:
            return
    run(['openssl', 'req', '-x509', '-newkey', 'rsa:2048', '-nodes', '-sha256',
         '-days', '30', '-subj', '/CN=Flight Enterprise Local Test CA',
         '-addext', 'basicConstraints=critical,CA:TRUE',
         '-addext', 'keyUsage=critical,keyCertSign,cRLSign',
         '-keyout', str(key), '-out', str(ca)], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    key.chmod(0o600)
    run(['openssl', 'req', '-new', '-newkey', 'rsa:2048', '-nodes', '-sha256',
         '-subj', '/CN=localhost', '-keyout', str(STATE / 'proxy.key'),
         '-out', str(STATE / 'proxy.csr')], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    (STATE / 'proxy.key').chmod(0o600)
    (STATE / 'proxy.ext').write_text(
        'subjectAltName=DNS:localhost,DNS:redis-enterprise,IP:127.0.0.1\n'
        'basicConstraints=critical,CA:FALSE\nkeyUsage=critical,digitalSignature,keyEncipherment\n'
        'extendedKeyUsage=serverAuth\nsubjectKeyIdentifier=hash\nauthorityKeyIdentifier=keyid,issuer\n')
    run(['openssl', 'x509', '-req', '-in', str(STATE / 'proxy.csr'), '-CA', str(ca),
         '-CAkey', str(key), '-CAcreateserial', '-out', str(STATE / 'proxy.crt'),
         '-days', '30', '-sha256', '-extfile', str(STATE / 'proxy.ext')],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    marker.write_text('2')


class Management:
    def __init__(self, cfg):
        self.cfg = cfg
        # Pin the API certificate obtained through the authenticated Docker
        # control channel. Its generated hostname differs from localhost.
        # CA validation stays enabled; this context is never used for data TLS.
        self.load_context()

    def load_context(self):
        self.context = ssl.create_default_context(cafile=str(STATE / 'api.crt'))
        self.context.load_verify_locations(cafile=str(RUNTIME / 'ca.crt'))
        self.context.check_hostname = False

    def request(self, path, body=None, method=None):
        token = base64.b64encode((self.cfg['admin_user'] + ':' +
                                 self.cfg['admin_password']).encode()).decode()
        req = urllib.request.Request(API + path,
            data=None if body is None else json.dumps(body).encode(),
            headers={'Authorization': 'Basic ' + token, 'Content-Type': 'application/json'},
            method=method)
        try:
            with urllib.request.urlopen(req, context=self.context, timeout=20) as response:
                body = response.read()
                return json.loads(body) if body else {}
        except urllib.error.HTTPError as exc:
            # Surface API validation reasons, never request bodies or passwords.
            try:
                error = json.loads(exc.read())
                detail = json.dumps({k: error[k] for k in
                    ('error_code', 'description', 'message', 'error') if k in error})
            except (ValueError, TypeError):
                detail = 'no JSON validation reason'
            for key in ('password', 'admin_password', 'default_password'):
                detail = detail.replace(self.cfg[key], '[redacted]')
            raise RuntimeError('Management request %s %s failed: HTTP %d %s' %
                               (req.get_method(), path, exc.code, detail[:500])) from None
        except urllib.error.URLError as exc:
            if not isinstance(exc.reason, ssl.SSLCertVerificationError):
                raise
            # Cluster creation replaces the image's initial API certificate.
            # Re-pin only through Docker, never from an unverified network peer.
            cert = dc('exec', '-T', 'redis-enterprise', 'cat',
                      '/etc/opt/redislabs/api_cert.pem', stdout=subprocess.PIPE,
                      stderr=subprocess.DEVNULL).stdout
            if cert == (STATE / 'api.crt').read_bytes():
                raise
            (STATE / 'api.crt').write_bytes(cert)
            self.load_context()
            return self.request(path, body, method)


def resp_read(stream):
    line = stream.readline()
    if not line:
        raise ConnectionError('Redis closed the connection')
    kind, value = line[:1], line[1:-2]
    if kind == b'-':
        raise RuntimeError(value.decode(errors='replace'))
    if kind == b'+': return value.decode()
    if kind == b':': return int(value)
    if kind == b'$':
        n = int(value)
        if n == -1: return None
        data = stream.read(n)
        if stream.read(2) != b'\r\n': raise RuntimeError('Invalid RESP framing')
        return data.decode()
    if kind == b'*': return [resp_read(stream) for _ in range(int(value))]
    raise RuntimeError('Unsupported Redis response')


class Redis:
    def __init__(self, username=None, password=None, trusted=True, tls=True):
        sock = socket.create_connection(('127.0.0.1', 16390), timeout=5)
        if tls:
            context = ssl.create_default_context(cafile=str(RUNTIME / 'ca.crt') if trusted else None)
            try:
                sock = context.wrap_socket(sock, server_hostname='localhost')
            except BaseException:
                sock.close()
                raise
        self.socket = sock
        self.stream = sock.makefile('rb')
        if username is not None:
            try: self.command('AUTH', username, password)
            except BaseException:
                self.close()
                raise

    def command(self, *args):
        parts = [str(a).encode() for a in args]
        payload = ('*%d\r\n' % len(parts)).encode()
        for part in parts:
            payload += ('$%d\r\n' % len(part)).encode() + part + b'\r\n'
        self.socket.sendall(payload)
        return resp_read(self.stream)

    def close(self):
        self.stream.close()
        self.socket.close()


def verify(cfg, api):
    bdb = api.request('/bdbs/' + str(cfg['database_uid']))
    if not (bdb['tls_mode'] == 'enabled' and bdb['shards_count'] == 3
            and bdb.get('default_user') is False
            and bdb.get('query_performance_factor', {}).get('active') is True
            and bdb['query_performance_factor']['scaling_factor'] == 4):
        raise RuntimeError('Database configuration does not match TLS/ACL/3-shard/4x requirements')
    shards = api.request('/bdbs/%s/shards' % cfg['database_uid'])
    if len([s for s in shards if s['role'] == 'master' and s['status'] == 'active']) != 3:
        raise RuntimeError('Three active primary shards are required')
    redis = Redis(cfg['username'], cfg['password'])
    try:
        if redis.command('PING') != 'PONG': raise RuntimeError('PING failed')
        if redis.command('ACL', 'WHOAMI') != cfg['username']: raise RuntimeError('Unexpected ACL identity')
        redis.command('FT._LIST')
        # Exercise Search reads and writes with unique fixture names, then clean up.
        name = 'it_flight_env_' + secrets.token_hex(6)
        key = name + ':row'
        redis.command('FT.CREATE', name, 'ON', 'HASH', 'PREFIX', 1, name + ':',
                      'SCHEMA', 'value', 'NUMERIC', 'SORTABLE')
        try:
            redis.command('HSET', key, 'value', 42)
            retry('Search write visibility', lambda: redis.command('FT.SEARCH', name, '@value:[42 42]')[0] == 1, 30)
        finally:
            redis.command('FT.DROPINDEX', name, 'DD')
    finally:
        redis.close()
    for label, kwargs in [
        ('unauthenticated TLS', {}),
        ('wrong password', {'username': cfg['username'], 'password': 'incorrect'}),
        ('untrusted certificate', {'trusted': False}),
        ('plaintext', {'tls': False}),
    ]:
        try:
            bad = Redis(**kwargs)
            try: bad.command('PING')
            finally: bad.close()
        except (RuntimeError, OSError):
            pass
        else:
            raise RuntimeError(label + ' unexpectedly succeeded')
    evidence = {'tls': 'verified', 'username': cfg['username'], 'shards': 3,
                'qpf': bdb['query_performance_factor'], 'redis_version': bdb.get('version'),
                'search_read_write': 'passed', 'negative_connection_checks': 'passed',
                'topology': 'single Software node, three primary shards; no replicas'}
    (STATE / 'verification.json').write_text(json.dumps(evidence, indent=2) + '\n')
    print('Verified Redis Enterprise: 3 primary shards, active 4x QPF, TLS, ACL identity, Search reads/writes and rejected insecure connections.', flush=True)


def up(cfg):
    certificates()
    paths = compose_env()
    (STATE / 'paths.json').write_text(json.dumps({k:paths[k] for k in ('ENTERPRISE_FLIGHT_SOURCE','ENTERPRISE_FLIGHT_COMPOSITION','ENTERPRISE_GO_MODULE_CACHE')}))
    if not (STATE / 'management.env').exists(): private_file(STATE / 'management.env', '')
    # Compose resolves env_file even when only the Redis service is selected.
    if not (STATE / 'flight.env').exists(): private_file(STATE / 'flight.env', '')
    dc('up', '-d', 'redis-enterprise')
    def get_api_cert():
        result = dc('exec', '-T', 'redis-enterprise', 'cat', '/etc/opt/redislabs/api_cert.pem',
                    stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
        (STATE / 'api.crt').write_bytes(result.stdout)
        return True
    retry('API certificate', get_api_cert)
    api = Management(cfg)
    requested = False
    def bootstrap():
        nonlocal requested
        status = api.request('/bootstrap').get('bootstrap_status', {}).get('state')
        if status == 'completed': return True
        if status == 'failed': raise RuntimeError('Redis Enterprise cluster bootstrap failed')
        if status == 'idle' and not requested:
            api.request('/bootstrap/create_cluster', {
                'action': 'create_cluster', 'cluster': {'name': 'flight-test.local'}, 'node': {},
                'credentials': {'username': cfg['admin_user'], 'password': cfg['admin_password']}})
            requested = True
        return False
    print('Waiting for isolated Redis Enterprise cluster bootstrap...', flush=True)
    retry('cluster bootstrap', bootstrap)
    api.request('/cluster/certificates', {'certificates': [{
        'name': name, 'certificate': (STATE / 'proxy.crt').read_text(),
        'key': (STATE / 'proxy.key').read_text()} for name in ('proxy','api')]}, 'PUT')
    api.load_context()
    api.context.check_hostname = True
    bdbs = api.request('/bdbs')
    bdb = next((b for b in bdbs if b['name'] == 'flight-integration'), None)
    if bdb is None:
        body = {'name': 'flight-integration', 'type': 'redis', 'redis_version': '8.6',
                'port': 12000, 'memory_size': 1073741824, 'replication': False,
                'data_persistence': 'disabled', 'eviction_policy': 'noeviction',
                'shards_count': 3, 'sharding': True, 'shards_placement': 'dense',
                'tls_mode': 'enabled', 'enforce_client_authentication': 'disabled',
                'authentication_redis_pass': cfg['default_password'], 'default_user': False,
                'query_performance_factor': {'active': True, 'scaling_factor': 4},
                'shard_key_regex': [{'regex': r'.*\{(?<tag>.*)\}.*'}, {'regex': r'(?<tag>.*)'}]}
        def create():
            try: return api.request('/bdbs', body)
            except RuntimeError as exc:
                if 'HTTP 406' in str(exc): return False
                raise
        bdb = retry('database creation', create)
    cfg['database_uid'] = bdb['uid']
    private_file(STATE / 'credentials.json', json.dumps(cfg))
    retry('active database', lambda: api.request('/bdbs/' + str(bdb['uid']))['status'] == 'active')
    def get_or_create(path, name, body):
        found = next((v for v in api.request(path) if v['name'] == name), None)
        return found if found is not None else api.request(path, dict(body, name=name))
    acl = get_or_create('/redis_acls', 'flight-test-all', {'acl': '+@all ~* &*'})
    role = get_or_create('/roles', 'flight-test-data', {'management': 'none'})
    api.request('/bdbs/' + str(bdb['uid']), {'roles_permissions': [{
        'role_uid': role['uid'], 'redis_acl_uid': acl['uid']}]}, 'PUT')
    users = api.request('/users')
    if not any(u.get('name') == cfg['username'] for u in users):
        api.request('/users', {'name': cfg['username'], 'email': 'flight-test@flight-test.local',
                    'password': cfg['password'], 'role_uids': [role['uid']]})
    retry('authenticated Search endpoint', lambda: data_ready(cfg))
    verify(cfg, api)
    private_file(STATE / 'flight.env', 'REDIS_URI=rediss://%s:%s@redis-enterprise:12000/0\n' %
                 (cfg['username'], cfg['password']))
    # Host clients that support an explicit CA can use this file; Go tests run
    # in Linux containers so the same CA is also trusted by Flight's own client.
    private_file(STATE / 'host.env', 'REDIS_URI=rediss://%s:%s@localhost:16390/0\nREDIS_CA_FILE=%s\n' %
                 (cfg['username'], cfg['password'], RUNTIME / 'ca.crt'))
    private_file(STATE / 'management.env', 'REDIS_ENTERPRISE_API=https://redis-enterprise:9443/v1\nREDIS_ENTERPRISE_ADMIN_USER=%s\nREDIS_ENTERPRISE_ADMIN_PASSWORD=%s\nREDIS_ENTERPRISE_CA_FILE=/runtime/ca.crt\nREDIS_ENTERPRISE_DATABASE=flight-integration\nREDIS_ENTERPRISE_SHARDS=3\n' % (cfg['admin_user'], cfg['admin_password']))
    build_env = compose_env()
    architecture = subprocess.check_output(['docker', 'info', '--format', '{{.Architecture}}'], text=True).strip()
    goarch = {'aarch64': 'arm64', 'arm64': 'arm64', 'x86_64': 'amd64', 'amd64': 'amd64'}.get(architecture)
    if goarch is None:
        raise RuntimeError('Unsupported Docker architecture: ' + architecture)
    build_env.update({'GOOS': 'linux', 'GOARCH': goarch, 'CGO_ENABLED': '0',
                      'GOCACHE': str(Path(tempfile.gettempdir()) / 'redis-flight-go-cache'),
                      'PYTHONDONTWRITEBYTECODE': '1'})
    print('Building qualified enterprise Flight for Linux ' + goarch + '...', flush=True)
    run(['python3', str(Path(build_env['ENTERPRISE_FLIGHT_COMPOSITION']) / 'build.py'),
         '--source', build_env['ENTERPRISE_FLIGHT_SOURCE'], '--', 'go', 'build', '-trimpath',
         '-o', str(RUNTIME / 'redis-flight-server'), './cmd/redis-flight-server'], env=build_env)
    provenance = {
        'qualified_manifest_sha256': hashlib.sha256((Path(build_env['ENTERPRISE_FLIGHT_COMPOSITION']) / 'overlay.json').read_bytes()).hexdigest(),
        'source': build_env['ENTERPRISE_FLIGHT_SOURCE'],
        'composition': build_env['ENTERPRISE_FLIGHT_COMPOSITION'], 'platform': 'linux/' + goarch,
    }
    (STATE / 'build.json').write_text(json.dumps(provenance, indent=2) + '\n')
    dc('up', '-d', '--force-recreate', 'flight')
    retry('enterprise Flight backend health', lambda: dc(
        'exec', '-T', 'flight', '/runtime/redis-flight-server', '-healthcheck',
        '-listen', '127.0.0.1:8815', stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL).returncode == 0)
    print('Flight endpoint: grpc://localhost:18816\nRedis TLS endpoint: localhost:16390\nLocal credentials: .local/enterprise/credentials.json (not printed)', flush=True)


def data_ready(cfg):
    try:
        redis = Redis(cfg['username'], cfg['password'])
        try: return redis.command('PING') == 'PONG'
        finally: redis.close()
    except RuntimeError:
        return False


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=['up', 'verify', 'test', 'down', 'status', 'smoke'])
    parser.add_argument('test_args', nargs=argparse.REMAINDER)
    args = parser.parse_args()
    cfg = config()
    if args.action == 'up': up(cfg)
    elif args.action == 'verify': verify(cfg, Management(cfg))
    elif args.action == 'status':
        api = Management(cfg)
        print('Bootstrap:', api.request('/bootstrap').get('bootstrap_status'))
        try:
            redis = Redis(cfg['username'], cfg['password'])
            try: print('Data endpoint:', redis.command('PING'))
            finally: redis.close()
        except (RuntimeError, OSError) as exc:
            print('Data endpoint not ready:', str(exc))
        print('Databases:', [{k: b.get(k) for k in ('name', 'status', 'shards_count', 'tls_mode', 'query_performance_factor')} for b in api.request('/bdbs')])
    elif args.action == 'down': dc('down')
    elif args.action == 'smoke':
        verify(cfg, Management(cfg))
        dc('run', '--rm', 'tests', 'go', 'run', '/checks/main.go')
    else:
        verify(cfg, Management(cfg))
        dc('run', '--rm', 'tests', *(args.test_args or ['go', 'test', '-count=1', '-p', '1', '-race', './...']))


if __name__ == '__main__':
    os.umask(0o077)
    try: main()
    except (RuntimeError, subprocess.CalledProcessError, OSError) as exc:
        # Requests and commands above contain no credential arguments. Keep
        # unexpected API bodies, TLS payloads and configuration out of logs.
        print('Environment setup failed: ' + str(exc), file=sys.stderr)
        sys.exit(1)
