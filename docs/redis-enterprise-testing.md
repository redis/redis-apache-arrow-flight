# Local Redis Enterprise integration environment

Provision the isolated environment from this worktree:

```sh
python3 scripts/enterprise-env.py up
```

Requires Docker Compose, Python 3.9+, OpenSSL, Go 1.27.3 and a matching private
enterprise composition. The defaults use the qualified checkouts at
`~/sandbox/work/redis-apache-arrow-flight` and
`~/sandbox/work/redis-apache-arrow-flight-enterprise`. Override
`ENTERPRISE_FLIGHT_SOURCE` and `ENTERPRISE_FLIGHT_COMPOSITION` when necessary.
For the repaired suite, select the matching private composition branch and
export `ENTERPRISE_FLIGHT_SOURCE` as this public checkout. The private builder
verifies the complete source fingerprint before building or testing. Selected
paths are retained in ignored `.local/enterprise/paths.json` for later commands. This worktree's newer public changes are not silently substituted
for the qualified source pair.

The environment uses the locally cached `redislabs/redis:8.2.0-78.18` image and
Redis 8.6 with Search. It provisions one Software cluster node with three primary
shards, no replicas, 1 GiB of database capacity, and active 4x query performance
factor. This is a local functional test topology; it does not test node failover
or qualify throughput. Existing Redis containers are left alone.

| Endpoint | Address |
| --- | --- |
| Redis TLS | `localhost:16390` |
| Flight SQL / Arrow | `grpc://localhost:18816` |
| Enterprise metrics | `http://localhost:19816/metrics` |
| Management API | `https://localhost:29443` |

Published ports bind only to loopback. Redis requires verified TLS and the
`flight_test` username with a generated password. The default database user is
disabled. Flight's client-facing endpoint is local plaintext gRPC; its Redis
connection uses TLS and credentials. Enable Flight TLS separately if a client
validation case requires it.

Secrets, a 30-day local CA and private keys live under the ignored
`.local/enterprise/` directory. Credentials are not printed. The setup generates
separate management and data credentials, creates a database-only role, and
installs a proxy certificate valid for localhost and the Docker service name.
The test role grants all data commands/keys in this disposable database for the
integration suite; it has no cluster-management role. Separate generated admin
credentials are mounted only into the test runner for management API ACL fixtures. The management client pins
the API certificate read through Docker because the generated API hostname
differs from localhost. Data clients verify both the CA and hostname.

Flight and the Go test runner run in Linux containers with this CA supplied
through `SSL_CERT_FILE`. No macOS keychain or global trust store is modified.
Native macOS clients may need an explicit CA option; `.local/enterprise/host.env`
contains the host Redis URI and CA path. Do not print or commit this file.

Verify the environment, including Search read/write and rejection of plaintext,
no credentials, wrong passwords and untrusted certificates:

```sh
python3 scripts/enterprise-env.py verify
```

Check real SQL writes through the running Flight endpoint:

```sh
python3 scripts/enterprise-env.py smoke
```

This creates a unique table, inserts and updates one row, reads back the exact
values, and removes the fixture. It exercises the running Flight container.

Run the qualified enterprise integration suite:

```sh
python3 scripts/enterprise-env.py test
```

Pass a focused command as needed:

```sh
python3 scripts/enterprise-env.py test go test -count=1 -race ./server \
  -run '^(TestBuildIntegrationRoundTrip|TestIntegrationSQL|TestIntegrationWritesRunOnce|TestIntegrationIngest)$'
```

The runner mounts source and the enterprise composition read-only and uses an
isolated Go build cache. Its Redis endpoint is the TLS-only Enterprise database.
The qualified pair includes its own original test sources; this does not
requalify newer source changes in this worktree. Unsupported Redis Software
commands or failed assertions remain failures and must be reported separately
from environment readiness.

Stop services while retaining test database state:

```sh
python3 scripts/enterprise-env.py down
```

`up` can restart and verify the retained environment. Certificates expire after
30 days; `up` renews the local CA and proxy certificate when less than one day
remains and restarts Flight. Re-export the CA for any external clients after
renewal. To reset intentionally, stop the environment, remove its Compose data
volume and ignored local state, then rerun `up`; do not delete state while the
services are running. This environment uses Redis Software's bundled trial;
trial expiry is an environment blocker, not permission to switch to plaintext
or OSS Redis.

Configuration follows the Redis references for
[TLS databases](https://redis.io/docs/latest/operate/rs/security/encryption/tls/enable-tls/),
[database-only roles](https://redis.io/docs/latest/operate/rs/security/access-control/create-db-roles/),
[cluster certificates](https://redis.io/docs/latest/operate/rs/references/rest-api/requests/cluster/certificates/),
and [query performance factor](https://redis.io/docs/latest/operate/oss_and_stack/stack-with-enterprise/search/query-performance-factor/).

## Current validation

The environment, backend security checks, live Flight SQL smoke and the full
Enterprise race suite passed with the repaired private composition. Original
Software ACL fixture and streaming/cursor failures are retained as historical
evidence alongside their fixes. See the
[validation record](validation/2026-11-01-enterprise-tls/README.md) for exact
coverage and failures; no full-suite pass is claimed.

## Release qualification

The public release workflow dispatches the private `enterprise-qualification.yml`
workflow in `Redislabs-Solution-Architects/redis-apache-arrow-flight-enterprise`.
The private manifest must bind the exact public commit before that source's
scripts or tests execute. The workflow verifies the fingerprint, provisions an
isolated TLS/ACL Enterprise database, runs the complete race suite and live SQL
smoke, and uploads a small qualification receipt.

The public gate rejects missing credentials, unqualified commits, failed or stale
runs, missing/expired artifacts, and receipt mismatches. Binaries and images are
built from the verified immutable SHA; publication cannot proceed on gate
failure. PR artifact checks do not publish and do not claim release qualification.

`ENTERPRISE_CI_TOKEN` is stored as a public-repository Actions secret, scoped to
Actions read/write and Contents read on the single private repository. Its
organization access currently awaits owner approval; it expires 2026-12-01 and
must be rotated before then. The existing broadly scoped CLI login is never
stored as a workflow credential.
