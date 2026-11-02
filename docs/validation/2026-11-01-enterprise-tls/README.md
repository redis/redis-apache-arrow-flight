# Local Redis Enterprise TLS environment validation

Date: 2026-11-01 (America/Los_Angeles). Local execution, not hosted CI.

Provisioned with `python3 scripts/enterprise-env.py up`, using the separately
qualified public/private source pair under `~/sandbox/work`. The builder's full
module fingerprint check remained enabled. This does not qualify this worktree's
newer public Go changes. The Linux ARM64 enterprise executable runs in Docker.

Environment: Redis Software image `redislabs/redis:8.2.0-78.18`, database Redis
8.6.2 with Search, one cluster node, three active primary shards, no replicas,
RAM storage, 1 GiB configured database memory, active 4x query performance factor.
This is functional coverage, not failover, multi-node or performance evidence.
All published ports are bound to loopback. Existing containers were left intact.

## Passed

- Provisioning and repeated `up` against the existing environment.
- `verify`: trusted CA and hostname validation, `AUTH` with the dedicated
  `flight_test` identity, `ACL WHOAMI`, `PING`, Search index creation, hash write,
  indexed readback and cleanup.
- Rejected unauthenticated TLS, wrong password, untrusted CA and plaintext Redis.
- Enterprise Flight backend health: `SERVING`.
- Focused qualified enterprise race suite:
  `TestBuildIntegrationRoundTrip`, `TestIntegrationSQL`,
  `TestIntegrationWritesRunOnce`, `TestIntegrationIngest`.
- `smoke`: SQL CREATE, INSERT, UPDATE and exact readback through the running
  Flight endpoint, followed by fixture cleanup.
- Python syntax compilation, Go vet for the smoke client, Go formatting and
  `git diff --check`.

## Full qualified suite failures

`python3 scripts/enterprise-env.py test` ran all packages with `-race` and failed.
No test assertions were removed or converted to skips:

- `TestIntegrationCancellationIdentity`, `TestIntegrationCancellationReadOnlyFallback`,
  `TestIntegrationStaleCredentials`, `TestIntegrationRedisACL` and
  `TestIntegrationSessionOwner` attempt `ACL SETUSER`. Redis Software rejects that
  command. Their fixture setup needs the Software management API with equivalent
  role/ACL assertions. Redis Software also does not implement `ACL DELUSER`.
- `TestIntegrationSIGTERM` and subtests of `TestIntegrationStreamResults` failed
  cursor/streaming expectations. Examples: no cursor after the first batch and
  285 cursors observed where a single-shard expectation was 96. These failures
  require investigation of the streaming path and shard-aware assertions; this
  record does not establish their root cause or treat them as passing.

The environment is usable for reproducing these failures. A working environment,
healthy Flight endpoint and focused successes do not constitute a green full
integration suite or release qualification.

## Reproduce

```sh
python3 scripts/enterprise-env.py verify
python3 scripts/enterprise-env.py smoke
python3 scripts/enterprise-env.py test
```

See [setup and lifecycle](../../redis-enterprise-testing.md). Passwords, private
keys and authenticated URIs are confined to ignored local state and are omitted
from this evidence. `.local/enterprise/verification.json` records the latest
nonsecret environment checks; `build.json` records the composition manifest hash.
