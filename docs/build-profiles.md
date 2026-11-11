# Community and enterprise build profiles

This checkout builds **community** with basic verified TLS and Redis ACL
authentication. The minimum security baseline was added at the requester's
explicit direction for shared snapshots (2026-11-11). Monitoring, tuning and
automation retain their existing enterprise composition boundaries.

The community profile resolves only public dependencies and contains community
implementations at the paid-feature boundaries. Enterprise builds compose the
complete paid implementation source with the same qualified core. This is compile-time composition, not a runtime
plugin, a Flight requirement for native ADBC, or a network license check.

| Feature | Community | Enterprise |
|---|---|---|
| SQL/ADBC/Arrow or Flight protocol | Correct basic execution, types, errors and cleanup | Same APIs plus implemented paid capabilities |
| Monitoring | No diagnostics file, empty optional metrics; Flight metrics HTTP 204 and health UNKNOWN | Existing diagnostics, metrics, backend health and request logging |
| Optimization | Full driver SQL execution; basic Flight connections and one dataset endpoint | Full driver SQL execution plus Flight pooling and accelerated streaming as implemented |
| Automation | Full driver automation; basic Flight protocol/shard routing | Full driver automation plus existing Flight management automation |
| Basic security | Verified Flight TLS, Redis TLS/custom CA, Redis ACL login and encrypted expiring tokens | Same minimum contract; extra security capabilities require their own review |
| Backup, Active-Active adaptation and infrastructure migration | Unavailable | Not yet implemented; this split does not implement the roadmap |

Both profiles reject insecure TLS verification and plaintext credential transport.
Redis ACL login requires Flight TLS and a verified rediss backend. A custom Redis
trust bundle uses the same `adbc.redis.tls_ca_file` option in gateway and native
clients. No enterprise composition is required for this basic security baseline.
Basic operational errors/startup/shutdown logs
and SQL/storage/Arrow integrity and cleanup remain core behavior; optional
monitoring and automation cannot remove those invariants.

Go callers can inspect `server.FeatureCapabilities()`. Community health UNKNOWN
must not be interpreted as Redis readiness. Dataset reads still return all rows
through their single basic endpoint. Both Flight profiles use the same full public
ADBC driver. The commercial rule applies only to Flight-owned implementations;
driver monitoring, optimization, automation and security remain in the driver.

## Build and qualification

Existing `make build` and `go test` commands build/test community. Enterprise
composition is supplied separately and uses Go's `-overlay` source-file mapping.
The package symbol/type contracts stay in the core; the enterprise build replaces
those compile-time implementation boundaries and restores their original tests.
A manifest fingerprints the full qualified Go/C module source and rejects changed,
added or missing input before running the command. No private imports, URLs or
repository access are required by community `go.mod` or builds.

This source-composition choice preserves package-private engine types without
exporting storage internals or introducing circular Go modules. It is deliberately
source-revision-coupled: core changes need a newly qualified composition manifest.
It is not binary plugin ABI compatibility or license enforcement. Keep all original
copyright/license notices. The split does not revoke existing licenses/history.

Test both profiles. Tests that assert paid plans, telemetry or automation live
with those implementations, with their assertions intact. Basic TLS/ACL tests
are public alongside their core implementation. Core result/lifecycle tests and explicit stub/fail-closed tests remain
public. Preserve exact Arrow values, errors, cleanup, and cancellation. Software
integration requires verified active 4x QPF; no Active-Active or benchmark
qualification follows from a successful build or unit pass.

## Startup and readiness

Compose waits for Redis health before starting Flight. It does not establish
Flight readiness; do not use `--wait` with the community service, which has no
backend healthcheck. Community health reports `UNKNOWN`; verify application
readiness with a real SQL query, as `scripts/wait-flight.py` does in CI.

## CI and integration qualification

Public CI runs formatting, vet, community unit tests and a static binary build.
All integration suites and client validation require a disposable Redis
Enterprise cluster with Search, verified TLS and username/password authentication.
Core TLS/authentication and snapshot ownership tests run in the community unit
suite with generated certificates and a TLS RESP test fixture. Full SQL/native
snapshot qualification still requires a disposable authenticated TLS Software
endpoint. Enterprise feature assertions additionally require their qualified source
composition. `make test-integration` and its `make test-cluster` alias require an
exported authenticated `rediss://` `REDIS_URI`; they never provision plaintext Redis.

See [testing and recommended private CI setup](sdlc/testing.md) for provisioning,
secret management and the outstanding external qualification gate. Public CI
success and release artifact version/help checks are not integration evidence.
The local Compose configuration remains available for community development.
