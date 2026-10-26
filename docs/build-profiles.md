# Community and enterprise build profiles

The default public checkout builds the community profile. It resolves only
public dependencies and contains community implementations at the paid-feature
boundaries. Enterprise builds compose the complete paid implementation source
with the same qualified core. This is compile-time composition, not a runtime
plugin, a Flight requirement for native ADBC, or a network license check.

| Feature | Community | Enterprise |
|---|---|---|
| SQL/ADBC/Arrow or Flight protocol | Correct basic execution, types, errors and cleanup | Same APIs plus implemented paid capabilities |
| Monitoring | No diagnostics file, empty optional metrics; Flight metrics HTTP 204 and health UNKNOWN | Existing diagnostics, metrics, backend health and request logging |
| Optimization | Full driver SQL execution; basic Flight connections and one dataset endpoint | Full driver SQL execution plus Flight pooling and accelerated streaming as implemented |
| Automation | Full driver automation; basic Flight protocol/shard routing | Full driver automation plus existing Flight management automation |
| Security | Requested TLS/authentication/security operations fail explicitly before execution | Existing verified TLS/custom CA, Redis credentials/ACL integration and Flight authentication |
| Backup, Active-Active adaptation and infrastructure migration | Unavailable | Not yet implemented; this split does not implement the roadmap |

Community security stubs do not authenticate credentials, accept certificates,
skip verification or downgrade a secure connection to plaintext. Use enterprise
for TLS or authenticated Redis. Basic operational errors/startup/shutdown logs
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

Test both profiles. Tests that assert paid plans, telemetry, authenticated
connections or automation live with those implementations, with their assertions
intact. Core result/lifecycle tests and explicit stub/fail-closed tests remain
public. Preserve exact Arrow values, errors, cleanup, and cancellation. Software
integration requires verified active 4x QPF; no Active-Active or benchmark
qualification follows from a successful build or unit pass.

## Community CI

CI runs core integration tests against standalone Redis and a three-shard
cluster. Dataset tests require all rows through one community endpoint and keep
the enterprise endpoint-splitting assertions when optimization is available.
Stream-admission rejection is enterprise-only; community separately checks that
buffered queries preserve SQL errors, complete all rows and release cursors.
Cancellation and shutdown tests use results large enough to keep the transport
active even when SQL results have already been buffered.

The community Compose service has no backend health probe, matching its image.
`docker compose up --wait` waits for Redis health and the Flight container to run;
it does not establish Flight readiness. CI then runs `scripts/wait-flight.py`
through the pinned Python smoke environment to verify a real `SELECT 1` round
trip before the examples. The metrics check requires HTTP 204 with an empty body.
Release binary verification requires the documented health UNKNOWN response,
while the reusable test workflow exercises actual SQL and dataset behavior.
