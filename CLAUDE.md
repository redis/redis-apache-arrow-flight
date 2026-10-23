# Redis Arrow Flight server

Go service exposing Flight SQL and named Arrow datasets on one gRPC endpoint.
Read [the SDLC index](docs/sdlc/README.md) and the change's intent, spec, and plan.

## Commands

- Build: `make build` → `bin/redis-flight-server`; requires Go from `go.mod` (1.26.10).
- Unit tests: `env -u REDIS_URI make test`; Redis integration tests skip.
- Integration: `make test-integration`; cluster: `make test-cluster`. Both start Docker Redis and run with `-race`.
- Format check: `gofmt -l .` (empty output); static checks: `go vet ./...`.
- Client smoke: `make example` and `make jdbc-smoke`, with Redis and the server running.
- No `make lint` target exists. See [testing](docs/sdlc/testing.md) for prerequisites and CI coverage.

## Architecture and conventions

- `cmd/redis-flight-server/`: flags/environment, signals, health probe.
- `server/router.go`: PATH descriptors/JSON tickets → datasets; CMD/protobuf → Flight SQL.
- `server/sqlserver.go`, `metadata.go`: SQL through the Redis ADBC driver; preserve the pinned fork replacement in `go.mod` unless dependency changes are in scope.
- Before updating the Redis ADBC driver, always check upstream for the latest release and default-branch commit. Verify the Go submodule version against upstream rather than trusting a potentially stale `@latest` lookup; pin the selected immutable revision and record the check date and validation results.
- `server/datasets.go`, `ipc.go`: generation-based Arrow IPC storage; preserve cluster hash tags, monotonic sequence keys, atomic publication, and legacy LIST reads.
- `server/tenant.go`, `pool.go`, `session.go`: identity-scoped resources and session state. Release Arrow references, connections, cursors, and reservations on error, cancellation, and panic.
- `server/config.go` and command flags must stay consistent with README configuration.

## Constraints to preserve

- SQL needs Redis Search; plain datasets do not. SQL tables and datasets are separate interfaces.
- Reads and writes have different execution timing; do not make ticket replay execute writes again.
- Auth defaults to `none`. ACL auth uses each client's Redis identity; sealed tokens contain encrypted credentials. Keep secrets out of logs, help output, and committed artifacts.
- Health intentionally bypasses auth/sessions. Metrics are a separate, optional, unauthenticated HTTP listener.
- The fallback result budget is not a cap on all process memory. Batch/page sizes are approximate, not strict limits for oversized rows.
- Dataset HASH writes are incompatible with older LIST-only readers. Review upgrade and rollback implications.

## Completion

Follow [REVIEW.md](REVIEW.md). Run checks appropriate to the change and report commands, results, skips, and limitations. Use a disposable Redis for integration tests and examples. Update affected docs with behavior changes. This file is advisory; no agent hooks or eval runner are configured by this documentation.

## Community and enterprise split

Default builds use community implementations; read [build profiles](docs/build-profiles.md).
Do not reintroduce paid implementations into public source or add private imports
to its module. Preserve core SQL/protocol/storage/resource safety and fail closed
when a requested security feature is absent. Paid tests belong with their original
implementations; do not weaken assertions to make the community profile pass.
Enterprise source composition must match the qualified full-module fingerprint.

The commercial feature rule applies only to Flight-owned implementations. Both
Flight profiles use the same full public ADBC driver, including its monitoring,
optimization, automation and security capabilities. No driver enterprise package
or separate driver composition is required.
