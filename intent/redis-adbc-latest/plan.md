# Plan and evidence

Prepared 2026-10-20. Inputs: [intent.md](intent.md), [spec.md](spec.md).

1. Verify upstream revision, release and module version; review changes and existing GitHub issues.
2. Update go.mod/go.sum, agent guidance and dependency context.
3. Run unit tests, vet, build, standalone and cluster race tests; adapt only verified compatibility changes.
4. Create endpoint gap issues and record their URLs and validation outcomes here.

Validation and issue links are recorded below. Rollback: restore the previous driver pin/checksums and corresponding compatibility edits; no storage migration is planned.

## Implemented compatibility updates

- Updated the driver require/replacement and checksums; added the driver's regexp2 dependency through go mod tidy. Apache ADBC remains v1.12.0.
- Upstream now folds unquoted SQL identifiers to lowercase and infers small integer literals as INTEGER/Arrow int32. Updated exact expectations and added quoted-name and explicit BIGINT cases. The streaming test reader accepts int32 and int64 and rejects other types.
- Reviewed all seven additional SHOW ALL entries in upstream session.go: they have fixed initial values and no setter/parser, so no additional session scan state needs propagation.
- Added the always-check-latest rule to CLAUDE.md. The agentmemory lesson tool is unavailable in this session; the repository instruction is the durable record.
- Documented the private dependency download requirement and clean CI/Docker access limitation.

## GitHub issues created

- [Bound large SQL execution and define behavior when stream slots are exhausted](https://github.com/alberttwong/redis-apache-arrow-flight/issues/50)
- [Expose explicit Flight SQL query cancellation with lifecycle-safe cleanup](https://github.com/alberttwong/redis-apache-arrow-flight/issues/51)
- [Audit Flight SQL metadata and publish a tested capability matrix](https://github.com/alberttwong/redis-apache-arrow-flight/issues/52)
- [Decide and document DoExchange support for the Flight RPC endpoint](https://github.com/alberttwong/redis-apache-arrow-flight/issues/53)
- [Define a supported workflow between Flight datasets and SQL tables](https://github.com/alberttwong/redis-apache-arrow-flight/issues/54)
- [Expand reproducible Flight SQL and Flight RPC client compatibility coverage](https://github.com/alberttwong/redis-apache-arrow-flight/issues/55)
- [Define and verify session routing and restart behavior across server replicas](https://github.com/alberttwong/redis-apache-arrow-flight/issues/56)
- [Establish end-to-end Flight performance and deployment acceptance evidence](https://github.com/alberttwong/redis-apache-arrow-flight/issues/57)
- [Restore reproducible clean CI and Docker builds with the private Redis ADBC dependency](https://github.com/alberttwong/redis-apache-arrow-flight/issues/58)

## Validation results

All checks below passed on 2026-10-20 using Go 1.27.2 on macOS arm64 (go.mod requires at least 1.26.6):

- `env -u REDIS_URI go test ./...` (backend tests intentionally skip in this invocation).
- `go vet ./...` and `make build`.
- `REDIS_URI=redis://localhost:16379/0 go test -count=1 -race ./...`: both command and server packages passed; server suite 22.982s.
- `REDIS_URI=redis://localhost:7001/0 go test -count=1 -race ./...`: both command and server packages passed; server suite 21.751s.
- Formatting and `git diff --check`.

The first runs exposed the upstream typing, identifier and SHOW ALL changes described above; all corresponding failures were resolved before the final runs. Integration suites used disposable Redis 8.6.2 standalone and Cluster containers; the pre-existing Redis instance on port 6379 was not used.

Python/JDBC smoke checks were skipped because uv and a Java runtime are unavailable. Clean Docker/CI private-module access remains open in issue #58; local builds used authenticated dependency downloads. The user subsequently requested a PR; publication is recorded in GitHub. No release or deployment is part of this change.
