# Plan and validation

Checked 2026-11-11 against baseline `1461cd11253a4d73c1723eb7fc63b84feaec3f12`.

1. Queried upstream GitHub latest release, release tag, default branch HEAD, and release Go module metadata.
2. Updated the replacement/checksums, Go minimum and container pins, dependency documentation, and agent guidance.
3. Ran race-enabled unit tests, vet, static build, and whitespace checks.

## Upstream evidence

- Latest stable release: [v0.0.24](https://github.com/redis/redis-adbc-driver/releases/tag/v0.0.24), released 2026-11-10.
- Tag commit: `b052eedad9f2fb2c6eeb7c79fe63f8bbc3ca6bdd`.
- Default branch HEAD observed: `766dded08cc3728edfd092f5e4941e95932bfc9a`; not selected because the user requested releases.
- `go list -m -json github.com/redis/redis-adbc-driver/go@b052eedad9f2fb2c6eeb7c79fe63f8bbc3ca6bdd` resolved `v0.0.0-20261110200608-b052eedad9f2`.
- Release `go/go.mod` requires Go 1.27.3; no other dependency versions changed after `go mod tidy`.

## Results and limits

PASS on macOS arm64, Go 1.27.3:

- `env -u REDIS_URI -u REDIS_INTEGRATION_REQUIRED go test -count=1 -race ./...`
- `go vet ./...`
- `make build` (CGO disabled)
- `git diff --check`

Backend integration tests skip without an authenticated TLS disposable Redis and qualified enterprise composition. Docker builds and client smoke tests were not run. These results do not establish SQL/storage integration compatibility.

The release uses SQL storage protocol 17 and rejects legacy storage; it does not upgrade existing SQL storage automatically. Validate against fresh supported storage and plan any existing SQL data migration before deployment. Flight dataset storage is a separate interface. Reverting the dependency/build pins is a source rollback, not evidence of SQL data compatibility after new-driver writes.

The agentmemory daemon was unavailable; the persistent preference is recorded in `CLAUDE.md`. No release, deployment, migration or maintainer approval occurred.
