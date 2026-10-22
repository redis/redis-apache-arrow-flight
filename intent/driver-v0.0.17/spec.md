# Release pin and compatibility

Checked upstream on 2026-10-22. Input: [intent.md](intent.md).

- Release: [v0.0.17](https://github.com/alberttwong/redis-adbc-driver/releases/tag/v0.0.17).
  Its immutable source commit is `9cc8e25c0f0149b19d2d76446fa45a280317c8d4`.
- Go submodule resolution of that SHA yields
  `v0.0.0-20261022161448-9cc8e25c0f01`; the root repository release tag is not
  a `go/v0.0.17` module tag. Use the resolved version in both require and replace.
- Also checked the default branch: `6af99eb06a79a2d879af36470be1ec6377327327`.
  Select the user-requested release, not subsequent unreleased changes.
- Resolve the driver's newer driverbase and generated protobuf modules, and
  replace regexp2 v1.12.0 with upstream's regexp2/v2 v2.9.1. No local forks or
  cross-major module overrides are required.
- OpenTelemetry 1.47.0 still imports backoff/v5. Keep that supported transitive
  version: an isolated override to backoff/v7 passed application unit tests but
  failed `go mod tidy` with a module-used-for-two-paths error. This upgrade does
  not claim every transitive module is on the latest major.
- Validate unit/race tests, standalone and cluster integration/race tests, vet,
  build, module checksums, Go vulnerability scanning and Python client smoke
  tests on Go 1.26.10. Preserve historical client evidence and report skips.
