# Implementation and evidence

1. Inspect issue/native contract, public sessions, identity scopes and pooling.
2. Pin released Redis driver v0.0.24 after checking latest release/default branch
   on 2026-11-11; update Go/container minimum to 1.27.3.
3. Implement owned session acquire/status/release and explicit attempt guard,
   terminal failure handling, dedicated reader connection and chunk validation.
4. Add Go regression/race tests, Python ADBC Flight SQL example and transport
   qualification suite; document protocol, native boundary and deployment gate.
5. Run formatting, vet, unit/race and build checks. Record actual results here.

Local evidence:
- `go test -race ./...`: passed with REDIS_URI/REDIS_INTEGRATION_REQUIRED unset.
  Initial sandbox attempt could not bind the existing SIGTERM test listener;
  rerun with local-listener permission passed. Backend integrations skipped.
- Final snapshot regression suite with `-race`: passed, including blocked-stream
  deadline cleanup and cancelled acquisition.
- `go vet ./...`, `gofmt -l .`, `git diff --check`: passed.
- `make build`: passed (static binary); Go printed a nonfatal read-only module
  stat-cache warning in the sandbox.
- Python example/test `py_compile`: passed; no Python transport runtime packages
  installed in the current interpreter.
- Real Redis/Python TLS integration not run: no disposable Redis Enterprise
  endpoint credentials were supplied. A private composition is no longer
  required for the minimum TLS/ACL baseline. Additional qualification cases are listed
  in the [contract](../../docs/shared-read-snapshots.md#qualification-gate).
- Review: local automated review only; maintainer decision pending.

Follow-up security scope (requester: "do the minimum"):
- Implemented verified Flight TLS, Redis TLS/custom CA, Redis ACL credential
  validation, encrypted/expiring bearer tokens and core feature availability.
- Recorded the explicit scope/packaging decision in [security.md](security.md).
- Security tests use real TLS Flight RPCs and a TLS RESP authentication fixture,
  with a native snapshot test double; full Software/Python qualification remains.
- Final security follow-up verification: full `go test -count=1 -race ./...`,
  `go vet ./...`, formatting, diff checks and static build passed. The full unit
  run used permission for ephemeral localhost listeners; backend tests skipped.
