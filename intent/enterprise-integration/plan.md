# Implementation and validation

1. Add shared URI validation to server and command integration setup.
2. Require explicit Enterprise backend configuration in Make targets.
3. Remove plaintext integration execution from public workflows and document
   the private enterprise workflow recommendation and qualification gap.
4. Update current testing/client setup guidance; retain historical evidence.

Validation results are recorded after local checks below. Enterprise backend
execution is unavailable without the separately qualified source composition,
cluster endpoint and credentials. No enterprise pass is claimed.

Local results:
- `make test`: passed (backend tests intentionally skipped).
- `env -u REDIS_URI -u REDIS_INTEGRATION_REQUIRED go test -count=1 -race ./...`:
  passed for all three packages; backend tests intentionally skipped.
- `go test -race ./internal/integrationtest`: passed, including rejection of
  plaintext, missing credentials and `skip_verify=true`, with redacted errors.
- `go vet ./...`, `gofmt -l .`, `git diff --check`: passed.
- `make build`: passed.
- `env -u REDIS_URI make test-integration`: failed as expected before invoking Go.

Tests used a writable temporary GOCACHE. The command lifecycle unit test needed
localhost binding outside the sandbox. Enterprise tests were not run. No private
workflow, required check, secret, cluster or release gate was provisioned.
Actual reviewer decision and external integration result remain unrecorded.


Follow-up requested by the user: provision the local test environment. Added
`compose.enterprise.yaml`, the provisioning/verification runner and a live Flight
SQL smoke client. Located the separately qualified source pair under
`~/sandbox/work`; its fingerprint passes. The environment runs this pair, not
newer public Go sources from this worktree. See
[actual TLS environment evidence](../../docs/validation/2026-11-01-enterprise-tls/README.md)
for successful security/read-write checks and full-suite failures. Private hosted
CI and its release gate remain unconfigured.


User follow-up: repair the full Enterprise suite and private CI release gate.
Implemented Software management-API ACL fixtures, propagation checks and cleanup;
fixed Software command-denial status mapping; repaired stream/shutdown oracles
for bounded driver range scans while preserving row/lifecycle assertions; and
added per-shard cursor reservation coverage in the private composition. The full
Enterprise race suite passed. The release gate binds successful private workflow
receipts to both immutable revisions and the manifest, and blocks binary/image
publication on failure. Seven gate tests and workflow lint passed.

With explicit user approval, created/stored a narrowly scoped dispatch token.
Organization approval is pending; no hosted qualification or merge is claimed.
