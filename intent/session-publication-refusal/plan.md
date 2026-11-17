# Implementation and evidence

1. Read issue #74, session/pool lifecycle and native publication cleanup.
2. Verify and pin latest driver main at the requester's instruction.
3. Add the exact native refusal exception, preserving status and execution count.
4. Reproduce the old failure, run negative/race tests, and exercise native
   publication refusal through Flight with real SQL state.
5. Document source-bound reasoning and actual validation limits.

## Regression evidence

Before the fix, with the new driver pin, focused Go regressions failed:
`TestPublicationRefusalClassification/native_refusal` returned false and all
four `TestSessionPublicationRefusal` workers observed `conn=nil`, `temp=false`
after the IO/40001. The preceding NotFound controls kept their connections.
After the fix, the same focused suite passed, including the shared snapshot
terminal-state check.

`TestIntegrationSessionPublicationRefusal` uses a verified TLS proxy in front of
disposable authenticated Redis Enterprise. It replaces only the exact native
catalog `publish` operations for a single workspace with native `seal`, returning
integer zero after successful sealing, including metadata-only rebases. Sealing
is the native operation's pre-CAS side effect: omitting it creates an impossible
schedule and causes a merge-lease error instead of the intended refusal.
The real pinned driver emits IO/40001; no synthetic ADBC error replaces
execution. Setup creates real SQL timezone/schema settings and
a populated temporary table. Assertions cover NotFound, native refusal, fresh
statement retry, unchanged connection/cookie, script-prefix readback, and failed
Arrow-ingest readback followed by a new input reader. This forces a definite
pre-commit refusal, not an ambiguous lost reply.

## Validation

- Full unit/race suite: `env -u REDIS_URI -u REDIS_INTEGRATION_REQUIRED go test -race ./...`
  passed; backend tests skipped in that run.
- `go vet ./...`, `make build`, `git diff --check`: passed.
- Native backend regression with `-race`: passed on macOS arm64, Go 1.27.3,
  latest driver `640dbf1fa880`. The three failure scenarios each forced nine
  native CAS refusals (initial publication plus eight metadata-only rebases).
  After every refusal and explicit fresh retry, SQL settings and temporary
  rows matched expectations. Script and Arrow readbacks contained no duplicates.
- Restarted disposable fixture verification passed: three Enterprise shards,
  active QPF 4x, Search, trusted TLS, named-user authentication, write/read/delete,
  and rejection of plaintext, untrusted certificates and invalid credentials.
- A second native run using a Go overlay to remove only the new classification
  reproduced the original defect after nine CAS refusals: `SHOW TIME ZONE`
  returned `UTC`, `SHOW search_path` returned `public`, and the existing temp
  table returned NotFound. The preceding NotFound control passed. This directly
  establishes the server mechanism on latest driver code, independently of the
  original uninstrumented downstream trace.
- Initial backend attempts found the prior issue51 disposable fixture stopped;
  it was restarted for this regression. No production endpoint was used.
- Original downstream Rust audit and the full private enterprise composition
  suite are separate qualification; no pass claimed here.
- Reviewer/maintainer decision: not recorded. No deployment or release.

Backend regression command (export disposable authenticated `REDIS_URI` and,
for a private CA, `REDIS_TLS_CA_FILE`; never log either credentials or the URI):

```sh
REDIS_INTEGRATION_REQUIRED=1 python3 scripts/run-integration.py -- \
  go test -race ./server -run '^TestIntegrationSessionPublicationRefusal$' \
  -count=1 -v -timeout=3m
```
