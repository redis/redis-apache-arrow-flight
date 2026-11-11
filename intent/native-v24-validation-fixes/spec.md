# Specification

- #68: add read-only, protocol-17 resource inspection for tests. Resolve a table's
  current immutable metadata through the native root/bucket/value indirection in
  one Redis script. Report primary and retained-segment indexes. Fail explicitly
  on unsupported formats, broken metadata and unavailable cursor statistics.
- #69: assert all three native table types exactly. Create a materialized view
  and verify typed discovery and serialized schema through Flight SQL.
- #70: discover the actual session temporary schema/index and managed scan
  indexes. After SIGTERM, assert zero cursors, logical temporary-object removal,
  physical temporary-index reclamation after normal native sweep, truncated
  unread results, and successful completion/read-back of the concurrent upload.
- #71: the supported backend target uses `go test -p 1`. A POSIX advisory lock
  serializes cooperating suite commands sharing the same scope and lock directory.
  Redis aliases/Flight clients must explicitly share FLIGHT_INTEGRATION_SCOPE;
  separate runner hosts must use separate disposable databases. Preserve command
  exit status and hold the lock through child termination. Never retry writes.

Inspection is for trusted disposable integration environments only. It neither
exports attachment handles nor bypasses native client authorization. Enterprise
composition qualification and independent Airbyte checks remain separate gates.
