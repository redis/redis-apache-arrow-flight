# Specification

- Implement CancelFlightInfo (and SDK translation of legacy CancelQuery) for
  read-only query tickets using shared, leased Redis control records. Apply
  authenticated-principal ownership, pre-fetch/active/repeated cancellation,
  expiry and completion semantics. Close cursors, readers, pool/session/tenant
  holds on completion, transport loss, cancellation or errors. Writes are never
  rolled back; completed/write/legacy/non-writable-control tickets are not cancellable.
- Preserve SQL for read-only ACL users. Registration is atomic with expiry and
  denied control writes yield a normal non-cancellable ticket. Store no passwords,
  SQL or bound values in control records.
- Reject unknown session cookies before executing with ABORTED and a fresh cookie.
  Reserve bounded, short-lived issuance metadata to distinguish first confirmation
  from wrong-replica/restart/expired state. Do not open connections for issuance.
- Add opt-in stream-overflow=reject, preserving default buffer behavior. Reject
  eligible scans before execution when per-user/table/index capacity is unavailable.
  Do not claim this bounds all backend execution memory.
- Demonstrate dataset projection -> SQL ingestion using standard clients and
  explicit unsupported nested-type behavior, unique fixtures and cleanup.
- Pin client smoke versions; publish observed versus unvalidated application
  compatibility, including JDBC 19 getTypeInfo and #23 limitations.
- Provide reproducible synthetic transfer/query/concurrency/memory measurements
  and local failure/recovery/rollback evidence. Keep production targets unresolved
  without representative workload/topology/ownership evidence.

Rollout: new session semantics require reconnect/recovery; old replicas do not
honor new cancellation records. Upgrade ticket readers together or retain routing
to upgraded replicas. Reconnect and discard outstanding tickets on rollback.
