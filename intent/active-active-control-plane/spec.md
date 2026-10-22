# Specification

Date: 2026-10-22. Status: proposed. Refs [intent](intent.md), [plan](plan.md),
[evidence](evidence.md), [architecture](../../docs/active-active-control-plane.md).

- Flight owns the optional management plane; no Flight dependency for native
  bootstrap, SQL/Arrow, identities, schema/default coordination or logical recovery.
- Adopt a shared versioned core contract. Attach/stop Flight around an existing
  native deployment without resetting identities or invalidating healthy writes.
- Implement authorized Cloud/Software adapters and durable, fenced, idempotent
  migration/config jobs in Redis. Native backend mechanisms perform physical work.
- Preserve keys/IDs/retained results across cutover, reconnect/reprepare safely,
  and never replay ambiguous writes or mutate private metadata inconsistently.
- Publish estimated duration/ranges, progress, errors/completion via authenticated
  API/logs/metrics; no short pause SLA, no unbounded individual client waiting.
- Preserve local execution/native CRDT conflicts, WAN-local DML availability,
  one default schema owner and safe DDL wait/takeover. No global transaction or
  constraint guarantee and no mandatory non-Redis coordinator.
- Gate privileged APIs separately from existing unauthenticated health/metrics.
  Preserve tenant ACLs and redact secrets/identifiers. No admin access from SQL.
- Qualify named dataset storage separately from driver SQL and distinguish native
  driver performance from Flight service overhead. Beta regressions are reported.

Acceptance requires native-without-Flight and managed-job failure tests described
in the plan. The exact identity/bootstrap, fencing, adapter/auth/API, dataset and
resource policies remain unimplemented design gates, not current capabilities.
