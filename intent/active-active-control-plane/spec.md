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

Security is first-priority scope before remote management/live migration: verified
TLS on data/control/Flight/management links (TLS >=1.2, prefer 1.3), explicit CA
and product-supported client-certificate handling, no verification bypass or
plaintext downgrade; separate data/runtime/status/management identities and
least-privilege role scopes; tenant/action authorization for every admin job;
trusted lineage/generation-bound configuration and endpoint restrictions; secret
redaction and credential/certificate rotation/revocation including live pools;
private network/admin isolation, protected audits and adversarial acceptance tests.
Existing AuthNone/health/metrics exceptions cannot grant management access. Native
clients enforce their part without Flight. Product-native replication encryption
must be qualified separately. See the architecture security contract for detail.

Commercial policy: monitoring, optimization, automation, backup and security are
paid-feature categories. A match to any one triggers requestor/maintainer review
before implementation/release. Record category/scope, existing versus new behavior,
paid packaging, standalone/safety impact and the actual decision. Current proposed
security/TLS and operational/control-plane capabilities are pending that review;
no implicit baseline exemption, existing-feature relicensing or billing/enforcement
implementation is authorized by this documentation. Preserve safe TLS and execution.

Build-profile update: community builds compile with public provider interfaces
and stubs; enterprise artifacts link complete implemented paid features. No
private dependencies are resolved by a clean community build. Optional telemetry
returns no feature data; SQL execution retains correct basic results. Requested
management/security/backup operations cannot silently succeed when their provider
is absent. Preserve safe failure and explicit capability reporting. Runtime
source extraction and both-profile tests remain pending.

## Managed-profile decision (2026-10-25)

The maintainer permits selected architectural decisions to change in an explicit
Flight-managed operating profile. Exact variations remain undecided. Record each
changed decision, service dependency, authority boundary, outage behavior, safe
transition and acceptance tests before implementing it. Native requirements remain
the baseline unless individually reclassified; attaching Flight does not silently
change them. This operating profile is distinct from community/enterprise builds.
See the [shared profile contract](https://github.com/alberttwong/redis-adbc-driver/blob/b71bc7c5678b3128abcf4a3029f8214e11943926/docs/flight-managed-profile.md).
