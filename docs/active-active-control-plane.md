# Optional control plane for standalone Redis ADBC

Status: proposed design, 2026-10-22. No management/migration runtime is implemented
by this document. Refs [intent](../intent/active-active-control-plane/intent.md),
[spec](../intent/active-active-control-plane/spec.md),
[plan](../intent/active-active-control-plane/plan.md) and
[driver design PR](https://github.com/alberttwong/redis-adbc-driver/pull/501).

The ADBC driver must work in ordinary and Active-Active deployments without
Flight. Flight is the optional full control plane for control, centralized
configuration, provisioning/migration and operator observability. Native ADBC
queries do not have to transit Flight. Flight SQL remains a separate optional
network data interface using the same driver.

## Ownership

| Layer | Owns |
|---|---|
| ADBC driver | Independent runtime bootstrap/backend identity, all current SQL/Arrow capabilities, safe numeric allocation, default schema ownership, local metadata/index readiness, native options, pause enforcement, logical SQL recovery, retained partitions and lightweight local diagnostics. No mandatory Flight RPC, heartbeat or Flight-issued identity. |
| Flight control plane | Product management APIs, desired configuration, provisioning/control databases, native migration/verification, source fencing, endpoint handoff, rollback decisions, managed owner/range-policy changes, durable job recovery, authorized admin/status APIs, progress/ETA, logs and metrics. |
| Flight service data plane | Authentication/tenants, SQL request translation, pools/cookie sessions, prepared handles/tickets, streaming/admission/cancellation and named Arrow IPC datasets. SQL semantics are delegated to the driver. |
| Redis Cloud/Software | Physical persistence, replication/CRDT conflicts, local HA and supported import/transfer/restore mechanisms invoked by Flight. No new replication engine is proposed. |

Use one versioned driver/control-state contract. Flight calls shared driver
validation/reconciliation/configuration interfaces rather than editing opaque
managed metadata or implementing a second allocator/SQL engine. Ordinary ADBC
connection/statement/session options remain usable without Flight; centralized
deployment policy/configuration belongs here.

## Standalone is mandatory, management is optional

Test native ordinary and Active-Active bootstrap, DML/DDL, ingestion, partitions
and logical recovery with Flight never installed. Then attach Flight to that
already running common-format deployment without resetting IDs/keys or recreating
metadata. Stop the control plane and prove healthy native execution continues on
valid local state. Flight must not be the source of an expiring permission needed
for every native write or local allocation refill. Flight SQL clients naturally
still need a running Flight data service for their network requests.

Independent native identity/bootstrap remains an unqualified driver design gate,
especially for Cloud proxies with empty INFO CRDT. Flight management discovery
does not waive this requirement. Do not label Active-Active supported merely
because Flight can fill otherwise mandatory metadata for the first time.

## Minimal native configuration and product adapters

The driver may accept a URI/ADBC option locating Redis-backed control state;
exact syntax and independent backend identity are unqualified design work.
Flight may configure/manage that same state but must not become a required
configuration endpoint. Root enrollment/renewal may wait during WAN isolation;
local block refills and ordinary DML within reserved capacity must remain local.
A copied record or IP address is not proof of safe regional identity after restore.

Use the [Software proxy](https://redis.io/docs/latest/operate/rs/databases/configure/proxy-policy/)
for native local shard routing/endpoint failover and the
[Cloud API](https://redis.io/docs/latest/operate/rc/api/) or
[Software CRDB API](https://redis.io/docs/latest/operate/rs/references/rest-api/objects/crdb/)
for Flight product discovery/management. Do not put management polling on the SQL
hot path or equate product active status with allocator, index or migration
readiness. Qualify supported API fields/operations and authoritative source
fencing independently. An ordinary-to-Active-Active in-place switch is not assumed.

## Security is a prerequisite for control-plane implementation

Security is first-priority acceptance scope, before enabling remote management or
live migration. These are proposed requirements, not claims about current defaults.
Standalone drivers must enforce the same connection/control-state trust rules
without Flight; the optional service must not weaken them.

- **Verified encryption on every network hop.** Require TLS for deployed Redis
  data/control connections, Flight clients/admin APIs and product-management HTTPS
  calls. Set a TLS 1.2 minimum and prefer TLS 1.3 where supported. Verify trust
  chains and DNS/IP subject alternative names; support explicit private CA bundles
  and client certificates where required. An IP URI must match an IP SAN or use
  an explicitly configured verified server name. No skip-verification option,
  silent plaintext fallback or credential forwarding after an unverified redirect.
  Any plaintext development mode is explicit, loopback-only, and cannot expose
  privileged management. Qualify Redis-native inter-region/control replication
  encryption separately; client TLS alone does not secure those links.
- **Separate identities and least privilege.** Distinguish application data access,
  driver runtime control-state access, read-only operator status and privileged
  Flight deployment management. Applications never receive Cloud account/API keys
  or management credentials. Define required Redis commands/key scopes per role,
  including scripts/Search and cross-database access; verify product ACL behavior.
  Protect issuer/owner records from ordinary DML identities. Authentication alone
  does not authorize provisioning, endpoint changes or migration. Every admin/job
  operation checks authenticated subject, tenant, deployment and action scope;
  AuthNone is rejected for management. Delegated DDL must retain caller permissions
  rather than inherit the schema owner's administrative access.
- **Trust configuration and endpoint changes.** Authenticate and authorize control
  records and bind them to tenant/database lineage, regional incarnation and
  monotonic generations; TLS proves endpoint identity, not ledger freshness.
  Reject stale/replayed/foreign handoffs. Allowlist product/control destinations,
  constrain redirects and revalidate DNS/TLS on reconnect to prevent configuration
  or URI inputs from turning privileged HTTP clients into arbitrary network access.
  Provisioning credentials cannot be selected by an untrusted query or URI.
- **Secret and certificate lifecycle.** Accept separate protected files or secret
  references; do not put management keys, nested passwords or private keys in
  connection URI query parameters, tickets, jobs, error text, logs or metrics.
  Redact connection diagnostics and restrict file/control-record/backup access.
  Support credential and CA/client-certificate rotation with bounded pool refresh;
  test revocation against existing pooled sessions, not only new connections.
  Define compromised-controller credential revocation and writer fencing without
  reusing issued IDs or replaying uncertain writes. A hosted secrets service is
  optional, not a new mandatory backend.
- **Network and administrative isolation.** Prefer private endpoints and network
  allowlists. Keep admin/status authorization separate from SQL, health and current
  unauthenticated metrics listeners. Public health exposes only minimal liveness;
  restrict metrics access at the deployment boundary and keep privileged job data
  behind authorized APIs. Bound management requests, job admission and retries.
- **Auditable changes and negative tests.** Record actor, authorized target/action,
  request/job ID, generation, phase/outcome and time for configuration changes,
  provisioning, migration, owner changes and authorization failures. Keep sensitive
  audit records access-controlled with explicit retention and integrity/tamper
  controls, separate from bounded public metric labels. Never log SQL values or
  credentials. Qualify wrong/expired/untrusted certificates, hostname mismatch,
  missing client certificates, ACL denial, cross-tenant requests, replayed handoffs,
  malicious endpoint inputs, secret redaction and rotation/revocation under load.
  Security failures fail closed with explicit errors; no insecure recovery bypass.

Use [Cloud TLS guidance](https://redis.io/docs/latest/operate/rc/security/database-security/tls-ssl/),
[Software TLS configuration](https://redis.io/docs/latest/operate/rs/security/encryption/tls/enable-tls/)
and [Software certificate guidance](https://redis.io/docs/latest/operate/rs/security/certificates/)
for product adapters. Qualify exact product versions, trust stores and client-auth
capabilities. Measure ADBC performance with these security settings enabled;
beta performance tolerance does not permit disabling verification or authorization.

## Managed migration workflow

1. Observe an authorized desired-topology/configuration change; no driver migration
   command or application mode/region toggle. Preflight identity, driver format,
   capability parity, topology/resources, permission, capacity and transfer support.
2. Provision targets/control resources through Cloud/Software adapters or existing
   deployment tools under the Flight job. Keep application target writes disabled.
   Record cost/ownership and secret references privately; no query may implicitly
   authorize spending or privileged management access.
3. Invoke the product's supported native migration and verification. Preserve
   physical keys, numeric IDs and committed retained results across cutover.
   Do not re-ingest SQL rows with regenerated identities. Software's documented
   migration creates a new target; do not assume an in-place topology flag.
4. Estimate/report total preparation and write-pause duration/range/confidence,
   phase/progress and revised ETAs. Seconds/minutes/hours are acceptable; client
   operation deadlines remain independent. Unknown progress stays unknown.
5. Fence source writers, coordinate driver drain and final catch-up/readiness, then
   commit a verified lineage/endpoint generation. A local driver's acknowledgement
   is not proof every writer is fenced; qualify an authoritative fence/participant
   protocol. No global SQL transaction or normal-write consensus barrier is added.
6. Let drivers validate/adopt the handoff, reconnect/reprepare for future work and
   resume. Do not replay uncertain writes, reconsume input or transplant cursors.
   Keep unexpired retained results accessible; complete regional loss need not
   preserve ADBC retained results. Never silently regenerate a missing result.
7. Journal phases/requests idempotently in Redis, fence duplicate controllers and
   recover from restart. Pre-commit abort may resume only a verified authoritative
   source; after commit never reopen a stale source just to meet an ETA. Explicitly
   report unknown/failed outcomes. Resource deletion is separately authorized.

Root/region control state remains Redis-hosted; additional Redis control databases
are acceptable. No required PostgreSQL/etcd/SaaS coordination service. Native
default allocation/schema operation stays in the driver; Flight manages optional
policy changes through the same fenced core protocol. Selecting/proving that
protocol and Software/Cloud recovery guarantees remains implementation work.

## Availability, API and security

Use native Redis conflict resolution. SQL execution/checks use local state;
cross-region convergence is asynchronous. One automatically selected schema
owner is accessible through every client. During WAN isolation or owner loss,
local reads/DML continue while DDL/migration wait within request deadlines for
safe owner recovery/replacement. Flight absence alone does not freeze native DDL.

Expose versioned authenticated/authorized management/configuration and read-only
status interfaces. Keep privileged management credentials out of query clients,
cookies/tickets, logs/metrics and public health. Existing AuthNone or a successful
PING must not enable admin operations. Exact transport/role/config schemas are
proposed work; no built-in standard Flight SQL migration API is asserted.

Extend the existing structured logger and Prometheus registry. Report managed
job state separately from local driver/service state, including observation scope
and stale/unknown participants. Keep liveness separate from read/write readiness.
Existing unauthenticated health/metrics surfaces must not expose privileged job
details. Use bounded labels without SQL/user/table/secret identifiers. Driver
callbacks/snapshots must not start their own server or force network publication.

Preserve caller authorization, cancellation and outcome/retryability through
gRPC errors. A retryable not-started maintenance error is different from an
ambiguous write; ticket replay never executes a write again. Refresh stale
Flight-owned pools/sessions/prepared state without duplicating driver recovery.

Named datasets bypass SQL storage and have separate generation/publication/IPC
semantics. Assess/test their Active-Active migration independently; driver SQL
qualification is not dataset qualification. Define Flight ticket/cache/dataset
durability separately from the agreed region-local ADBC retained-result contract.

## Qualification and current source

Qualify Cloud Pro first in AWS us-west-1/us-west-2 with at least three primary
shards per regional application database; Software next with the same minimum
and verified effective 4x QPF per instance. Verify Cloud supported settings
separately; do not assume Cloud Active-Active accepts Software QPF configuration.
Keep credentials/endpoints/resource identifiers private. No new resources were
provisioned here. No Valkey, ElastiCache or Memorystore target is introduced.

Measure ADBC ingestion and scans/aggregation first, ClickBench long term; beta
regressions are reported rather than blockers. Measure Flight queue/network/IPC
overhead separately. Future ClickBench uses Software/4x and verified compatible
full-scale backups for repeat queries, not reloads unless loading is requested.
Correctness, feature parity, authorization, lifecycle and failure checks remain.

The existing service has [metrics](../server/metrics.go), [health](../server/health.go),
[pools](../server/pool.go), [sessions](../server/session.go),
[SQL execution/tickets](../server/sqlserver.go) and [datasets](../server/datasets.go).
Use those as integration points; they do not currently implement this control
plane. Runtime implementation requires a separately qualified driver revision;
this change does not update go.mod/go.sum or claim availability in a release.
