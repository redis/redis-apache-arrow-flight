# Public integration and paid-feature build boundary

Status: Active-Active control-plane design only; reconciled 2026-10-25.
The build split exists; migration/adaptation runtime is not implemented.

Monitoring, optimization, automation, backup and security implementations belong
in a separate commercial extension for Flight-owned capabilities only. The public
Flight project retains integration contracts and compilable community stubs.
The ADBC driver retains its full implementation and has no enterprise split.
This supersedes earlier placement of paid implementations directly in the
public Flight project. This document does not publish implementation repository details.

On 2026-10-25 the maintainer additionally classified every Flight-owned
Active-Active feature as Flight Enterprise automatically, whether or not it fits
those five categories. This includes managed-profile variations whose designs
are still TBD. The edition decision is settled; individual capability contracts,
safety and qualification remain required. Native Active-Active functionality
stays in the full public driver and does not acquire an enterprise dependency.

## Community stubs and enterprise builds

The requestor selects two build profiles. Community builds compile from public
source without access to paid implementation modules; enterprise builds compile
with the complete paid implementations. Public code owns versioned provider
interfaces and community stubs. Paid implementation packages supply those same
interfaces and are linked into enterprise artifacts. No runtime Flight dependency
or network license service is introduced by this build model.

| Feature category | Community stub behavior | Enterprise implementation |
|---|---|---|
| Monitoring | No optional telemetry values/events; capability says unavailable, never fabricated zero/healthy/progress | Full diagnostics, reporting/export and operational monitoring |
| Optimization | Use correct basic execution; no tuning recommendations or paid acceleration | Complete paid optimizer/tuning and accelerated execution paths |
| Automation | No job/action is performed; explicit unavailable result for requested management operations | Provisioning, discovery/control workflows and automation |
| Backup | No backup/restore side effect or success claim; explicit unavailable result | Managed native-product backup/restore orchestration when implemented |
| Security | No paid security output or privilege grant; reject operations requiring an absent provider | Complete configured TLS/auth/authorization and security management |
| Flight-owned Active-Active | Contracts and compilable unavailable stubs; no feature data, managed action or success claim | All implemented Active-Active capabilities; roadmap items remain unavailable until implemented and qualified |

“No output” applies to optional feature data, not successful empty SQL rows,
empty Arrow schemas or successful no-op mutations. Preserve correct SQL/ADBC
results, ownership, cancellation and cleanup in basic execution. Never silently
accept a credential, omit an authorization check, bypass certificate verification
or downgrade a requested secure transport. A community build without a required
security implementation fails explicitly before executing that operation.

Return a capability/profile description so callers distinguish unavailable from
zero, empty, healthy or completed. Exact option/API/error identifiers remain
implementation work. Community and enterprise API signatures/schemas remain
compatible; tests assert explicit availability differences and equivalent SQL
results for their shared supported paths. Enterprise includes all implemented
features, not unimplemented roadmap items.

Keep proprietary implementation imports out of community modules and dependency
resolution. Enterprise composition must avoid module cycles and must include the
appropriate tests/configuration. Test a clean community checkout with no private
credentials or modules, and an enterprise checkout with all providers. Required
provider absence/invalid configuration fails during setup or before the affected
operation, never after pretending success. Document build commands and artifact
profile/capabilities; production builds cannot select insecure stub behavior by
accident. A linker flag alone is not isolation of paid implementation source.

## Integration requirements

Native ADBC works without a running Flight service or an enterprise composition.
Both Flight profiles use the same full public driver. Flight integrates its own extension
providers and delegates SQL to ADBC. The optional full control plane is an
enterprise capability. Preserve IDs/keys, local consistency, retained results,
caller identity, deadlines and uncertain-write classification. Redis owns native
physical HA, persistence and CRDT replication; no new replication engine is added.

## Commercial feature review gate

A Flight-owned match to monitoring, optimization, automation, backup or security triggers
requestor/maintainer review before implementation/release. All Flight-owned
Active-Active features are already classified as Enterprise by the maintainer;
record that decision without asking for another category/edition decision. Record
existing/new scope, build-profile behavior, packaging, standalone/safety impact and the actual
decision. The chosen build model does not itself select a license or entitlement
system. Source copyright/license notices remain intact.

Active-Active scope includes Flight-specific topology discovery, provisioning,
migration/cutover, regional policy/routing, schema coordination, admission/drain,
recovery, dataset adaptation and status. Shared SQL delegation still uses the
complete public driver; it does not extract or gate the driver's native support.

## Security is a prerequisite for control-plane implementation

Require verified transport, authenticated/authorized identity, tenant isolation,
protected secrets and safe recovery for enabled operations. Missing security
providers fail closed; community compilation does not imply plaintext/anonymous
fallback. Security tests include unavailable-provider behavior in both profiles.

## Extraction status

The runtime split now uses compile-time community implementations and separately
composed enterprise source. See [build profiles](build-profiles.md) and the runtime
change evidence. This implements feature placement/build behavior, not the future
Active-Active allocator, migration, backup or advanced security roadmap. Full
Software qualification results and unresolved failures must remain explicit.

## Authoritative Active-Active proposal

The maintainer selected the current driver proposal on 2026-10-23 over the
older driver proposal. It owns the shared runtime/format
contract: complete current-driver feature parity with local SQL behavior, native
operation without Flight, Redis-hosted root/regional coordination, preserved IDs,
keys and planned-cutover retained results. Root fencing/durability and independent
native identity remain implementation gates, not solved product capabilities.

Flight owns optional management APIs, provisioning/tooling integration, migration
and status/progress/ETA/logs/metrics; it calls the same driver contracts rather
than becoming a mandatory allocator endpoint or a second SQL/metadata engine.
Qualify Cloud Pro first in AWS us-west-2/us-east-1 with at least three primary
shards per regional database, Software next with verified 4x QPF. Support two or
more regions. The previous append/read-only and partition exclusions are
superseded. No Active-Active runtime or new qualification is claimed here.

## Optional Flight-managed architectural profile

On 2026-10-25 the maintainer requested an explicit operating-profile option that
allows selected design decisions to change when Flight provides a centralized
managed plane. This may change schema execution/ownership, admission coordination,
recovery responsibility, enrollment/authority management and availability
dependencies. It can enable capabilities unavailable to a standalone driver
under its process and authority constraints. Individual variations remain TBD.

This operating profile is separate from the community/enterprise build profile.
Native mode retains the standalone contract unless a particular requirement is
explicitly reclassified. Flight-managed mode may require an available authorized
control plane for named operations under an approved capability contract. Merely
attaching Flight does not activate that contract or change an existing native
deployment's guarantees.

For each variation, record the baseline decision being changed, required managed
capabilities, driver/control-plane/data-service/Redis responsibilities, caller
authorization, durable evidence, Flight/WAN failure behavior, caller deadlines,
safe activation/exit and qualification. There is no silent fallback to a weaker
native contract after losing Flight. Central service availability does not itself
prove fencing, durability or global SQL consistency.

See the [shared managed-profile proposal](https://github.com/alberttwong/redis-adbc-driver/blob/067cb6bc4d608eb39d6e9b139c122b31adb6aca9/docs/flight-managed-profile.md). Public contracts and
unavailable community stubs may describe selected capabilities; all Flight-owned
Active-Active implementations automatically belong in Enterprise. Classification
does not approve a particular architectural variation or imply implementation.
No provider, public option, dependency update or managed runtime is implemented
by this documentation change.
