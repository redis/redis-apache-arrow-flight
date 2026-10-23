# Public integration and paid-feature build boundary

Status: proposed build/interface contract, 2026-10-22; runtime implementation in review.

Monitoring, optimization, automation, backup and security implementations belong
in separate commercial extension packages. Public driver/Flight projects retain
provider interfaces, integration contracts and compilable community stubs.
This supersedes earlier placement of paid implementations directly in these
public projects. This document does not publish implementation repository details.

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

Native ADBC must work without a running Flight service. Driver extension code
runs locally in its enterprise artifact. Flight integrates its own extension
providers and delegates SQL to ADBC. The optional full control plane is an
enterprise capability. Preserve IDs/keys, local consistency, retained results,
caller identity, deadlines and uncertain-write classification. Redis owns native
physical HA, persistence and CRDT replication; no new replication engine is added.

## Commercial feature review gate

A match to monitoring, optimization, automation, backup or security triggers
requestor/maintainer review before implementation/release. Record existing/new
scope, build-profile behavior, packaging, standalone/safety impact and the actual
decision. The chosen build model does not itself select a license or entitlement
system. Source copyright/license notices remain intact.

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
