# Plan

Date: 2026-10-22. Author: Codex. Status: design delivered; runtime pending.
Refs [intent](intent.md), [spec](spec.md), [evidence](evidence.md).

Before implementation, classify every proposed capability against monitoring,
optimization, automation, backup and security. All Flight-owned Active-Active
features are automatically Enterprise under the maintainer's 2026-10-25 decision;
record that classification without another edition decision. For other matches,
record the actual commercial decision and keep unresolved packaging pending.
Inventory overlapping features and preserve the TLS/security boundary. This rule
applies only to Flight; the complete native driver remains public.

0. Establish the security/trust model first: separate runtime and management roles,
   verified TLS/CA/client-certificate options on each hop, tenant/action checks,
   endpoint restrictions, protected audit/secret storage, rotation/revocation and
   negative tests. Gate management/migration enablement on this work; measure
   performance with production security enabled. No insecure bootstrap fallback.
1. Agree/pin the driver's independent common-format identity/bootstrap, readiness,
   configuration and outcome/event contract. Prove native ordinary/Active-Active
   execution with Flight never installed; Cloud empty INFO cannot be waved away.
2. Implement Flight management/config/status authorization and Cloud/Software
   adapters. Keep keys/private identifiers out of credentials carried by SQL.
3. Implement Redis-backed jobs, single-controller fencing, desired-state discovery,
   provisioning/transfer/verification, write-pause coordination, commit/abort and
   restart recovery using native provider mechanisms and shared driver helpers.
4. Integrate existing logger/metrics, pool/session/ticket lifecycle and errors.
   Define the custom management transport/schema without claiming standard Flight
   SQL APIs or repurposing unauthenticated health as privileged readiness.
5. Qualify standalone/managed modes, attach/stop Flight, duplicate-controller and
   every job phase crash, stale identities, WAN/owner loss, cancellation/deadlines,
   credential/tenant isolation, retained cutover, region loss and ambiguous writes.
   Verify complete Arrow values/effects, not just a command response or row count.
6. Review/qualify dataset generation/publication/migration separately. Retain its
   atomic publication, replay, ownership, same-slot and lifecycle guarantees.
7. Cloud Pro first: AWS us-west-2/us-east-1, at least three primary shards per
   regional application DB. Software next: at least three plus verified 4x QPF
   per instance. Record exact versions/settings, costs/ownership and cleanup.
8. When runtime integration is ready, check the latest upstream driver release
   and default-branch commit before selecting an immutable tested dependency,
   following CLAUDE.md. No dependency update is performed in this design change.

Prioritize bulk ingestion/scans/aggregations; publish beta performance deltas
without a regression threshold. Follow the Software/4x/backup ClickBench rules
for later query benchmarks. No live migration or benchmark belongs to this patch.

Build-profile update: community builds compile with public provider interfaces
and stubs; enterprise artifacts link complete implemented paid features. No
private dependencies are resolved by a clean community build. Optional telemetry
returns no feature data; SQL execution retains correct basic results. Requested
management/security/backup operations cannot silently succeed when their provider
is absent. Preserve safe failure and explicit capability reporting. Runtime
source extraction already exists; the Active-Active runtime and its qualification
remain pending.

## Managed-profile decision (2026-10-25)

The maintainer permits selected architectural decisions to change in an explicit
Flight-managed operating profile. Exact variations remain undecided. Record each
changed decision, service dependency, authority boundary, outage behavior, safe
transition and acceptance tests before implementing it. Native requirements remain
the baseline unless individually reclassified; attaching Flight does not silently
change them. This operating profile is distinct from community/enterprise builds.
See the [shared profile contract](https://github.com/alberttwong/redis-adbc-driver/blob/067cb6bc4d608eb39d6e9b139c122b31adb6aca9/docs/flight-managed-profile.md).
