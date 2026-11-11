# Intent: shared read snapshots over Flight SQL

- Requester: issue #65, https://github.com/redis/redis-apache-arrow-flight/issues/65.
- Date: 2026-11-11. Baseline: `97cbe7cf75b78de0cf3beb20a1e4f28ea95fbfe1`.
- Status: implemented locally; independent enterprise validation pending.
- Goal: one authenticated managed-catalog view for metadata, schema validation
  and sequential multi-table Airbyte extraction, with explicit bounded lifecycle
  and no silent fallback after loss.
- Scope: core Flight SQL consistency/protocol, plus the minimum verified TLS/ACL
  authentication baseline explicitly authorized by the requester on 2026-11-11.
  See [security decision](security.md).
- Non-goals: write transactions, arbitrary HASH/shard snapshots, durable resume,
  changing the external connector or claiming it is enabled.
- Acceptance/evidence: [spec](spec.md), [plan](plan.md),
  [contract and outstanding qualification](../../docs/shared-read-snapshots.md).
- Maintainer packaging/reviewer approval: not recorded. No deployment or release.
