# SQL grant revocation qualification follow-up

The first hosted enterprise qualification of private candidate `7d34429` against
public source `bdac4cc` failed `TestIntegrationSnapshotSQLGrantRevocation`: the
ordinary probe connection was denied while a retained connection still completed
a read. The initial local run had passed. Issue #65 was reopened while this
failure was investigated; the first hosted run is not a qualification pass.

The native v0.0.28 implementation verifies management state before returning from
REVOKE but explicitly permits asynchronous propagation to proxies/shards and
completion of existing streams. See [the immutable native implementation](https://github.com/redis/redis-adbc-driver/blob/b63a010d88d2b69707594cb9dc063992dac06b32/go/rbac.go#L394).
A denial on one connection therefore does not establish a barrier on another.
This was already part of the gateway's documented revocation boundary.

The corrected test requires PermissionDenied on the exact retained connection
within 15 seconds, then requires repeated guarded SELECT 1 calls to fail. An
unrelated timeout or transport error does not satisfy the permission check. No
SQL write is replayed. This strengthens the terminal-state assertion while
allowing the documented propagation interval; it changes no server/driver runtime.
Local observation reproduced successful reads during that interval followed by
PermissionDenied and terminal failure, rather than an indefinite grant bypass.

Initial local repeat/full-suite attempts accidentally overlapped: each Docker
container had its own default lock directory. Those attempts included management
HTTP 406 and authentication fixture failures and do not qualify the change. The
local runner now supplies one lock directory on its shared source volume, and
the final focused/full runs execute sequentially. Failed/invalid raw receipts
remain ignored, separately from sanitized passing results.

Final serialized results and the updated composition fingerprint accompany this
follow-up. Python transport, independent Airbyte and upload measurements in the
original receipt remain tied to their recorded source. This follow-up changes
only qualification code; it does not update their production dependencies or
claim those suites were executed again.

The final serialized focused command passed all ten repetitions with the race
detector in 73.145 seconds on verified TLS/ACL Redis Software, Search, three
primary shards and active 4x QPF. The complete backend reruns are recorded
separately when they finish; this focused result alone is not their substitute.
