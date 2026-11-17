# Native ADBC and Flight companion contract

This is the integration contract for [Flight #61](https://github.com/redis/redis-apache-arrow-flight/issues/61),
which owns the remaining work from [native #214](https://github.com/redis/redis-adbc-driver/issues/214).
The [qualification and acceptance map](validation/2026-11-17-companion-contract/README.md)
identifies exactly which builds and clients were exercised. It is source
qualification, not a published release or production deployment certification.

## Service ownership and deployment

The native Redis ADBC driver runs in the client's process and communicates with
Redis. It neither starts nor requires a Flight service. This repository owns a
**separately deployed** Go gateway: standard Flight SQL commands call native ADBC;
PATH-based Flight RPC stores and retrieves named Arrow datasets. Both interfaces
share one gRPC service but have different storage and execution contracts.
There is no second Flight server implementation to add to the native driver.
The protocol follows the [Apache Flight SQL specification](https://arrow.apache.org/docs/format/FlightSql.html);
the [Apache ADBC Flight SQL driver](https://arrow.apache.org/adbc/current/driver/flight_sql.html)
is one standard client, separate from the Redis ADBC driver.

Community builds resolve only public dependencies and provide core SQL, protocol,
Arrow/storage safety and the verified TLS/Redis ACL baseline. The private
enterprise overlay supplies the capabilities listed in [build profiles](build-profiles.md),
including pooling, streaming optimizations and optional operational features.
It must match the complete public Go/C module fingerprint. A successful community
unit build does not qualify an arbitrary enterprise composition. The native
ADBC driver is the same full public driver in both profiles.

Operators own Redis/Search availability, verified TLS trust, Redis users/grants,
capacity, token-secret distribution, session affinity and gateway rollout. See
[server operations](server-guide.md#deployment-and-operations) and
[rollback constraints](sdlc/operations.md#rollback-constraints). A shared token key
allows bearer authentication across replicas; it does not transfer sessions,
open readers, temporary objects or snapshot attempts.

## Protocol and execution

The [method matrix](flight-capabilities.md) is the authoritative endpoint inventory.
It separates SQL execution from dataset transfer and records unsupported methods.
Standard Flight SQL metadata uses specified Arrow schemas; optional SQL type-name
metadata is distinct from Arrow physical types. Unknown precision/scale remains
NULL. Prepared schemas can be incomplete before binding; clients must use the
rebound prepared handle and the actual returned result schema.

| Operation | Contract |
| --- | --- |
| SQL and prepared query | Ordinary read-only SELECT/SHOW executes at DoGet. GetFlightInfo plans the schema and issues a ticket; it does not freeze rows. Replay can observe later publications. |
| SQL with side effects | Runs when its FlightInfo is requested; returned tickets contain or reference its result. Replaying DoGet does not execute the write again. Reissuing the original update is a new request, not an exactly-once retry protocol. |
| Ingestion | Flight SQL create/append/replace/create-or-append modes; consume the Arrow stream and check the result. Autocommit only. |
| Transactions/savepoints | No read/write transactions or rollback guarantees. BeginTransaction is UNIMPLEMENTED; transaction IDs are refused. The ADBC client reports unsupported transaction control. |
| Substrait and DoExchange | Explicit UNIMPLEMENTED. DoExchange directs clients to DoPut/DoGet; a generic Flight interface does not imply bidirectional query transforms. |
| Shared snapshots | The separate [protocol-1 lifecycle](shared-read-snapshots.md) binds metadata/planning/reads to one native managed catalog/hash slot. Ordinary SQL tickets and datasets do not implicitly acquire it. |

A Flight wrapper still runs the native SQL/storage engine and decodes managed
Redis data. It does not provide native typed server export; that remains
[native #195](https://github.com/redis/redis-adbc-driver/issues/195).

## Four different descriptor contracts

All descriptor bytes are opaque to applications. Treat SQL tickets and partition
bytes as query data: they may contain SQL, literals, bound Arrow values or already
returned result data. They are not encrypted credentials or portable attachment
authority. Keep them out of logs and send them only through the proper interface.

| Descriptor | Issuer and consumer | Meaning and scope |
| --- | --- | --- |
| Native Redis ADBC partition | Native `Statement.ExecutePartitions` → native `Connection.ReadPartition`, without Flight | Driver-version-specific deferred ranges or retained results, depending on options. The native driver owns supported query shapes, layouts, authorization, snapshot/retention and cleanup. See its [partition implementation at the qualified release](https://github.com/redis/redis-adbc-driver/blob/b63a010d88d2b69707594cb9dc063992dac06b32/go/partitions.go). |
| Flight SQL result ticket | Gateway CMD GetFlightInfo → Flight DoGet | Gateway query/prepared/result handle, with query control state when available. Currently SQL FlightInfo has one endpoint. It is not a native row-range partition and does not imply sharded parallel execution. |
| ADBC Flight SQL client partition | Apache's Flight SQL ADBC `ExecutePartitions` → that driver's `ReadPartition` | A client transport descriptor for Flight endpoints. Using the ADBC API does not make these bytes native Redis descriptors. Independent connections need the same appropriate identity, routing and required session/snapshot context. |
| Flight RPC dataset endpoint | Gateway PATH GetFlightInfo → Flight DoGet | An immutable stored dataset generation, optionally projected columns and stored-batch ranges. Read every endpoint to cover the complete advertised result. Stored batches can differ from uploaded batches in the enterprise profile. |

None is interchangeable with the others. The gateway does not call the native
ExecutePartitions/ReadPartition API to manufacture its SQL tickets. It calls
native query execution and streams Arrow results through Flight. A dataset name
is not a SQL table or native partition target; use the explicit dataset-to-SQL
workflow when a managed SQL copy is required.

A same-identity independent Flight SQL client can replay an ordinary fully
qualified query without temporary/session-dependent state. It is a **new read**,
not durable resume, a retained snapshot or proof that every session is portable.
Native retained partition transport has its own native contract; Flight does not
forward its attachment/reference options automatically.

## Identity, replay, expiry and cleanup

For `redis-acl` deployments, both Flight and Redis require verified TLS. Native
operations execute as the authenticated Redis user. Tenants, sessions and native
connections keep credentials isolated. Query cancellation ownership is bound to
the authenticated username; a valid replacement credential for the same user can
cancel that user's query. Cookies and snapshot handles additionally require their
owning process/session and identity context. With `auth=none`, all clients share
the configured Redis identity and cancellation scope.

A ticket is not a replacement for authentication or Redis ACL checks. Copying
bytes that already contain data cannot retract that data from the copying client.
Legacy/client-constructed query tickets may lack cancellation state; they do not
provide ownership-enforced cancellation. Do not interpret an opaque handle as an
independent authorization grant or forward another user's cookie/bearer token.

| State | Lifetime and failure |
| --- | --- |
| Cancellable query control | Pending/completed records have a ten-minute TTL; active reads renew the lease. Missing/expired records return NOT_FOUND. Foreign owners get PERMISSION_DENIED; cancelled reads get CANCELLED. |
| Materialized SQL result referenced by key | Ten-minute retention; missing result is NOT_FOUND. Small results may be carried directly in the ticket. This is result delivery, not write replay. |
| Dataset generation | Replaced generations have a five-minute grace period; configured DatasetTTL and explicit drop can end access. A generation-zero/current ticket can see a newer generation. Dataset generation numbers are not native snapshot handles. |
| Cookie session | Process-local, idle expiry plus sweep. Unknown/closed/restarted/wrong-replica sessions fail ABORTED before execution and receive a recovery cookie; clients must explicitly restore settings/state. No transparent resume. |
| Shared snapshot | Fixed native deadline, separate idle and native lease cleanup. Expiry, authority loss and extraction failures terminate the attempt. Redis/SQL grant changes propagate asynchronously; no synchronous global revocation barrier is promised. |

CancelFlightInfo and legacy CancelQuery share cancellation state in Redis for
non-session query routing across replicas. Pending cancellation prevents fetching;
active cancellation cooperatively interrupts readers and releases resources.
Completed/non-cancellable/write-result tickets return NOT_CANCELLABLE; cancellation
cannot roll back an autocommitted write. Early release/disconnect/stall paths must
close readers, native cursors, connection reservations and retained Arrow memory.
Abrupt idle disconnect has no universal close signal; session reaping/native
leases bound retention. See [data access](data-access.md) for exact status behavior.

If the identity cannot write cancellation metadata, supported read-only SQL can
issue a non-cancellable ticket; other backend failures are not silently ignored.
Pool/stream/session limits and result budgets apply as documented. The fallback
result budget is not a cap on every native/process allocation, and default
stream-capacity fallback can materialize a result. See [capacity issue #28](https://github.com/redis/redis-apache-arrow-flight/issues/28).

## Qualification and compatibility boundaries

The [acceptance map and receipt](validation/2026-11-17-companion-contract/README.md)
link each original #214 item to named executable checks, source fingerprints and
recorded outcomes. Existing full Software/4x-QPF backend results are reused only
where their Go-module source matches. New real-network Python checks use Apache
ADBC Flight SQL and PyArrow Flight, verified TLS/ACL, complete value/schema checks
and no loaded native Redis client library.

[Historical client sweeps](client-compatibility.md), retained guide endpoints and
matched [performance measurements](performance.md) retain their own revisions.
They are not reruns of this build. JDBC unknown prepared schemas (#17), JDBC type
metadata (#38), production/performance acceptance (#35), bounded execution (#28),
and unsupported GUI application workflows remain separate issues. No new speedup,
SLO, independent-shard snapshot, failover or production rollback qualification is
implied by the companion contract.
