# Flight SQL shared read snapshots

Status: gateway protocol v1 implementation for [issue #65](https://github.com/redis/redis-apache-arrow-flight/issues/65),
**pending independent enterprise/transport qualification**. No gateway release is
qualified by this change. The Airbyte connector must continue rejecting
`read_consistency=shared_snapshot` for `flight_sql` until that evidence exists.
Native snapshot results alone do not establish Flight SQL support.

## Versions and deployment

This implementation pins Redis ADBC **v0.0.24**, Go submodule
`v0.0.0-20261110200608-b052eedad9f2` (release commit
`b052eedad9f2fb2c6eeb7c79fe63f8bbc3ca6bdd`), requiring Go **1.27.3**.
Checked upstream on 2026-11-11: latest completed release v0.0.24; default branch
`766dded08cc3728edfd092f5e4941e95932bfc9a`. The release was chosen rather than
unreleased main. [Native contract](https://github.com/redis/redis-adbc-driver/blob/v0.0.24/docs/shared-read-snapshots.md).
Python qualification targets ADBC Flight SQL/driver manager **1.13.0** and
PyArrow **26.0.0**; these are test targets, not a validation claim.

Acquisition requires enabled cookie sessions, Flight `redis-acl` authentication
and Flight TLS certificates. At the requester's direction (2026-11-11), the
minimum verified TLS/Redis ACL authentication baseline is implemented in core.
No private composition is needed for these basic features. Enterprise feature
validation still requires a qualified composition matching the updated module.
This adds no monitoring, tuning, automation or advanced security feature set.

The native dependency update introduces the driver's current **managed storage
protocol 17** and its fresh-storage requirements. Do not point it at old managed
SQL storage expecting an in-place migration. Provision fresh disposable storage
for qualification and follow the native compatibility policy for deployment.
Dataset IPC generations are a separate interface. Rollback requires discarding
sessions/tickets and separately compatible managed storage; there is no snapshot
resume or automatic storage downgrade.

## Client-visible protocol v1

Use standard Flight `SetSessionOptions`, `GetSessionOptions` and `CloseSession`,
with these Redis extension names. ADBC Flight SQL exposes them through
`adbc.flight.sql.session.option.<name>`; they are **connection** options.

| Option | Meaning |
|---|---|
| `redis.read_snapshot` | Write-only JSON command. Set it alone: `{"action":"acquire","retention_ms":900000}` or `{"action":"release"}`. Unknown fields/actions and invalid lifetimes fail. |
| `redis.read_snapshot_status` | Read-only JSON after an acquisition attempt: `state`, diagnostic `generation`, UTC `deadline`, random gateway `handle`. Terminal states include `failed`, `expired` and `released`. Active status validates native authority; failure is an RPC error. |
| `redis.read_snapshot_protocol` | `"1"` on an authenticated TLS deployment. Describes the gateway protocol; successful acquire is the native capability probe. |

1. Open a **fresh dedicated** connection with
   `adbc.flight.sql.rpc.with_cookie_middleware=true`, authenticated over
   `grpc+tls://` with verified trust.
2. Set `redis.read_snapshot` to acquire **before** GetObjects, GetTableSchema,
   parameter/schema planning or table extraction. Treat any error as failure of
   the entire full-refresh attempt. Do not downgrade or retry on another connection.
3. Read `redis.read_snapshot_status`. Require `state=active`. Install its `handle`
   as `adbc.flight.sql.rpc.call_header.x-redis-read-snapshot`. Every discovery,
   schema/planning and read RPC must carry this header and the session cookie.
4. Rediscover and validate **all** selected schemas inside this attempt, then
   consume tables/views sequentially on this connection. Close/drain each Arrow
   reader/cursor before starting another operation.
5. Release in `finally`, after all readers close, on success, failure, generator
   close and cancellation. Close the ADBC connection/session afterwards. Leave the
   guard installed until close: released sessions cannot perform live reads.

The [Python context manager](../examples/python/shared_snapshot.py) uses the real
ADBC Flight SQL connection options, without loading the native Redis client:

```python
import adbc_driver_flightsql.dbapi
from shared_snapshot import shared_snapshot

# Load credentials and trusted CA from your deployment's secret/trust store.
with adbc_driver_flightsql.dbapi.connect(
    "grpc+tls://gateway.example:8815",
    db_kwargs={
        "username": username,
        "password": password,
        "adbc.flight.sql.rpc.with_cookie_middleware": "true",
        "adbc.flight.sql.client_option.tls_root_certs": trusted_ca_pem,
    },
    autocommit=True,
) as conn:
    with shared_snapshot(conn):
        with conn.adbc_get_objects() as metadata:
            objects = metadata.read_all()
        schemas = [conn.adbc_get_table_schema(t) for t in ("orders", "order_items")]
        for table in ("orders", "order_items"):
            with conn.cursor() as cur:
                cur.execute(f'SELECT * FROM "{table}"')
                consume(cur.fetch_arrow_table())
```

This does not add a standard Flight SQL read transaction. Ordinary native ADBC
connection snapshot options are not forwarded automatically by Flight SQL.

## Ownership, concurrency and failure

One session owns exactly one dedicated native connection and at most one attempt.
Metadata, planning, parameters and every query use that connection. Snapshot
queries bypass pooled streaming/settings-copy paths; the connection stays locked
until its Arrow reader releases. Concurrent use/control returns
`RESOURCE_EXHAUSTED`; close the reader first. Snapshot reads also consume the
existing per-identity stream allowance, with no buffered/live fallback when full.
The existing server session cap is 10,000; native catalog admission remains
bounded by **128 pins**, **16 live generations**, metadata/control budgets and
Redis capacity. Retention can backpressure publishers; admission fails explicitly.

The cookie belongs to the authenticated Redis identity (including credentials),
server instance and configured catalog/tenant. The random attempt handle alone
cannot attach: both its owned cookie and current authenticated identity must
match. Missing/foreign/stale state fails before data access. A native generation
ID is diagnostic only. Tickets/prepared handles created in an attempt carry its
handle and cannot be consumed under another attempt or without the guard.

The native connection uses that identity's Redis credentials. Current Redis ACLs,
logical SQL grants and metadata visibility rules remain enforced. A retained
snapshot does not preserve revoked SELECT access. Revocation does not retract
already fetched or buffered copies or impose a synchronous in-flight barrier.
Changing session catalog/schema/timezone after acquisition is refused.
Native read-only restrictions also reject DML/DDL, ingestion, temporary objects,
sequences with side effects and unsupported execution APIs on this connection.
Publish changes from another connection. Dataset RPCs are outside this contract.

`retention_ms` is fixed at acquisition, defaults to **900000**, and must be
**120000–3600000**. It includes active readers and cannot renew/extend the
snapshot lifetime. The gateway validates status, native generation and deadline
before connection use and outgoing chunks; the native reader also validates its
view. Expiry, lease loss, unavailable authority and extraction errors are terminal;
the gateway closes the native connection and cannot reopen it for this attempt.
Restart discovery and **all** selected reads in a fresh session. Partial output
remains provisional until the whole extraction succeeds.

Use existing CancelFlightInfo/CancelQuery where available and transport/reader
cancellation. Cancellation control remains accessible while a snapshot reader
holds the connection; existing query owner checks apply. Cancelled/stalled readers
release the stream and terminate the attempt. Explicit release is idempotent and
closes the dedicated connection. It requires readers to be closed first.

Graceful ADBC connection close sends CloseSession. Abrupt client disconnect has
no reliable session-close signal: active stream cancellation cleans up, while
an idle abandoned session is reaped after SessionIdleTimeout (default 10 minutes,
plus the sweep interval). The native fixed deadline bounds retention regardless
of idle traffic. Gateway process death stops native pin renewal; its existing
two-minute lease expires within two minutes of the last acknowledged renewal.
Native acquisition/sweep on subsequent activity performs physical collection;
immediate deletion on an idle Redis server is not promised.

No durable reconnect/resume, cross-replica attachment, migration or generation
attachment is supported. Route the entire attempt to its owning gateway. A lost
cookie, replica change, restart or session expiry requires a new full attempt;
keep the explicit guard installed so recovery cookies cannot permit live reads.

## Exact consistency boundary

The guarantee is the native managed catalog, SQL view definitions and managed
row versions in **one Redis hash slot**. Supported concurrent DML/DDL,
TRUNCATE/DROP and compaction preserve that view. Imported managed copies retain
their captured values but do not improve the earlier application HASH capture.
This is not a coordinated snapshot of independent shards, arbitrary external
HASHes, source TTLs or volatile/system expressions. Direct mutation, eviction
or expiry of driver-owned storage lies outside the protocol and must fail if
its required data/authority becomes unavailable.

## Qualification gate

Run `make test-shared-snapshot` after installing
`requirements/shared-snapshot.txt`, with `FLIGHT_URI=grpc+tls://...`,
`FLIGHT_USERNAME`, `FLIGHT_PASSWORD` and optionally `FLIGHT_TLS_ROOT_CERTS` (PEM
file). Set `FLIGHT_SNAPSHOT_LONG_TESTS=1` for the two-minute expiry case.
Use a disposable Redis Enterprise catalog. Basic TLS/ACL is available in core;
use a qualified composition when exercising additional enterprise features.
No credentials belong in command arguments or committed output.

The Python tests exercise the actual ADBC Flight SQL client: two tables and a
view, exact schemas/values/duplicates, metadata before/after publications,
DML/DDL/TRUNCATE/DROP, repeated updates for compaction, foreign handles, exception
cleanup, expiry and no live fallback. Go tests exercise real TLS Flight handshakes and Redis authentication against a
TLS RESP fixture, plus snapshot ownership with a native-driver test double.
They also exercise connection affinity, failed acquire, lost authority, terminal errors, stream cancellation/stalls and
resource release with test doubles. Neither mocked Go tests nor Python syntax
checks establish enterprise integration support.

Still required before enabling Airbyte: run and record the Python suite and
revoked-grant/Redis-ACL tests with independently authenticated principals, forced
native lease loss, gateway kill/restart and disconnect cleanup through the
qualified deployment; verify compaction actually occurred. The new TLS unit tests
are evidence of transport/security behavior, not native Redis snapshot qualification. Record gateway commit,
composition fingerprint, native/client versions and full results. There is no
qualified gateway/driver/client combination published yet.
