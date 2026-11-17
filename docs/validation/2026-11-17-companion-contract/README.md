# Companion integration qualification — 2026-11-17

Status: v0.0.29 qualification in progress after its release during the audit.
The v0.0.28 results below are historical until superseded by final results.

This records the acceptance audit for [Flight #61](https://github.com/redis/redis-apache-arrow-flight/issues/61)
and original [native #214](https://github.com/redis/redis-adbc-driver/issues/214).
The resulting [companion contract](../../companion-contract.md) is the integration
boundary. It does not expand protocol, production or application support.

## Recorded sources and environment

[Initial v0.0.28 results](results-v0.0.28.json) bind the new Python test file and gateway
binary by SHA-256, record exact Python/client versions, and include complete
public-module/replaced-file/private-overlay fingerprints. The public base is
`ee39b4d` plus this test/documentation change. Go runtime source is byte-for-byte
equivalent, under the enterprise builder's complete module guard, to qualified
public `a2fa5ae0aebe9d74f9a82e096b3f84f3673db60d` and private candidate
`a6c640a84ccd3467d6c3122fbc3000e48b099172`. No Go source or dependency changed.
The builder was rerun for the new gateway and rejected no source mismatch.

The earlier [complete serialized backend results](../2026-11-17-snapshots/revocation-results.json)
record 206 passing enterprise named tests/subtests (one community-only skip) and
154 community passes (one enterprise-only skip). The
[hosted enterprise qualification](https://github.com/Redislabs-Solution-Architects/redis-apache-arrow-flight-enterprise/actions/runs/44184407701)
passed its full race suite, live SQL read/write smoke, TLS/ACL verification and
composition receipt. Those source-matched results are reused, not presented as
a new Go rerun. The earlier failed revocation-timing run and invalid overlapping
local attempts remain disclosed in the [follow-up](../2026-11-17-snapshots/revocation-followup.md).

The five **new** tests passed with Python **3.14.10**, Python ADBC Flight SQL and
manager **1.13.0**, PyArrow **26.0.1**, pytest **9.2.0**, redis-py **8.1.0**.
They loaded only Apache Flight clients, not the native Redis shared library.
The rebuilt Linux arm64 gateway uses Go 1.27.4, Arrow Go 18.9.0, ADBC Go 1.13.0,
gRPC 1.85.0 and released native Redis ADBC v0.0.28 (`b63a010d88d2`).

A fresh disposable Software fixture used `redislabs/redis:8.2.0-78.18`, Redis
8.6.2, Search, three active primary shards, verified active **4x QPF**, 1 GiB,
no replicas and noeviction. Provisioning verified Search writes/reads and rejected
plaintext, missing/wrong credentials and untrusted CAs. Both client→Flight and
Flight→Redis used authenticated verified TLS. Credentials, certificates, raw
logs and operational identifiers remain ignored. The gateway key prefix was the
default `arrowflight:`; only this isolated fixture was used for expiry injection.

## Original #214 acceptance map

| Original item | Concrete contract and executed evidence |
| --- | --- |
| Choose companion ownership/deployment; publish method matrix | [Service ownership](../../companion-contract.md#service-ownership-and-deployment), [profiles](../../build-profiles.md), [method matrix](../../flight-capabilities.md). Existing separate gateway, no native Flight dependency. Both guarded build profiles have recorded passing results. |
| Define sessions, TLS/authentication, Redis identity isolation | Contract identity/lifecycle sections; source-matched `TestIntegrationSessionOwner`, `TestIntegrationSessionReplicaLoss`, `TestIntegrationSnapshotIndependentPrincipal`, `TestSecurityFlightTLSAuthAndSnapshotOwnership`. The new real-network Python cases authenticate every client over TLS. |
| Define prepared metadata, errors, cancellation and result lifetime | Contract execution/lifetime sections; `TestIntegrationPreparedTypes`, `TestIntegrationPreparedScripts`, `TestIntegrationSQL`, `TestIntegrationQueryCancellation`, plus the new natural-shortened-TTL check. |
| Autocommit, ingestion, quotas/backpressure; native partitions versus datasets | Descriptor table and resource limitations in the contract; `TestIntegrationIngest`, `TestIntegrationStreamResults`, `TestIntegrationDatasets`; new Python ingestion, independent Flight-driver partition replay, dataset transfer and unsupported transaction tests. |
| Standard Flight SQL client for SQL, metadata, binding and unsupported methods | Apache Go Flight SQL in the source-matched backend suite and independent Python ADBC 1.13.0 now. Exact schemas/type-name metadata, duplicates, NULLs, Unicode, integers above 2^53, empty results, prepared parameters, create/append ingestion and explicit unsupported operations. |
| Exact Arrow data, independent clients, expiry, cancellation/early release, reconnect, credential isolation | Five new Python cases below plus the detailed lifecycle map and existing actual snapshot transport/fault checks. All endpoint streams are consumed where a complete result is claimed. |
| Matched performance comparisons; typed export remains separate | [Existing comparison](../../performance.md) keeps its October 30 source/hardware/workload and complete validation. No new benchmark or current-build speedup is claimed. Native #195, Flight #28/#35 remain separate. |

Names provided by an enterprise overlay are identified by the composed suite,
not assumed to exist in the public community implementation. The public module
and private source fingerprints in results.json identify that exact composition.

## Lifecycle and error evidence

| Behavior | Source-matched executed test / practical limit |
| --- | --- |
| Read tickets replay as fresh reads; writes do not execute again | New `test_flight_driver_partitions_are_deferred_and_replayable`; existing `TestIntegrationWritesRunOnce`, including repeated result fetch and retained-result TTL assertions. Fully qualified ordinary SQL is used; no session portability is inferred. |
| Ticket expiry and unknown control state | New `test_query_ticket_expiry_is_terminal` verifies the issued TTL is positive and at most ten minutes, then shortens only its exact newly issued record to 100 ms and waits for real Redis expiry. Both subsequent reads return ADBC NOT_FOUND. This is not a ten-minute wall-clock soak. Existing cancellation expiry case removes its test record to simulate unavailable authority. |
| Pending/active cancellation and resource cleanup | `TestIntegrationQueryCancellation/before_fetching_and_repeated_cancellation`, `/active_stream_across_replicas_frees_resources`, `/completion_races_cancellation`; active count and Search cursor cleanup asserted, Arrow checked allocator used. |
| Independent authenticated identities | Enterprise `TestIntegrationCancellationIdentity` refuses another user's cancel/fetch; `TestIntegrationSessionOwner` refuses foreign session access. Snapshot independent-principal test uses real secured transport and denied cookie/handle reuse. |
| Early release, disconnect and slow consumer | Enterprise `TestIntegrationStreamResults/a_cancelled_read_frees_the_connection`, `/a_client_that_stalls_gets_an_error`; real-backend `TestIntegrationSnapshotReaderCleanup` and `TestIntegrationSnapshotDisconnectAndIdleExpiry` assert pin cleanup and no live fallback. These are the recorded single Software deployment, not a new multi-node Cluster API qualification. |
| Idle expiry, wrong replica, restart and reconnect | `TestIntegrationSessions/idle_sessions_close`, `TestIntegrationSessionReplicaLoss`; actual Python snapshot SIGKILL/restart case from the prior receipt waits for the real native lease. No persistent session or durable snapshot resume. |
| ACL/grant changes and credentials | Enterprise `TestIntegrationRedisACL`, `TestIntegrationStaleCredentials`; public secured snapshot independent-principal and SQL grant revocation tests. Revocation propagates asynchronously; the retained connection must observe PermissionDenied and then remain failed. |
| Explicit unsupported operations | Source-matched Go `TestFlightUnsupportedCapabilities` checks DoExchange, BeginTransaction and Substrait RPC status. New PyArrow DoExchange and ADBC transaction-control checks observe explicit unsupported errors over verified TLS. |

Some Go protocol/lifecycle cases use in-process Flight transport; they are not
misrepresented as a new external network/client run. The added Python cases
supply real network/TLS standard-client coverage. Snapshot-specific natural
lifetime, compaction, death and cleanup results retain their earlier client
versions and recorded revisions.

## New executable checks

All five cases in `tests/integrations/test_companion_contract.py` passed without
skips: SQL/metadata/binding/ingestion, deferred/replayed Flight-driver partitions,
PyArrow dataset endpoints/DoExchange, unsupported transactions and ticket expiry.
The expiry case requires explicit destructive-test opt-in; absence is a skip,
never qualification. Tables/datasets are uniquely named and cleaned up.

Install `requirements/shared-snapshot.txt` into Python 3.14, provision the secured
Software backend and a matching enterprise composition, then supply credentials
and trust privately through the environment:

```sh
# FLIGHT_URI=grpc+tls://..., FLIGHT_USERNAME, FLIGHT_PASSWORD,
# FLIGHT_TLS_ROOT_CERTS=<CA PEM path>; never put values in committed commands.
# FLIGHT_INTEGRATION_SCOPE=<shared scope identifying the disposable backend>.
make test-companion-contract
# For the isolated lease-expiry case only:
# FLIGHT_SNAPSHOT_FAULT_INJECTION=1, authenticated REDIS_URI=rediss://...,
# REDIS_CA_FILE=<CA PEM path>; serialize all publishers on this database.
```

The default integration runner scope/lock directory must be shared by every
cooperating process/container using that catalog. Separate hosts need separate
disposable databases. No SQL mutation is retried by these checks.

## Remaining boundaries

The five #61 gates are mapped by the contract, complete original-item map,
source-matched lifecycle/security results, secured Software/4x-QPF checks and
links from capability/client documentation. This does not close #28 (bounded
execution), #35 (deployment/performance), #17/#38 (JDBC), #41 (Superset) or #57
(Datasets). Historical BI/application sweeps and retained-endpoint guide recipes
are not current-run application acceptance. No gateway/connector release,
production rollout, failover, cross-shard snapshot or rollback certification is
created by publishing this source contract.
