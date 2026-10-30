# Release and maintenance guide

Status: repository-derived operational reference and proposed response workflow. No production deployment or alerting is configured by this document.

## Build and release behavior

[Makefile](../../Makefile) builds a static binary with an embedded version. `make dist` recreates `dist/` and creates archives/checksums for Linux amd64/arm64 and macOS arm64. [Dockerfile](../../Dockerfile) produces a nonroot distroless image with a built-in healthcheck.

[release.yml](../../.github/workflows/release.yml) runs on `v*` tag pushes, manual dispatch for an existing tag, and selected release-related PR paths. It calls the test workflow, builds archives, checks architecture/checksums, runs binaries on their target platforms, and builds the multiarch container. PR runs do not publish. Tag/manual runs push to GHCR and attach archives to GitHub releases. A manual rerun replaces assets using `--clobber` and republishes image tags. A tag containing `-` is treated as a prerelease.

Image publishing depends on tests; release attachment waits for both image and native-binary verification. Thus image publication can occur before native-binary verification completes. Do not describe the entire release as one atomic publish gate.

Publishing release artifacts is distinct from deploying to production. No production deployment job, environment approval configuration, or automatic rollback pipeline is visible in this repository. Do not infer production readiness from a release tag alone.

## Deployment checks

Before a deployment, record the exact source SHA, binary/image version or digest, configuration, and known-good rollback artifact. Confirm the following for the target environment:

- SQL has a Search-enabled Redis. Dataset-only use has different backend requirements.
- `auth=none` shares the URI credentials across clients. For separate identities, configure Redis ACL authentication and TLS. Protect the token secret; replicas need the same secret for shared bearer tokens.
- Session cookies refer to process-local session state. Review load-balancer affinity and reconnect behavior for session-dependent workloads; shared bearer keys do not share sessions.
- Metrics bind only where intended; their HTTP endpoint has no application authentication.
- Container probe settings match the server. The Docker healthcheck reads environment configuration, not server flags supplied separately.
- The orchestrator gives shutdown more time than the configured 25s default drain; Compose uses 30s.
- Aggregate cursor usage across replicas and other Redis clients remains within index limits. Per-server limits are insufficient by themselves on cluster.
- Dataset storage compatibility has been checked before mixed-version traffic or rollback.

See [Server operations](../server-guide.md#deployment-and-operations), [healthcheck command](../../cmd/redis-flight-server/healthcheck.go), [server lifecycle](../../server/server.go), and [dataset storage](../../server/datasets.go).

## Available signals

The gRPC health service reports cached Redis PING state, checks every five seconds by default, and reports NOT_SERVING from shutdown start. Health requires no login and does not exercise SQL, Search, every shard, every identity, or dataset decoding. On cluster, its seed connection stands for the backend check.

The optional Prometheus endpoint exposes these project metrics, plus Go/process collectors:

| Signal | Investigation |
| --- | --- |
| `redis_flight_redis_up` | Backend reachability, configured credentials, network, TLS |
| `redis_flight_calls_total{method,code}` and `redis_flight_call_duration_seconds` | Failure codes and latency by call type; transfers are timed until completion |
| `redis_flight_index_cursor_limit_errors_total` | Shared Search cursor exhaustion; inspect concurrent readers and other replicas |
| `redis_flight_query_streams_open`, `redis_flight_queries_read_whole_total{reason}` | Streaming pressure and fallback reasons: query, session, user_limit, table_limit, index_cursors |
| `redis_flight_result_memory_bytes`, `redis_flight_result_memory_refusals_total` | Accounted fallback-result pressure; compare with total process memory separately |
| `redis_flight_query_results_stalled_total` | Slow or disconnected consumers and stall timeout |
| `redis_flight_pool_connections{state}`, `redis_flight_pool_wait_seconds` | Pooled connection use and waiting |
| `redis_flight_sessions_open`, `redis_flight_session_connections`, `redis_flight_sessions_full_total` | Session pressure, including connections outside the pool |
| `redis_flight_dataset_rows_read_total`, `redis_flight_dataset_bytes_read_total` | Dataset read workload |
| `redis_flight_dataset_rows_written_total`, `redis_flight_dataset_bytes_written_total` | Committed upload workload |
| `redis_flight_build_info{version}` | Served version |

Evidence: [metrics.go](../../server/metrics.go), [health.go](../../server/health.go). Metric labels intentionally omit usernames/table names. No production baseline, alert window, threshold, scrape configuration, or SLO is checked in; measure a representative baseline before setting thresholds.

## Incident response and return to planning

1. Capture timestamps/timezone, deployment version, symptom, affected interface/client, gRPC code, and sanitized metrics/logs. Separate observations from hypotheses in an [incident record](incident-template.md).
2. Check health and a representative authorized operation. A green PING alone cannot establish SQL or dataset correctness.
3. For capacity failures, inspect fallback reasons, pool waits, open streams, cursor errors, stalls, and process memory. Avoid raising every limit together; consider the resulting Redis and server memory costs.
4. For auth failures, check credential validity, token expiry/key consistency, and ACL permissions without recording passwords or bearer tokens.
5. For dataset failures, inspect metadata generation, payload type/layout, expiry, and partial/corrupt state using appropriate access. Preserve sequence keys and evidence; avoid destructive cleanup while diagnosing.
6. Propose a bounded corrective change in `intent/<change-id>/intent.md`, then attach spec/plan, regression evidence, review findings, and the release record.
7. Record recovery evidence from the affected operation and metrics, then add the regression test or future agent evaluation case that would prevent recurrence.

These steps are a manual runbook. No scheduled watcher, autonomous diagnosis, external message, or automatic corrective action is enabled.

## Rollback constraints

Keep a known-good artifact and configuration available, drain the affected instance, restore the compatible artifact/configuration through the environment's release mechanism, and verify health plus SQL/dataset/client behavior before restoring traffic. Record the operator decision and results. A concrete production command cannot be supplied until its deployment system is known.

Current readers support legacy LIST generations and current HASH generations; older LIST-only readers fail on new HASH payloads. After new-format uploads, prefer a reader-compatible rollback build or a forward fix. Pause uploads during a storage-version transition until all replicas can read the resulting format. Reverting a server binary does not undo SQL mutations, dropped/expired datasets, or a storage migration. Restarting also loses process-local sessions and invalidates random-key bearer tokens; plan reconnection accordingly.

## Cancellation/session rollout

New cancellable query tickets depend on leased Redis control records and the new
readers. Upgrade all readers before relying on explicit cancellation across a
load-balanced fleet; old binaries ignore the cancellation ID. During rollback,
discard outstanding tickets and reconnect clients. No dataset or SQL storage
format changes are introduced by this endpoint lifecycle work.

Session cookies reserve process-local metadata. Wrong-replica, restart, expiry
and closed-session requests now fail with ABORTED before executing and receive
a recovery cookie; restore application settings/temp objects explicitly before
retrying. Preserve session affinity. Sharing the authentication token secret
permits bearer authentication across replicas but does not migrate sessions.

--stream-overflow=reject is an opt-in capacity policy for eligible scans; the
default buffer policy is retained. Neither choice caps all driver execution
memory for materialized operators. Use the measured benchmark methodology and
production workload evidence when setting capacity targets.

See [endpoint acceptance defaults](../endpoint-acceptance.md) for the requested
10,000-row workload, initial operational objectives and local benchmark method.
