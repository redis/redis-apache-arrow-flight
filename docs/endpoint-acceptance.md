# Endpoint acceptance defaults

For #57, the requester chose a **10,000-row local baseline** on 2026-10-20
and delegated the remaining defaults. These are starting assumptions for a
small analytical service. Production availability and network targets below
still require deployment validation.

| Dimension | Default |
| --- | --- |
| Result | 10,000 rows; int64 id, int32 group, float64 value, 64-character UTF-8 label |
| Payload | About 88 Arrow bytes/row, 0.84 MiB/result, excluding transport overhead |
| Readers | 16 simultaneous full-result readers; default buffering policy |
| Query mix | Full scans plus aggregate, join, residual sort, distinct and window checks |
| Placement | Clients, Flight and Redis in one region/zone; target RTT at most 2 ms per hop |
| Initial deployment | Two Flight replicas, each 4 vCPU/8 GiB RAM; separate Redis with Search; sticky routing for cookie sessions |
| Availability objective | 99.9% successful eligible requests over 30 days; scheduled maintenance included; capacity refusals count as failures at the promised load |
| Latency objective | At 16 readers, p95 first batch within 1 second and full result within 2 seconds; caller deadline 10 seconds |
| Capacity policy | Buffer by default; use reject mode only with client backoff and a lower explicitly advertised admission capacity |

Latency/availability are proposed operational objectives, not measured guarantees.
Validate at least 100 requests per scenario for p95 and a sustained soak before
using these in a service agreement. The local three-repeat check uses the maximum
observed latency, not a statistically meaningful p95. Start investigating above
2 GiB Flight RSS per replica on this fixture; sampled RSS is not a memory limit.
Observe Redis memory/cursors and client memory separately. A 4 vCPU deployment
must be tested independently of the larger development machine.

## Reproduce the local baseline

Use a disposable Redis with Search at localhost:16379 and a server at
localhost:18815 configured to use it. Install the pinned dependencies from
`examples/python/requirements-smoke.txt`. Start a fresh server per profile to
avoid retained heap from prior larger workloads; supply its PID to sample RSS.

```sh
python examples/python/benchmark.py --server-pid "$FLIGHT_PID" \
  --policy-label buffer --output benchmarks/local-endpoint.json
```

The defaults are 10,000 rows, label width 64, 3 repeats and 16 readers. Each run
creates unique fixtures and removes them. Use a separate fresh server with
`--stream-overflow reject`, then run the same script with its URI/PID and
`--policy-label reject --cases sql_full_scan --repeats 1` to measure refusals.
The policy label records configuration; it does not configure the endpoint.

Raw evidence, versions, source/binary fingerprints and machine details are in
[the local records](../benchmarks/2026-10-20/). Flight RPC and SQL use the same
flat fixture. The RESP comparison uses SCAN plus pipelined HMGET and Arrow
conversion, and applies only to the unordered scan. Timings include Python
checksum work and use warmed connections. All processes share one development
host; no WAN delay or packet loss was emulated. RSS samples every 20 ms can miss
peaks, and exclude Redis/Docker memory. Query order and retained heap affect RSS.
No production sizing or general speedup follows from these results.

## Failure and rollout acceptance

Run `examples/python/failure_probe.py` with an explicitly named disposable Redis
container, a separately running rollback endpoint and an output path. It checks
a one-second request deadline during a backend pause, dataset and SQL recovery
after unpause, and old-reader compatibility with newly written data. gRPC may
surface the expired deadline as timeout or cancellation; the probe records the
observed type and bounds its elapsed time. This is a backend outage probe, not
Redis failover or an availability measurement.

The Go race suites cover slow consumers, cursor/pool cleanup, cancellation across
replicas, lost sessions and restart. Lost cookie sessions must fail before SQL
execution; clients reconnect and restore settings/temp objects. Upgrade ticket
readers together: old readers ignore new cancellation records. On rollback,
discard query tickets and cookie sessions and verify SQL plus dataset reads.
Storage format compatibility does not imply active session compatibility.

Before production acceptance, exercise the assumed 2 ms network and proposed
4 vCPU/8 GiB instances, sustained load, replica draining/restart, actual Redis
failover and the deployment system's rollback. Monitor RPC errors/latency,
health, pool/stream saturation, result refusals, stalled results, Redis cursors
and process memory. #57 remains open for this deployment evidence; the workload
and starting objectives no longer require further user input.
