# Plan and evidence

Implemented cancellation with single-key Lua state transitions and atomic SET EX
registration; session loss/recovery; opt-in admission rejection; standard Python
bridge and pinned client checks; protocol/operations/client documentation.

Tests cover pending/active/cross-replica cancellation, ownership, completed/write
results, expired records, completion races, read-only SQL fallback, malformed/lost
cookies, wrong replica/restart/expiry/CloseSession, no execution after session
loss, temporary-object cleanup, default buffering and reject-before-execution.

Local evidence on 2026-10-20:
- Final full standalone and Cluster race suites passed, including the script-ACL
  regression: server suites 23.995s and 23.714s, respectively. Static analysis and
  credential boundary tests also passed.
- Python quickstart and dataset-to-SQL example passed with PyArrow 26.0.0,
  ADBC Flight SQL/manager 1.12.0, pandas 3.0.6.
- JDBC 19.0.0 smoke passed in Temurin Java 25, including the observed empty
  JDBC getTypeInfo limitation; direct Go clients verify endpoint type metadata.
- Local timeout/recovery and old-reader SQL/dataset checks passed with a paused
  disposable Redis and a baseline server built from 4120689.
- Benchmark script and raw records live under examples/python/benchmark.py and
  benchmarks/2026-10-20/. Measurements are synthetic local evidence, not SLOs.

#50 remains open for driver execution-memory bounds beyond scan admission.
#57 uses the requested 10,000-row baseline with 16 readers and documented
default objectives in docs/endpoint-acceptance.md. Deployment and availability
validation remain open; no further workload clarification is required.
No external BI application or production deployment was validated.

Review found that a stalled transport could retain the cancellation lease after
its backend cursor was freed. Batch handoff acknowledgments now bound that wait
by StreamStallTimeout. A shared one-shot stall reporter prevents double-counting.
A DoGet ownership registry releases the SDK's current batch after a failed write;
a checked-allocator test exercises the real adapter's transport-error path.
The slow-consumer integration test checks query leases before the reader resumes.
