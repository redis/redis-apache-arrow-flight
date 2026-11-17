# Specification

Public snapshot qualification covers the actual Python ADBC Flight SQL client,
exact two-table/view data and metadata through supported concurrent publications,
native fixed expiry, observed installed compaction, forced loss of this test's
native pin, abrupt gateway death/restart and natural native lease collection.
Real TLS Flight/Redis Go tests cover independently authenticated principals,
stolen-cookie/handle rejection, SQL SELECT grant and Redis ACL revocation, idle
disconnect cleanup, pending/active cancellation and early reader release.

The Python example must preserve a primary extraction/cancellation error if
release also fails, while still reporting release failure after successful reads.
Fault tests explicitly opt in and use isolated disposable storage. No credentials
or arbitrary native attachment tokens are published. No durable resume is added.

An isolated Airbyte change exercises its real check/discover/read and generator
cleanup over Flight, with an explicit protocol-1 probe and no live fallback.
Published connector releases remain unsupported until that change is accepted.

Upload merging is enterprise optimization. The private buffer holds at most
about 2 MiB of Arrow buffer capacity and 1,024 pending record references;
concatenation can temporarily allocate comparable output. Large backing buffers
and extension arrays bypass merging; changed (including nested) dictionaries
flush. Preserve atomic publication, all error cleanup and stored-batch ranges.
Community uploads keep their existing behavior. Shared public benchmarks include
100,000 rows uploaded as 100 independent 1,000-row numeric batches. Compare
before/after write/read timings, IPC sizes and generation HASH MEMORY USAGE.
