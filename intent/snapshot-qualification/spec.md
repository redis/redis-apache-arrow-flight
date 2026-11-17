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

## Dependency refresh and shared catalog inspection

The requester additionally asked to check newer dependencies, update compatible
releases and reduce custom code. Upgrade the Go/Arrow/ADBC/gRPC stack and pinned
Python client/integration requirements, synchronize toolchain and workflow pins,
and requalify the final paired composition. Keep the native release pin immutable.

Replace three Go catalog readers/key-escaping/descriptor validators with one
read-only `internal/nativecatalog` implementation. Reject unknown protocols,
missing roots and malformed descriptors; do not guess legacy index names. Reuse
the session token validator for cancellation IDs. Preserve the failed-send batch
ownership workaround: Arrow 18.9's Flight SQL writer still needs its protection.
Retain exact native publication-refusal classification and custom ingest ordering;
upstream convenience helpers do not preserve those contracts.

Datasets 5.2.0 retains the independently reproduced zero-shard persistence defect.
Keep strict expected failures for the verified exact signatures in 5.1.0/5.2.0;
other failures and unexpected passes must fail. This does not close issue #57.
