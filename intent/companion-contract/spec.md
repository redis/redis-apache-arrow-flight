# Specification

Document service ownership, profiles, deployment, SQL/metadata/binding/ingestion,
autocommit/unsupported operations, ticket replay and lifetimes, identity/session
boundaries, quotas and cleanup. Explain the separate wire formats and owners of
native Redis ADBC partitions, Flight SQL tickets, Flight-driver ADBC partitions
and PATH dataset endpoints. Never imply interoperability or snapshot attachment.

Map every native #214 item and Flight #61 gate to named tests, immutable recorded
results and explicit limitations. Supplement existing Go and snapshot evidence
with independent Python ADBC/PyArrow clients over verified TLS/Redis ACL using
Python 3.14. Preserve exact schemas/types/NULLs/empty/duplicate/64-bit values,
metadata, prepared binding, ingestion, unsupported methods and independent reads.
No new production implementation, performance claim or release is in scope.
