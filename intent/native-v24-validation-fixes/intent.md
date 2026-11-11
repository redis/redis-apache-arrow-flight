# Intent: repair four backend validation blockers

Date: 2026-11-11. Requester: current chat, "fix those 4 issues so that validation
can be completed". Baseline: `56c90165a308cd13bbeca34ff2533d43e353daf6`.
Issues: #68 (cursor assertions), #69 (materialized-view metadata), #70 (SIGTERM
fixture), #71 (managed-catalog test isolation). Status: implemented; complete
real-Redis Go and Python reruns passed. No driver upgrade or production change.

Retain the original resource-cleanup and shutdown assertions. Resolve current
native resources instead of assuming legacy index names; count every table
segment and verify captured temporary indexes are physically reclaimed. Validate
advertised materialized-view metadata with a real fixture. Serialize publishers
in cooperating validation runs rather than retrying potentially completed writes.

This is core protocol/storage verification, not paid feature implementation.
Original optimization-specific tests remain gated with their assertions intact.
Maintainer review and release qualification are separate from local test success.
