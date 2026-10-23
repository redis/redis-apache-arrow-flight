# Implementation contract

Community compiles from public source. Paid feature implementations and their
original tests are removed/replaced at compile-time boundaries. Enterprise
composition restores all implemented capabilities using a qualified source map.
No new backup/migration/Active-Active runtime is claimed. Keep SQL/protocol
correctness, Arrow types/ownership, deadlines, errors and cleanup in the shared
core. Serial results stay correct; optional telemetry is absent. Requested
security/auth/TLS operations fail explicitly when unavailable. Do not treat
missing feature data as health, zero, successful mutation or completed backup.

Expose build capabilities. Keep module dependencies public; enterprise composition
verifies the full source fingerprint and avoids module cycles. Preserve original
copyright/license notices and all moved test assertions. Commercial packaging is
per the requestor's selected model, not a new license or entitlement system.

Basic endpoint/protocol/shard routing, authoritative row-ID lookup and per-table
SQL integrity guards remain core contracts. The automation boundary covers
optional orchestration/proactive recovery, not disabling those mandatory paths.
