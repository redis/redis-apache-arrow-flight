# Plan

1. Audit seven remaining issues and original #214; select #61 and verify upstream blockers.
2. Inspect actual routing/handles/native partitions and existing qualification receipts.
3. Add only missing secured standard-client acceptance and a repeatable make target.
4. Run against a disposable Software/Search backend with verified TLS/ACL and active 4x QPF.
5. Publish the contract, gate-to-test map, source fingerprints and sanitized evidence;
   link capability/client/build documentation and retain broader issues #28/#35.
6. Review, run docs/checks, create a PR, follow CI and close #61 only after its gates pass.

Status: contract, complete acceptance map, five secured standard-client cases,
make entrypoint and strict docs build completed. Exact source/overlay fingerprints
match the reused full backend qualification. Local review found no runtime,
credential, paid-source, replay or resource-contract changes. PR review/CI and issue disposition follow;
no human approval or release is implied.

A new stable native release v0.0.29 landed during the audit (22:18 UTC,
2026-11-17), after the initial v0.0.28 checks. Per CLAUDE.md, select its
immutable release commit 30a9eefbf13d19b3e3129171f8ecdc5579b852a2,
Go submodule v0.0.0-20261117215830-30a9eefbf13d, requiring Go 1.27.4.
The Go/report SourceType rename has no gateway references. Protocol stays17;
no old-storage migration or downgrade is inferred. Requalify both profiles,
new secured clients and actual snapshot faults with a new paired manifest;
the earlier fingerprint-equivalent v0.0.28 results remain historical.
