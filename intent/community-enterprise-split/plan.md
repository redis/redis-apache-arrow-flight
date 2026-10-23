# Execution plan

1. Extract paid implementation source and original feature tests; retain public
   symbol/type contracts and safe basic/no-op implementations.
2. Compose enterprise source using a fingerprint-checked Go overlay, using the
   same full public driver dependency in both Flight profiles. Prove no private dependency
   resolution in community.
3. Verify stubs/capabilities and fail-closed behavior; validate complete SQL results
   and both build artifacts. Run unit suites and Software/4x integration, recording
   every failure/skip without relabeling runtime failures as successful validation.
4. Update dependency pins, build/use documentation, evidence and review PRs.
