# Plan and evidence

1. Review issue contracts, public/private source boundaries and existing results.
2. Record baseline dataset measurements before implementation.
3. Implement enterprise merging and bounded-memory/dictionary/error/range tests.
4. Add real-backend snapshot faults, security/lifecycle qualification and preserve
   primary Python errors. Exercise the actual Airbyte source independently.
5. Reconcile the isolated enterprise composition with the current public core.
6. Pin the latest released native driver, run both profiles and secured backend
   checks, and record exact results and outstanding release/review gates.

Dependency check on 2026-11-17: latest completed native release v0.0.28,
release commit b63a010d88d2b69707594cb9dc063992dac06b32; default branch
2b6634e0c7c92d0c8148dff62225671cb7a15c4e. Selected the immutable release,
Go submodule v0.0.0-20261117191025-b63a010d88d2, requiring Go 1.27.3.
Historical evidence remains tied to its earlier native revision.

Final results: [dated evidence](../../docs/validation/2026-11-17-snapshots/README.md).
Review: source diffs checked for public/private boundaries, error propagation,
Arrow ownership, unsafe client overrides and retained resource limits. Runtime
checks passed; this is local review, not human approval or release certification.


Dependency follow-up: checked published module/PyPI metadata and official Go and
setup-uv releases on 2026-11-17. Go 1.27.4, Arrow Go 18.9.0, ADBC Go 1.13.0,
gRPC 1.85.0, PyArrow 26.0.1 and setup-uv 10.3.3 were selected, with compatible
transitive module updates. All seven refreshed Python profile environments
resolve and their automated runtime profiles passed. Both Go backend profiles,
snapshot faults and independent Airbyte tests passed on the refreshed stack;
earlier results remain separately retained and do not qualify newer dependencies
by themselves. Airbyte's concurrent upstream dependency refresh is incorporated
before its final independent rerun.
