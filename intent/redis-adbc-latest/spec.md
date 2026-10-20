# Specification

Input: [intent.md](intent.md). Prepared 2026-10-20.

- Check the upstream default branch, latest release, and Go submodule resolution. Pin an immutable revision in both the require and replacement entries; regenerate checksums with Go tooling.
- Latest observed release: `v0.0.14` (2026-10-08). Latest observed default-branch commit: `a66467bc5e6a34dbece7c85086d883d5ed62a911` (2026-10-20). Go submodule version: `v0.0.0-20261020162618-a66467bc5e6a`. The initial `@latest` response resolved an older October 8 revision, so resolve the verified SHA explicitly.
- Keep Apache ADBC, Arrow, and unrelated direct dependencies unchanged unless required by the driver. Record future update instructions in CLAUDE.md.
- Run unit/static/build checks and standalone/cluster integration tests against disposable Redis. Investigate changed behavior rather than weakening tests.
- File actionable GitHub issues with existing behavior, expected outcome, acceptance criteria, and references. Mark optional protocol features and data-model changes as design decisions, not conformance defects.

The driver was unavailable through the public checksum service. Download it with existing GitHub credentials and module-scoped GONOPROXY/GONOSUMDB settings; do not disable verification globally or store credentials in the repository.
