# Dependency update specification

Prepared 2026-10-22. Input: [intent.md](intent.md).

- Query the Go module proxy and PyPI for available stable versions. Update
  versioned requirements within their existing major versions, then tidy the
  module graph and regenerate checksums. Allow the newer generated Go modules
  selected by those requirements.
- Keep the private Redis ADBC replacement and driverbase revisions unchanged;
  these are source revision pins without a stable minor release stream.
- Raise the Go minimum and Docker builder to the latest Go 1.26 patch,
  1.26.10. Do not require a new Go feature release.
- Update the Python smoke environment's pandas pin from 3.0.6 to 3.1.0.
  Other pinned Python packages, Arrow Go and ADBC Go are already current.
- Preserve runtime behavior and existing tests. Validate on Go 1.26.10 using
  unit/race tests, standalone and cluster integration tests, vet and build.
  Check Python clients against the updated server and audit both dependency
  graphs for known vulnerabilities.

Security scan results describe the databases at scan time; they do not
guarantee the absence of vulnerabilities. Container image and JDBC dependency
audits are outside this Go/Python library refresh.
