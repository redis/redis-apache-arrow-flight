# Specification

- Query the Go module graph, PyPI package metadata and Maven Central for stable
  updates. Preserve module major versions and the Go 1.27 feature baseline;
  raise the patch minimum and builders to 1.27.4 for GO-2026-6859.
- Check both the latest Redis ADBC release and default branch, and pin the
  release's immutable Go submodule revision. Do not select unreleased main.
- Update active Python requirements and inline script dependencies together;
  preserve historical benchmark fixtures and dated validation evidence.
- Use ADBC's reader-retention contract in all three BindStream call sites.
  Release caller-owned readers on success and binding refusal; leave the
  ingest handler's reader reference owned by its Flight SQL caller.
- Update the dataset-to-SQL example to the current driver's typed-struct
  ingestion contract; verify nested values/types and keep scalar projection as
  an explicit choice. Do not continue asserting unsupported structs.
- Preserve protocol, authentication, publication, memory-budget and stream
  cleanup behavior. No new Flight-owned commercial feature is introduced.
- Verify ownership with checked Arrow allocations, and run community race tests,
  vet, static build, module verification and dependency vulnerability checks.
  Record missing Enterprise/client integration qualification explicitly.
