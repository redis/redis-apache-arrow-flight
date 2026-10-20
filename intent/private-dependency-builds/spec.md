# Specification

- Pin existing module versions/checksums. Restrict direct/private fetching to the Redis ADBC fork.
- Accept optional local credentials through REDIS_ADBC_TOKEN or REDIS_ADBC_TOKEN_FILE; otherwise use the caller's existing Git credential setup.
- Supply Docker credentials through a BuildKit secret; copy no credentials into layers.
- Wire a restricted REDIS_ADBC_READ_TOKEN repository/organization secret into test/reusable/release workflows. Ordinary GITHUB_TOKEN cannot read the other private repository; fork PRs require trusted branch validation.
- Test credential host/protocol/action boundaries and scoped module settings, then build from a clean Docker cache.
- Repository secret provisioning is an external prerequisite. Report it accurately rather than treating code support as proof that hosted builds work.
