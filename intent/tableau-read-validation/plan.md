# Tableau read validation plan and review

1. Prepare a separate branch from current main after Metabase PR #50 merged.
2. Publish the existing manual Tableau read result and reproduction steps.
3. Update the tool matrix, client guide, compatibility history and testing scope.
4. Check links and whitespace, then open a PR.

Recorded validation: Tableau Desktop Free Edition 2026.3 with JDBC 19.0.0
discovered `redis.public.bi_jdbc_validation`, its three columns, and displayed
three expected rows including NULL. The fixture and services were removed.
This PR documents that executed run; it does not assert a new application rerun.

Self-review: no runtime/dependency changes, credentials or unsupported read
claims. Initial SQL and write-back support are outside acceptance. No screenshot
or query log was captured; the evidence is a manual UI record. Validation
commands and final results are recorded in the PR description. Maintainer
review/approval remains unrecorded.
