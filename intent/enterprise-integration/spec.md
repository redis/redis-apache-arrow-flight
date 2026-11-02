# Contract

- Shared backend setup rejects plaintext, absent credentials, invalid connection
  options and disabled certificate verification without logging the URI.
- Unit runs remain backend-free. Integration targets fail if the URI is absent
  and never launch local Redis; `test-cluster` aliases the Enterprise suite.
- Provisioning establishes Redis Enterprise edition, Search, cluster topology
  and active 4x QPF. Do not equate OSS Cluster API detection with Enterprise.
- Python/JDBC/R/BI validation uses the same secured backend through a qualified
  enterprise Flight build. Community TLS/authentication stubs stay intact.
- Public CI provides unit/static/build and offline artifact checks only.
  Private integration CI and a required release check remain external setup.
