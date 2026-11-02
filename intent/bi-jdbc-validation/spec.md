# BI JDBC validation specification

Use disposable Redis and Metabase instances, with the checked-in community
Flight server and pinned dependencies. Test driver reads/writes/metadata;
test Metabase native and generated reads, schema discovery and writes through
its supported application API. Verify inserted, updated and deleted values
by reading them back. Record tested versions and limitations.

Tableau requires an installed application for application-level acceptance.
If unavailable, report its reads/writes as blocked. Do not infer its behavior
from JDBC or Metabase results. No server feature, commercial implementation,
dependency upgrade, production deployment or external publication is in scope.
