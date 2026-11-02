# BI JDBC validation plan and results

1. Inspect existing validation, applications and driver documentation.
2. Build source and launch isolated Redis/Flight services.
3. Run JDBC smoke, then Metabase API smoke with community plugin 0.8.0.
4. Record read/write results, update compatibility docs and remove services.

Completed 2026-11-01: JDBC smoke passed on Java 25. Metabase 0.64.2.9 passed
reads, metadata sync, generated filters/aggregates and custom SQL Actions for
INSERT/UPDATE/DELETE with read-back. Native DDL via the read-query endpoint
failed. Tableau application testing is blocked because it is not installed.
Python syntax compilation and whitespace checks passed; runtime Go source did
not change, so the Go suites were not rerun. Disposable services were removed.

Self-review found no runtime changes or credentials in artifacts. No maintainer
approval is asserted. See [full evidence and reproduction](../../docs/validation/2026-11-01-bi-jdbc/README.md).
