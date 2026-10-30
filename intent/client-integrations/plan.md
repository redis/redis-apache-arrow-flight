# Validation record

Date: 2026-10-29 (America/Los_Angeles). Baseline: `84ea021`.

- Built community server with `make build`, listening on `127.0.0.1:18815`,
  against disposable `redis:8.6.2` published on `127.0.0.1:16389`.
- Ran `examples/python/client_compatibility.py --uri grpc://host.docker.internal:18815`
  in `python:3.11-slim-bookworm` with `requirements-clients.txt` versions.
  Passed pandas, Polars and both DuckDB transfer routes. pandas `params=` is
  unsupported; direct ADBC binding/conversion passed.
- Ran `examples/r/flight_sql.R grpc://host.docker.internal:18815 /tmp/libadbc_driver_flightsql.so`
  in `r-base:4.5.2`. Passed DDL, writes, ordered/NULL/empty results and SUM.
  Driver/library versions and installation caveat are in the client matrix.
- Ran `JdbcSmoke.java` in `eclipse-temurin:25-jdk` with JDBC 19.0.0 and the
  README's two JVM memory flags. All checks passed. DBeaver is not installed;
  no application UI validation is claimed.
- In an isolated Python 3.11 venv, attempted installation of
  `apache-superset==6.1.0` and `sqlalchemy-adbc[flightsql]==0.3.0`.
  pip returned `ResolutionImpossible`: SQLAlchemy `<2` versus `>=2.0`.
  A dry run with adapter 0.2.0 found the same conflict. Published PyPI metadata
  also confirms adapter 0.1.0 requires SQLAlchemy `>=2.0`. Superset application
  execution is blocked for these combinations.
- Added local Make targets and dataframe coverage to the existing Test workflow.
  Hosted CI has not been run. Existing Go server suites were not rerun because
  no Go implementation changed.

Follow-up after the user excluded Superset: installed official DBeaver Community
26.2.3 temporarily on macOS arm64 with an isolated workspace, bundled Temurin
25.0.4.1, and custom JDBC 19.0.0 driver. Driver configuration succeeded, but
opening the connection form caused three macOS accessibility serialization
crashes under UI automation, before a connection test could run. A screenshot
interaction attempt also failed. Application browsing/generated SQL remain
unvalidated; the separate JDBC smoke pass still stands. Setup and diagnostic
scope are recorded in the client matrix. Superset follow-up is deferred and
tracked in endpoint issue #41 and upstream sqlalchemy-adbc issue #16.

Remaining acceptance: manual DBeaver browsing/generated SQL. DBI/adbi and
TLS/auth are outside this smoke test scope; Superset is excluded by request.
Maintainer decision: pending.
