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

Rerun on 2026-10-30: rebuilt `4a26c34` against fresh disposable Redis 8.6.2.
JDBC 19.0.0 / Temurin 25 passed all smoke checks. Installed DBeaver 26.2.3
reproduced the accessibility crash twice; the second attempt reached Test
Connection's “Set connection defaults” before crashing. Application connection,
browsing and generated SQL remain unvalidated. See the dated rerun in the
client matrix for diagnostic details. Temporary test services were stopped.


Manual follow-up on 2026-10-30: the user connected successfully using DBeaver
Community 26.2.3, bundled Java 25.0.4.1 and JDBC 19.0.0 with Generic driver type.
Screen inspection confirmed navigator browsing to `redis/public/dbeaver_sample`
and a quoted SELECT returning Apples/10, Oranges/20 and Pears/30. This supersedes
the application connection blocker for manual use; accessibility automation
remains affected. Column expansion, table Data tab, filtered aggregates, writes
from the UI, TLS/auth and large-result UI paging remain unverified. Test services
were restarted and left running for the user's manual testing.

Documentation follow-up requested by the user on 2026-10-30: add the screenshot
walkthrough and direct Maven Central JDBC 19.0.0 JAR link, use Generic rather than
SQLite driver type, document a fresh database's lack of tables, and publish a PR.
Based on `4c0eba6`; implementation in `docs/dbeaver.md`, client guide, README and
client evidence. Validation: link/anchor checks, exact JAR URL verification,
SQL fixture/query checks and whitespace checks; no server implementation change.
All seven SQL statements extracted from the guide passed through JDBC 19.0.0 /
Temurin 25 using a unique temporary table, including the quoted ordered SELECT
and filtered aggregate; the temporary fixture was dropped. This is JDBC evidence,
not a new DBeaver UI pass. Local links/anchors and `git diff --check` passed.
Maintainer review pending.
