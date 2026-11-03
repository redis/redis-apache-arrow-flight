# Apache Superset: choosing a Redis connection

[All integration guides](integrations/README.md).

Guidance checked on 2026-11-01. Choose the connection path for your Superset
version:

| Superset version | Recommended path | Evidence and limits |
| --- | --- | --- |
| 6.1 | Connect directly with the Redis ADBC driver's Superset integration | The driver supplies a SQLAlchemy 1.4-compatible dialect. The tested Flight SQL adapter has incompatible dependency requirements. |
| 7 | Flight SQL is a candidate when a remote Flight endpoint is needed | The SQLAlchemy 2 migration removes the known dependency blocker. SQL Lab API reads and writes passed in a 7.0.0 development runtime; browser and metadata discovery remain outside the new run. |

## Superset 6.1: direct Redis ADBC

Use the Redis driver's `superset` extra in a dedicated Python 3.12 environment:

```sh
python -m pip install 'apache-superset==6.1.0' 'adbc-driver-redis[superset]'
```

Install the package in every web and SQL Lab worker environment and restart
the workers. In Superset, add a database with this SQLAlchemy URI:

```text
redis+adbc://host:6379/0
```

Use `rediss+adbc://` for TLS. This path loads the native Redis ADBC driver in
the Superset worker and connects directly to Redis; no Flight server is needed.
Redis Search is required. Writes autocommit, and rollback does not undo them.

Follow the upstream [SQLAlchemy and Superset guide](https://github.com/redis/redis-adbc-driver/blob/main/docs/python-integrations.md#sqlalchemy-and-superset)
for current installation and platform requirements. Keep the Superset 6.1
environment separate from the driver's `sqlalchemy` extra and newer analytics
dependencies. Upstream recorded checks cover discovery, dataset queries and the
SQL Lab cursor path; they do not establish browser-rendered chart coverage.
See the [upstream evidence summary](https://github.com/redis/redis-adbc-driver/blob/main/docs/data-engineering-adbc-support.md#testing-status).

The tested `sqlalchemy-adbc[flightsql]` versions 0.1.0–0.3.0 require SQLAlchemy
`>=2.0`, while Superset 6.1.0 requires `>=1.4,<2`. Their installation fails with
`ResolutionImpossible`; see the [local validation record](client-compatibility.md#flight-client-validation-on-2026-10-29).
Do not force incompatible dependencies with `--no-deps`. This is a limitation
of that adapter/version combination, not a Flight SQL protocol limitation.

## Superset 7: Flight SQL candidate

Superset's [SQLAlchemy 2 migration](https://github.com/apache/superset/discussions/40273)
landed on the development branch for the 7.0 release. Its
[dependency declaration](https://github.com/apache/superset/blob/master/pyproject.toml)
uses SQLAlchemy 2. This removes the specific SQLAlchemy 1.4 versus 2 dependency
conflict above; it does not establish complete adapter compatibility.

For a deployment using this server, the candidate connection path is:

```text
Superset 7 -> sqlalchemy-adbc[flightsql] -> Redis Flight SQL server -> Redis
```

The third-party, beta [sqlalchemy-adbc adapter](https://pypi.org/project/sqlalchemy-adbc/)
uses a local URI such as `adbc+flightsql://localhost:8815?tls=false`. Treat this as a candidate
configuration until validated with the exact Superset 7 and adapter versions.
Before recommending it for deployment, verify dependency resolution, connection
and schema discovery, SQL Lab queries, dataset creation, and browser-rendered
charts including generated filters, aggregates and time grains. Record versions,
results and failures separately from the existing Superset 6.1 installation
failure. The live recipe run below records a limited SQL Lab API pass; it does not
certify the entire application.

## Read and write examples

After installing and configuring a compatible connection as above, enable
SQL Lab access. Use the [shared sample table](integrations/README.md#optional-sample-sql-table)
on the same backing database and run:

```sql
SELECT id, label, amount FROM flight_tool_sample ORDER BY id;
```

Expect three rows, including the NULL label on id 2. Where your Superset
connection and permissions allow DML, use an unused id `99` in a disposable
sample and execute each statement separately in SQL Lab:

```sql
INSERT INTO flight_tool_sample (id, label, amount) VALUES (99, 'guide', 9.5);
SELECT * FROM flight_tool_sample WHERE id = 99;
DELETE FROM flight_tool_sample WHERE id = 99;
SELECT * FROM flight_tool_sample WHERE id = 99;
```

Expect one row with amount `9.5`, then no rows. These statements passed through Superset 7.0.0 SQL Lab APIs in the live
recipe run linked below. Superset 6.1 uses the
direct Redis ADBC path above; the Flight adapter remains blocked for that
version. For a blocked/unavailable connection use an
[external ADBC ingestion job](integrations/python.md#write-and-verify-a-sql-table),
then refresh Superset after configuring a compatible read connection. Writes
are autocommit; do not rely on SQL Lab rollback to undo them.

## Live SQL Lab recipe validation — November 3, 2026

The [guide recipe run](validation/2026-11-03-guide-recipes/README.md) executed
these reads, INSERT, DELETE and fresh read-back through the real SQL Lab API
in the retained Superset 7.0.0 development runtime, using its Flask test client
and an isolated metadata database. It used sqlalchemy-adbc 0.3.0 and SQLAlchemy
2.0.55, not Superset 6.1. Browser charts, metadata discovery and deployment
were not tested by this run. The installed development runtime is not evidence
of a verified published Superset 7 release installation.

For this local Flight connection, set the URI's `tls=false` and the connection's
Advanced / Extra JSON to:

```json
{"engine_params": {"connect_args": {"autocommit": true}}}
```

Expose the database in SQL Lab and allow DML for the disposable write check.
Do not pass SQLAlchemy's `isolation_level="AUTOCOMMIT"` setter to adapter 0.3.0;
use the DBAPI connection argument above. Superset 6.1's incompatible Flight
adapter dependency requirements remain unchanged.
