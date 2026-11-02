# Apache Superset: choosing a Redis connection

Guidance checked on 2026-11-01. Choose the connection path for your Superset
version:

| Superset version | Recommended path | Evidence and limits |
| --- | --- | --- |
| 6.1 | Connect directly with the Redis ADBC driver's Superset integration | The driver supplies a SQLAlchemy 1.4-compatible dialect. The tested Flight SQL adapter has incompatible dependency requirements. |
| 7 | Flight SQL is a candidate when a remote Flight endpoint is needed | The SQLAlchemy 2 migration removes the known dependency blocker. End-to-end Superset 7 / Redis Flight SQL compatibility has not been validated here. |

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
See the [upstream evidence summary](data-engineering-adbc-support.md#testing-status).

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
uses a URI such as `adbc+flightsql://localhost:8815`. Treat this as a candidate
configuration until validated with the exact Superset 7 and adapter versions.
Before recommending it for deployment, verify dependency resolution, connection
and schema discovery, SQL Lab queries, dataset creation, and browser-rendered
charts including generated filters, aggregates and time grains. Record versions,
results and failures separately from the existing Superset 6.1 installation
failure. No Superset 7 application pass is claimed by this document.
