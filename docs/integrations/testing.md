# Repeatable Flight SQL integration checks

The runner inventories the **same 27 Flight SQL routes** used by the linked
[top-50 deck](https://docs.google.com/presentation/d/1CdXUMzl7VJNHgVke0hR0rl8n3TmqzcV19gNrnT8l-d8/edit#slide=id.inventory50_b).
It labels the six previously exercised routes and the 21 former candidates.
It covers 24 routes with automated tests and three with application acceptance
procedures: RStudio, Tableau and Metabase. Results are scoped by operation;
a passing library workflow does not certify an entire application.

## Start a disposable endpoint

Requires Go, Docker and uv. Do not use a backend containing application data.

```sh
make build
REDIS_PORT=16489 docker compose up --detach --wait redis
./bin/redis-flight-server --redis-uri redis://127.0.0.1:16489/0 \
  --listen 127.0.0.1:18825 --metrics-listen 127.0.0.1:19825
```

Use a port reachable by each runtime. Docker Desktop BI clients use
`host.docker.internal`; a remote worker cannot use the controller's localhost.
Community has no backend health probe; the fixture performs a real SQL round
trip. Tests create unique table names and clean them up. The BI helper uses
`bi_jdbc_validation` and requires a disposable backend.

## Automated profiles

In another terminal, prepare a full JDK 21+, R with `adbcdrivermanager` and
`nanoarrow`, and the Quarto CLI. Supply the JDBC JAR using the existing checksum
verified Make target:

```sh
make bin/flight-sql-jdbc-driver-19.0.0.jar
export JAVA_HOME=/path/to/full/jdk
export FLIGHT_JDBC_JAR="$PWD/bin/flight-sql-jdbc-driver-19.0.0.jar"
export QUARTO=/path/to/quarto
# If R packages live outside .libPaths():
export R_LIBS_USER=/path/to/r-library
python3 scripts/test-integrations.py --setup \
  --uri grpc://127.0.0.1:18825 --env-root /tmp/flight-integration-envs \
  --results /tmp/flight-results-001 --automated-only
```

Each model tool runs in its own interpreter. On the fresh macOS arm64 runtime,
loading PyTorch after XGBoost training in one process repeatedly aborted;
separate interpreters passed without runtime bypass flags.

`--setup` creates isolated uv environments and installs the pinned top-level
requirements in `requirements/integrations/`. Python 3.12 is used, except for
Airflow on 3.14. Full resolved package versions and interpreter versions are
saved for every run. OS-specific dependencies can differ. Quarto/JDK/R are
external prerequisites; the R run logs its session and package versions.
Omit `--setup` to reuse prepared environments. `--uv /path/to/uv` selects uv.
The November 3 run used JDK 21.0.13, Quarto 1.10.20, R 4.6.2,
adbcdrivermanager 0.25.0 and nanoarrow 0.9.0.

Select a subset using `--profiles core workflows models orchestrators spark
airflow documents`. The default runs all seven profiles. The runner continues
after a failed profile and saves its error. Missing runtimes, skipped tools,
missing reports and uncollected tests never count as passes. A nonempty output
directory is rejected to prevent old reports from contaminating a new run.

| Profile | Tools / operations |
| --- | --- |
| core | NumPy arrays, scikit-learn fit/predict, real Dagster resource/asset, SQLAlchemy dialect reads, pandas, Polars, PyArrow batches and empty schema |
| workflows | Jupyter separate kernel and saved notebook, Datasets save/reload/filter, Streamlit AppTest input rerun, SciPy sparse solve |
| models | XGBoost training/save/reload, PyTorch Dataset/DataLoader/forward pass, TensorFlow batches and NULL mask, Keras save/reload, offline tiny Transformers model, Plotly HTML/JSON, Matplotlib PNG |
| orchestrators | Kubeflow local SubprocessRunner with checked output, Metaflow local steps with persisted data |
| spark | Real Spark JDBC datasource: schema/rows, predicate, aggregate, two predicate partitions, empty result |
| airflow | Real local TaskFlow DAG: bound Flight SQL query, XCom handoff, downstream check and task success states |
| documents | Quarto HTML with executed Python query; R driver-manager DDL/writes/read/NULLs/empty/SUM/cleanup |

SQLAlchemy uses `adbc+flightsql://host:port?tls=false` with
`connect_args={"autocommit": True}`. The tested sqlalchemy-adbc 0.3.0 dialect
does not implement the SQLAlchemy isolation-level setter; passing
`isolation_level="AUTOCOMMIT"` raises `NotImplementedError`.
Airflow uses Python TaskFlow code, not a claim about the generic AdbcHook.
R is driver-manager coverage; DBI/adbi over Flight SQL remains untested.

Datasets 5.1.0's default empty save/reload has two exact, strict expected
failures. Its explicit `num_shards=1` workaround passes. Expected failures and
skips are retained in JUnit and tool limitations, separate from passing cases.

## Application acceptance

These checks require their real applications. A protocol-only JDBC test is
insufficient. Save fresh logs and the exact versions for each execution.

- **RStudio:** in its R console, source `examples/r/flight_sql.R`, then call
  `main("grpc://localhost:18825", "/path/to/libadbc_driver_flightsql.so")`.
  The library must match the R runtime OS/architecture; it can come from that
  platform's `adbc-driver-flightsql` wheel. Save `capture.output(main(...))` to
  a log, verify the PASS message in the console, and record the IDE version.
  This exercises the IDE's R session, not DBI, its Connections pane or Python.
- **Tableau:** follow the [native read acceptance procedure](../validation/2026-11-01-tableau/README.md#read-test-passed).
  Seed with `BiFixture.java create`, establish a new JDBC connection, select
  `redis/public/bi_jdbc_validation`, click Update Now, and verify all three
  values including NULL. Save a contemporaneous observation log. Do not count
  the JDBC fixture helper itself as Tableau evidence. Drop the fixture afterward.
- **Metabase:** use a fresh disposable instance and the checksummed plugin in
  the [existing reproduction instructions](../validation/2026-11-01-bi-jdbc/README.md#reproduction).
  Seed `BiFixture.java create`, then run
  `python3 examples/jdbc/metabase_smoke.py --metabase http://127.0.0.1:13030 --flight-port 18825`.
  Save stdout/stderr and exit status. This covers API reads, sync, generated SQL
  and write Actions. Drop the fixture and remove the disposable container.

The [November 3 application receipt](../validation/2026-11-03-flight-integrations/application-results.json)
is an example schema. Create a **new** receipt with the current endpoint,
UTC `observed_at`, tool status, scope and relative evidence paths. Supply it
with `--application-results /path/to/application-results.json`. The runner
checks the endpoint, allowed tool/status names, evidence-file presence and
that the receipt is less than 24 hours old. Receipts are operator attestations;
the runner cannot independently verify a screenshot or a UI observation.
Never reuse the saved November 3 receipt as evidence for a new endpoint run.

A full run exits nonzero unless all 27 tools pass and every selected profile
exits successfully. `--automated-only` limits the exit gate to the selected
automated profiles while leaving manual/not-run entries visible in the report.
It does not promote missing application evidence to passes.

## Results and automation

Every output directory includes `summary.json`, `summary.md`, per-profile logs,
JUnit XML, dependency-version snapshots and retained pytest artifacts (notebooks,
plots, model files, workflow outputs). Supplied application receipts and their
logs are copied into the report directory. Results include the source Git SHA
and Flight URI. Keep the directory with your build/deployment metadata.

`make integrations-test` creates environments and a timestamped `/tmp` result directory; use `INTEGRATION_ARGS` for its flags.
The **Flight SQL integrations** workflow can run each automated profile manually
and uploads results even on failure. Hosted workflow configuration is not a
recorded hosted pass. RStudio and Tableau continue to require native UI checks;
Metabase's disposable API procedure is separately reproducible.

The [November 3 saved validation](../validation/2026-11-03-flight-integrations/README.md)
records the actual tested source, versions, outcomes and limits.
