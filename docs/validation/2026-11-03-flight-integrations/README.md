# Flight SQL integration sweep — November 3, 2026

**All 21 previously unexercised candidates passed a scoped tool workflow.**
All 27 Flight SQL routes in the deck now have fresh execution evidence:
24 automated tool routes plus RStudio and Tableau native UI checks and a
Metabase application API check. This is a scoped local integration result,
not full application certification.

The final complete runner exited **0**: **28 automated test cases passed,
2 strict expected failures, 0 unexpected failures/errors**, and all three
application receipts passed. Expected failures are two reproductions of the
Datasets 5.1.0 default empty save/reload defect; its one-shard workaround passed.
The [tool-by-tool summary](reports/summary.md) lists all 27 entries and identifies
the six previously exercised routes. The [JSON report](reports/summary.json)
contains test names, profile exit codes, elapsed times, skipped/expected-failure
limitations and application scopes.

## Environment

- Server source `c019afcdcc11fed4288833bacbddd70dc2d50c1c`, community build,
  Go 1.27.2, pinned Redis ADBC revision `2f053d3affad`.
- New isolated standalone `redis:8.6.2`, local Flight SQL on port 18825.
  This run used Redis Open Source with Search, not Software + verified 4× QPF.
- Fresh Python environments installed from the committed profile requirements:
  Python 3.12.15, except Airflow on 3.14.8; ADBC Flight SQL/manager 1.13.0,
  PyArrow 26.0.0. Every profile saves its complete resolved package versions.
- Spark 4.3.0 used the real JDBC datasource and Flight SQL JDBC 19.0.0,
  with full OpenJDK 21.0.13. It passed typed/NULL/Unicode rows, filters,
  aggregation, two predicate partitions and empty results.
- R 4.6.2 / adbcdrivermanager 0.25.0 / nanoarrow 0.9.0;
  Quarto 1.10.20; RStudio 2026.10.1+239.
- Tableau Desktop Free Edition 2026.3 / JDBC 19.0.0.
- Disposable Metabase 0.64.2.9 / plugin 0.8.0 / JDBC 19.0.0.

[environment.json](environment.json) records the server binary checksum,
Go module build metadata, harness/requirements checksums and external runtimes.
The Git SHA identifies the built server; the accompanying harness is new work
on that source, identified by its file checksums.

Container image identities:

```text
redis:8.6.2 sha256:832d7785830f3f4b559300e6191fc914b15642c1935252338825cf4332200148
metabase/metabase:v0.64.2.9 sha256:a314433f07c192b18eb41a0c44cdadbe688525d2b3d1597c085e11f6c684c6a9
```

## Repeat the run

Follow the [integration testing guide](../../integrations/testing.md) for
endpoint setup, dependency profiles, prerequisites and application acceptance.
The exact local final invocation used the previously installed external
runtimes and a new environment root:

```sh
JAVA_HOME=/opt/homebrew/opt/openjdk@21/libexec/openjdk.jdk/Contents/Home \
FLIGHT_JDBC_JAR=/tmp/flight-endpoint-20261029/flight-sql-jdbc-driver-19.0.0.jar \
R_LIBS_USER=/tmp/redis-adbc-r-library \
QUARTO=/Applications/RStudio.app/Contents/Resources/app/quarto/bin/quarto \
python3 scripts/test-integrations.py --setup \
  --uv /tmp/redis-adbc-workflows-venv/bin/uv \
  --uri grpc://127.0.0.1:18825 --env-root /tmp/flight-repeatable-5068 \
  --results /tmp/flight-sweep-complete \
  --application-results docs/validation/2026-11-03-flight-integrations/application-results.json
```

These absolute runtime paths describe this execution; use your own installed
runtimes for a new run and generate fresh application receipts. `make
integrations-test` supplies a timestamped result directory and runs the default
profiles. Use `INTEGRATION_ARGS` to select profiles, environments or receipts.
A full run correctly fails its exit gate while application evidence is absent;
`--automated-only` gates only selected automated profiles.

## Evidence

- [Reports](reports/summary.md): profile logs, JUnit XML, resolved versions,
  application receipts and copied application logs in the same directory.
- [RStudio transcript](rstudio.log): the sourced R driver-manager harness ran
  in the real RStudio R console, including DDL, writes, reads, NULLs, empty
  result, SUM and cleanup. Console success was observed and the log written
  from that IDE session. This does not establish DBI/adbi compatibility.
- [Tableau UI observation](tableau.log): new JDBC connection, catalog/schema/
  table/column discovery and generated data preview. The UI displayed three
  rows with `10.50000`, `20.00000` and NULL; checked visually. This transcript
  is not a native query log or a saved screenshot.
- [Metabase API output](metabase.log): connection, native reads/NULL/empty/
  aggregates, metadata sync, generated filters/grouping, and INSERT/UPDATE/
  DELETE Actions with fresh read-back.
- [Output artifacts](artifacts.tar.gz): executed notebook, self-contained
  Plotly HTML/JSON, Matplotlib PNG, model save files, Quarto HTML and local
  workflow state/output from the final run. Extract with
  `tar -xzf artifacts.tar.gz`. Model child XML files are also combined into
  `models.xml`; count only the combined profile report to avoid duplication.

The two meaningful fresh-environment failures were retained under
[reproducibility-issues](reproducibility-issues/combined-model-runtime-abort.log):
PyTorch import after XGBoost training aborted the combined process, and Quarto
needed PyYAML explicitly installed. The runner now isolates every model tool
in its own interpreter and the document requirements pin PyYAML. The final
complete run uses these fixes. No native-runtime bypass flag was used.

## Limits and cleanup

Small, local custom pipelines are the stated integration routes for libraries
without a packaged database connector. No distributed/cloud deployment,
throughput, TLS, authentication, production retry guarantees or universal SQL
compatibility was tested. TensorFlow/Keras are CPU input/save workflows;
Transformers uses a tiny offline randomly initialized model. JupyterLab and
Streamlit browser rendering, Tableau worksheets/extracts/dashboards, Metabase
browser dashboards, and R DBI/adbi through Flight SQL remain untested.

sqlalchemy-adbc 0.3.0's Flight SQL dialect passed with DBAPI autocommit;
its SQLAlchemy `isolation_level="AUTOCOMMIT"` setter is unimplemented.
Airflow passed real local TaskFlow execution with task-local Flight connections,
not a generic provider hook or deployed scheduler. Kubeflow used its local
SubprocessRunner; Metaflow used its local datastore.

The report aggregation unit tests (2) passed. Repository `go test ./...`
passed with local socket access; REDIS_URI was unset, so this separate Go run
does not claim live backend integration coverage. The new workflow passed
`actionlint`; hosted GitHub execution remains unrun. Python syntax compilation
and `git diff --check` passed.

The disposable BI fixture was dropped, the test Flight process stopped, and
only the two containers created for this sweep were removed. Other services
were left running. Tableau retains an unsaved read preview of the stopped
endpoint; no workbook was published.

## PR integration with newer main

The PR was rebased onto `68106776b5a265af7802f5448337f8930e57efd3` after this
recorded run. The new main requires a qualified Enterprise backend with verified
TLS/authentication. Future-run instructions and the self-hosted dispatch
workflow now use that environment; they do not repeat the historical OSS setup.
These 27 scoped passes remain evidence only for the original source/backend.
The live profiles were not rerun against the rebased Enterprise composition.

Both Datasets failures are tracked by [Flight issue #57](https://github.com/redis/redis-apache-arrow-flight/issues/57),
which links the existing [upstream defect #8912](https://github.com/huggingface/datasets/issues/8912).
