# Live integration guide recipes — November 3, 2026

The new guide recipes were executed against the retained Enterprise-backed
Flight endpoint at `grpc://localhost:18816`. **All 27 inventoried routes passed
their stated recipe scope**, including remote reads, output writes and read-back.
Tableau's write is external ADBC ingestion followed by a native Tableau preview;
it is not a native Tableau writer. Go, Java, DuckDB, driver-level JDBC SQL and
Superset SQL Lab recipes also passed. See the [per-guide summary](summary.md)
and [machine-readable results](results.json).

Three defects in the new documentation were corrected and retested:

| Recipe | Initial failure | Correction and successful retest |
| --- | --- | --- |
| Jupyter | pandas produced Arrow `large_string`, while SQL returned `string`; exact table equality failed despite equal values | Pass `schema=source.schema` to `Table.from_pandas`; execute the complete cell in a separate real kernel |
| Airflow | The example DAG was not discoverable at module scope; `dag.test()` could not serialize it | Bind `dag_instance` at module scope and keep the example in an isolated DAG directory; local TaskFlow run finished successfully |
| DuckDB | DuckDB 2.0 removed `DuckDBPyConnection.fetch_arrow_table()` | Use `local.sql(...).to_arrow_table()`; documented 1.12 ADBC client environment passed read/write and cleanup |

[Initial failure evidence](initial-failures/) is retained. Terminal color escapes
and trailing whitespace were normalized in the saved text transcripts. An early extraction
harness also used package names as Python filenames and put harness files in
Airflow's discovery directory. Those harness errors were fixed before the final
runs; they are not client failures. The documented filenames use `_example.py`.

## Environment and provenance

- Existing local Enterprise Flight container:
  `redis-flight-enterprise-test-flight-1`; no new Redis/Flight deployment was built.
- Redis Software container `redislabs/redis:8.2.0-78.18`, database
  `flight-integration`, three active primary shards, active 4x QPF.
- Verified the Redis TLS hostname against its proxy certificate retrieved through
  the local Docker control channel and used as a pinned partial-chain trust
  anchor. Verified ACL username/password authentication and rejection of
  unauthenticated TLS, plaintext and an untrusted certificate. No global trust
  configuration changed. Flight clients used local plaintext gRPC.
- The original host bind mount and executable are no longer readable. The
  [environment receipt](environment.json) records the running container identity,
  but its binary checksum and full source fingerprint could not be recovered.
  **This is live client recipe evidence, not current-source or release
  qualification.** The source-fingerprint gate was not bypassed to build anything.
- Guide baseline: `d5158fcd93c466412336a8528d1ddafc85a30921`, plus the three
  corrections described above. [Guide hashes](guide-sha256.json) identify the final
  reviewed documents; the archive retains the executed code.
- Reused seven previously prepared dependency profiles from
  `/tmp/flight-repeatable-5068/`. Native model runtimes ran in separate processes.
  Created a fresh `clients` environment from
  `examples/python/requirements-clients.txt` for pandas, Polars, PyArrow, DuckDB
  and external application writes. Those final client runs used ADBC 1.12.0;
  the new profile-based examples used 1.13.0. Every environment has a
  `*-versions.json` snapshot. This was not a fresh installation test of every
  package or of JupyterLab's browser interface.
- JDK 21 with Flight SQL JDBC 19.0.0; R 4.6.2 with adbcdrivermanager 0.25.0
  and nanoarrow 0.9.0; RStudio 2026.10.1+239; Quarto bundled with RStudio.
- Fresh disposable Metabase 0.64.2.9, checksummed community plugin 0.8.0,
  bundled JDBC 19.0.0. The [API transcript](metabase.log) records the guide's
  exact INSERT/UPDATE/DELETE statements and native read-back after each.
- Retained Superset development runtime reporting package version 7.0.0,
  sqlalchemy-adbc 0.3.0 and SQLAlchemy 2.0.55. Used a separate copy of its test
  metadata database and the actual SQL Lab API via Flask's in-process test
  client. This does not verify a published Superset 7 release installation,
  Superset 6.1, browser rendering or metadata discovery.

## Execution and evidence

Python snippets were extracted from each guide and executed in guide order.
Only the endpoint and input fixture name were substituted: the standard sample
became the test-owned `guide_live_20261103_d2f23049d00d`, seeded with the guide's
three rows and NULL label. No arbitrary user table was replaced.

- Library/dataframe/model/plotting recipes read SQL, produce tool-specific
  output, ingest a unique output table through Flight SQL, verify exact ordered
  Arrow read-back, then drop the output. Plot/image files remain local artifacts.
- The Jupyter cell ran in a separate real kernel and its executed notebook was
  saved. Streamlit ran with AppTest, including the actual write-button click
  and assertions; browser rendering was not tested.
- Spark used its JDBC reader, then the documented small driver-side collection
  and ADBC write bridge. No distributed JDBC writer support is claimed.
- Airflow used `dag.test()`, Kubeflow used the local SubprocessRunner and
  Metaflow used its local datastore. These are real local framework executions,
  not deployed schedulers or clusters.
- Quarto rendered HTML containing the successfully executed read/write cell.
- The combined Go program and both Java guide programs executed against the
  endpoint. Generic JDBC guide DML also passed via the real JDBC driver;
  arbitrary JDBC applications remain unqualified by that check.
- R ran both standalone and in RStudio's actual R Console. The
  [RStudio transcript](rstudio.log) contains the harness PASS and session data.
- Tableau created a fresh JDBC data source, selected the test table and clicked
  Update Now after the external write. Its UI showed four rows, including
  `99 / guide / 9.50000`. The [UI observation log](tableau-ui.log) records the
  contemporaneous screenshot/AX observation; no screenshot file or workbook
  was published. The external DELETE/read-back then passed.

[Artifacts](artifacts.tar.gz) include executed source fragments, the notebook,
Quarto HTML and plot outputs. Extract with `tar -xzf artifacts.tar.gz`. Runtime
paths and the saved fixture name describe this run; adapt them to your own
installed runtimes and a new owned fixture when reproducing. Follow the
[qualified environment setup](../../redis-enterprise-testing.md), the individual
[guides](../../integrations/README.md) and the recorded package snapshots.
Jupyter, Streamlit, Quarto and native applications require their documented
execution contexts; running every block as an ordinary Python script is not
equivalent to those checks.

## Remaining application limits and cleanup

Power BI and a compatible connector were unavailable. Its external ADBC
write/read-back/cleanup passed, but no Power BI read or refresh is claimed.
Redis Insight was not installed: its exact disposable Redis command sequence
passed directly over verified TLS, but its UI was not tested. DBeaver had no new
SQL snippet in this change; its UI retest could not be completed because its
navigator lacked accessible controls and coordinate targeting failed. Existing
historical DBeaver results remain unchanged. Superset 6.1's Flight adapter
installation blocker remains unchanged.

No native Tableau or Power BI writes, R DBI/adbi, browser dashboards, full ML
training service, distributed execution, client-facing TLS/authentication,
production retries, throughput or release qualification are claimed. No expected
failure is counted as a recipe pass; the historical Datasets persistence defect
was not rerun by these new read/write examples.

The [cleanup transcript](cleanup.log) confirms the test-owned input table and
row 99 were removed and no `guide_` output tables remained. The disposable
Metabase container created for this run was removed. Existing Redis/Flight
containers and preexisting tables were left running and untouched. Tableau's
unsaved preview may retain cached rows after fixture cleanup; nothing was
published. Test artifacts and isolated framework metadata remain in `/tmp`.
