# Live guide recipe results

Status is scoped to the operation described. External ingestion is not native application write support.

| Guide | Result | Executed scope |
| --- | --- | --- |
| numpy | passed | SQL read, tool conversion, ADBC output ingestion, exact ordered read-back, output cleanup |
| scikit-learn | passed | SQL read, tool conversion, ADBC output ingestion, exact ordered read-back, output cleanup |
| sqlalchemy | passed | SQL read, tool conversion, ADBC output ingestion, exact ordered read-back, output cleanup |
| dagster | passed | SQL read, tool conversion, ADBC output ingestion, exact ordered read-back, output cleanup |
| pandas | passed | documented client versions read/write |
| polars | passed | documented client versions read/write |
| python | passed | documented client versions read/write |
| duckdb | passed | documented client versions read/write |
| scipy | passed | SQL read, tool conversion, ADBC output ingestion, exact ordered read-back, output cleanup |
| hugging-face-datasets | passed | SQL read, tool conversion, ADBC output ingestion, exact ordered read-back, output cleanup |
| xgboost | passed | SQL read, tool conversion, ADBC output ingestion, exact ordered read-back, output cleanup |
| pytorch | passed | SQL read, tool conversion, ADBC output ingestion, exact ordered read-back, output cleanup |
| tensorflow | passed | SQL read, tool conversion, ADBC output ingestion, exact ordered read-back, output cleanup |
| keras | passed | SQL read, tool conversion, ADBC output ingestion, exact ordered read-back, output cleanup |
| transformers | passed | SQL read, tool conversion, ADBC output ingestion, exact ordered read-back, output cleanup |
| plotly | passed | SQL read, tool conversion, ADBC output ingestion, exact ordered read-back, output cleanup |
| matplotlib | passed | SQL read, tool conversion, ADBC output ingestion, exact ordered read-back, output cleanup |
| kubeflow | passed | SQL read, tool conversion, ADBC output ingestion, exact ordered read-back, output cleanup |
| metaflow | passed | SQL read, tool conversion, ADBC output ingestion, exact ordered read-back, output cleanup |
| airflow | passed | local TaskFlow read/write, verification and cleanup |
| spark | passed | SQL read, tool conversion, ADBC output ingestion, exact ordered read-back, output cleanup |
| tableau | passed | native JDBC connection/catalog/schema/table discovery and preview; external ADBC INSERT and verification, native UI displayed row 99 / guide / 9.5; no native Tableau writes |
| power-bi | partial | external ADBC ingestion/read-back passed; Power BI application/connector unavailable, so refresh not run |
| jupyter | passed | separate notebook kernel |
| streamlit | passed | AppTest button execution; no browser rendering |
| quarto | passed | Python cell read/write/read-back and rendered HTML |
| r | passed | standalone R ADBC DDL/INSERT/read/NULL/empty/aggregate/cleanup |
| rstudio | passed | native RStudio R Console, sourced guide harness; reads and writes |
| go | passed | Go Flight SQL ExecuteUpdate and typed read-back, plus guide SELECT |
| java | passed | documented Java JDBC read and write programs; read-back asserted |
| jdbc-applications | passed | guide aggregate and INSERT/UPDATE/DELETE SQL through JDBC; arbitrary application UIs untested |
| metabase | passed | fresh application custom Actions INSERT/UPDATE/DELETE plus native read-back |
| superset | passed | Superset 7.0.0 development runtime SQL Lab via in-process Flask API; reads/INSERT/DELETE/read-back |
| redis-insight | partial | documented SET NX / GET / DEL / GET commands passed directly over verified Redis TLS; Redis Insight UI not installed |
| dbeaver | not_run | no new SQL recipe was added to this guide, only installation link; UI retest not completed because navigator content was unavailable to accessibility and coordinate targeting failed; historical results unchanged |
