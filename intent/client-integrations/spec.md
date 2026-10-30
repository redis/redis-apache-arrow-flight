# Client integration acceptance

Use unique fixtures and clean up after each runnable test. Assert ordered data,
NULLs and empty results for pandas, Polars and R. Validate Polars batch iteration,
DuckDB local SQL over both Flight SQL results and Flight RPC datasets, and the
existing JDBC metadata/session/concurrent-read contract.

Keep pandas parameter API limitations visible. A JDBC pass is driver evidence,
not a DBeaver application pass. An incompatible Superset adapter installation
is a blocker, not evidence of SQL Lab or dashboard compatibility. Record exact
versions, runtime, source revision, commands and exclusions in the client matrix.

No production backend, authentication changes or dependency overrides are needed.
