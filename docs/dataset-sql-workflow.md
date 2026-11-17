# Importing a Flight dataset into SQL

A Flight upload stores an Arrow dataset; it does not register a Redis ADBC SQL
table. SQL tables use row HASHes/Search indexes and different type rules. Keep
that distinction explicit so a client never silently changes storage or types.

`examples/python/dataset_to_sql.py` demonstrates the supported standard-client
workflow: upload a dataset, discover/read its endpoints, and explicitly use ADBC
bulk ingestion to create SQL tables. The example checks a direct import of list
and typed-struct columns, then a scalar projection, queries both, and cleans up
its unique fixtures. It verifies nullable values, empty lists and nested values
without silently flattening the struct. Redis ADBC 0.0.28 supports these nested
types; projecting scalar columns is an optional client choice.

The 2026-11-17 local run used PyArrow 26.0.1 and ADBC Flight SQL/manager 1.13.0
against the public Flight build and a disposable TLS/ACL Redis Enterprise
backend. See the [dependency refresh evidence](../intent/dependency-refresh-2026-11/plan.md).

No server-side import action is added. Standard Flight plus ADBC already covers
the defined workflow, while an automatic action would need new naming, ACL,
overwrite, transaction and conversion semantics. Use create mode to avoid
replacing an existing table accidentally; append/replace are explicit choices.
Ingestion is autocommit and is not promised to roll back a partially failed load.
For a large import, use a unique staging table, validate it, and decide how to
publish it before exposing it to readers.

The small example materializes data on the client. Large imports should project
columns using dataset tickets and feed record batches incrementally to ingestion.
When FlightInfo has multiple endpoints, consume every endpoint. Preserve nullable
values, signed ranges, decimal precision and timezone semantics. Check the
pinned driver's supported Arrow types before importing; unsupported types need
an explicit supported transformation or must remain Flight datasets.
Dictionary columns supported by the backend are decoded to their value types;
SQL storage does not preserve a dataset's dictionary encoding.

The Flight dataset interface continues to work without Redis Search. Importing
into SQL requires a Search-enabled backend and SQL write permissions. Each
interface uses the authenticated Redis identity; a client performing a bridge
needs read access to the dataset and write access to the target table.
