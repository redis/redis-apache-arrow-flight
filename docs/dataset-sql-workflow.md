# Importing a Flight dataset into SQL

A Flight upload stores an Arrow dataset; it does not register a Redis ADBC SQL
table. SQL tables use row HASHes/Search indexes and different type rules. Keep
that distinction explicit so a client never silently changes storage or types.

`examples/python/dataset_to_sql.py` demonstrates the supported standard-client
workflow: upload a dataset, discover/read its endpoints, select scalar columns,
use ADBC bulk ingestion to create a SQL table, query it, then drop both fixtures.
It uses unique names, nullable values and cleanup, and checks that direct struct
SQL ingestion fails with an unsupported-type error. The pinned driver supports
one-dimensional lists as SQL arrays; lists alone are not an unsupported fixture. The example was run locally
with PyArrow 26.0.0 and ADBC Flight SQL/manager 1.12.0 and is included in CI.

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
values, signed ranges, decimal precision and timezone semantics; unsupported nested types such as structs
need an explicit supported transformation or must remain Flight datasets.
Dictionary columns supported by the backend are decoded to their value types;
SQL storage does not preserve a dataset's dictionary encoding.

The Flight dataset interface continues to work without Redis Search. Importing
into SQL requires a Search-enabled backend and SQL write permissions. Each
interface uses the authenticated Redis identity; a client performing a bridge
needs read access to the dataset and write access to the target table.
