# Flight SQL reference

[Back to the README](../README.md). Detailed SQL capabilities, statement
execution, streaming and resource limits. Availability depends on the
[build profile](build-profiles.md).


### Flight SQL support

| Feature | Support |
|-|-|
| Queries (`CommandStatementQuery`), updates (`CommandStatementUpdate`) | Yes. A statement without a row count (DDL, `SET`) reports 0 rows |
| Prepared statements, with parameters | Yes. The handle carries the SQL and the bound values, so the server keeps no state. Clients must use the handle a bind returns (`DoPutPreparedStatementResult`, Flight SQL's stateless prepared statements). arrow-go, the ADBC Flight SQL driver and the JDBC driver do, and are tested; a client that ignores it loses the bound values. The parameter schema gives each parameter the type it's used as (a compared column's, an `INSERT` target's, a `CAST`'s, …), as the Redis ADBC driver infers it, or NULL. Parameters for a query are limited to 1 MiB |
| Bulk ingest (`CommandStatementIngest`) | Yes: create, append, replace, create-or-append, and pass-through options such as `adbc.redis.ingest.index_columns`. Temporary tables (`temporary`, or the `pg_temp` schema) need a [session](../docs/server-guide.md#sessions). `adbc.ingest.*` options are refused: set the target and mode with the command's fields |
| `GetCatalogs`, `GetDbSchemas`, `GetTables` (with schemas), `GetTableTypes`, `GetSqlInfo`, `GetXdbcTypeInfo` | Yes. The catalog is `redis`; schemas are key namespaces (`public` by default) |
| Primary and foreign keys | Empty results: the driver has no primary/foreign-key constraints |
| Sessions: `SetSessionOptions`, `GetSessionOptions`, `CloseSession` | Yes, for clients that keep cookies: see [Sessions](../docs/server-guide.md#sessions) |
| Explicit query cancellation | `CancelFlightInfo`; the deprecated Flight SQL `CancelQuery` is translated to it. Applies to read-only query tickets with cancellation metadata; see [cancellation details](../docs/data-access.md#query-cancellation) |
| Transactions, savepoints, Substrait | No |

How statements run:

- **Read-only statements** (`SELECT`, `SHOW`) run when their ticket is read,
  so any server replica can serve the ticket.
- **Every other statement** (`INSERT`, `UPDATE`, DDL, `SET`, …) runs when its
  FlightInfo is requested, so it runs exactly once even if the client never
  reads the stream (DB-API cursors often don't) or reads it twice. The result,
  such as the rows of `INSERT … RETURNING`, travels in the ticket. If it is
  larger than 1 MiB, it is kept in Redis for 10 minutes instead.
- **Outside a [session](../docs/server-guide.md#sessions), each request uses a pooled ADBC
  connection.** Session state then doesn't carry over between requests.
  A statement that changes it (`SET`, `BEGIN`, `CREATE TEMP TABLE`, or a
  table or view created in `pg_temp`) works within its own script, for
  example `SET TIME ZONE 'Asia/Tokyo'; SELECT …`, and its connection is
  closed afterwards rather than reused.
- **Plain scans stream from Redis.** For a query that reads one table
  through its index, row by row (`SELECT * FROM t`, or a filter and
  `ORDER BY` the index can do), the driver reads the rows a cursor page at
  a time while they are sent. The server then holds about one batch of
  them, whatever the table's size. Other queries (aggregates, joins,
  `DISTINCT`, window functions, sorts the driver does) are built whole in
  memory first. The driver's
  [Streamed results](https://github.com/redis/redis-adbc-driver#streamed-results)
  has the exact rules.
  - A streaming result keeps its pooled connection until it has been sent,
    or the client goes away. Each Redis user can have `--max-streams` (8)
    results streaming at once, but always fewer than `--pool-max-open`, so
    the user's other requests have a connection (with a pool of 1, nothing
    streams). With `--auth=none`, every client is the same user. Queries
    beyond that are read whole (see below), as are queries in a
    [session](../docs/server-guide.md#sessions) that has made temporary tables or views.
  - A streaming result also keeps a cursor open on its table's index.
    RediSearch allows an index `search-index-cursor-limit` open cursors
    (128 by default), and the driver reads every query through one, so at
    the limit every query on the table fails with `RESOURCE_EXHAUSTED`
    until some are closed; clients can retry. The limit is shared by every
    client of the index: other servers (replicas), other tools, and every
    Redis user. So each server streams at most `--max-table-streams` (32)
    results from one table at once, across all users, and reads the rest
    whole.
  - On standalone Redis, the server also reads how many cursors the
    index has open (`FT.INFO`, at most once a second per table), counting
    every client, and stops streaming from the table once three quarters
    of `search-index-cursor-limit` are open. The last quarter is left for
    queries read whole and other clients' short reads. A raised limit is
    picked up by itself.
  - On a cluster, `FT.INFO` doesn't report the count, so with several
    replicas keep their total `--max-table-streams` under the limit, or
    raise `search-index-cursor-limit` in Redis (and then
    `--max-table-streams`).
  - Set `--stream-overflow=reject` (`FLIGHT_STREAM_OVERFLOW=reject`) to
    refuse an eligible scan with `RESOURCE_EXHAUSTED` before execution when
    no stream/cursor slot is available. Retry after a stream finishes. The
    default `buffer` preserves the behavior below. This option does not bound
    driver execution memory for joins, aggregates or other non-streaming queries.
  - A query that could stream but finds no stream free (past
    `--max-streams` or `--max-table-streams`, or with the index near its
    cursor limit) is read into memory before it is sent, and its
    connection and cursor are given back at once. Such results share
    `--result-memory` (1 GiB) across the server until they have been sent. One that doesn't fit fails with `RESOURCE_EXHAUSTED`
    as soon as it would go over, and clients can retry, or read fewer rows
    or columns.
  - A client that takes longer than `--stream-stall-timeout` (2 minutes) to
    read a batch gets a `DEADLINE_EXCEEDED` error instead of the rest, and
    the server stops reading from Redis. Redis deletes a cursor that isn't
    read for 5 minutes.
  - To read every result whole instead, pass
    `-adbc-option adbc.redis.stream_results=false`.
- **Batches are sent at about 2 MiB each,** under gRPC's default 4 MiB limit.
