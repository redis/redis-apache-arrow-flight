# Data access reference

[Back to the README](../README.md). This reference describes query cancellation
and named Arrow dataset operations and storage.

## Contents

- [Query cancellation](#query-cancellation)
- [Datasets](#datasets)

## Query cancellation

`CancelFlightInfo` cancels a pending read-only SQL ticket or all active reads
of it, including prepared-query tickets returned by GetFlightInfo. The legacy
Flight SQL `CancelQuery` action maps to the same implementation. Active
cancellation is cooperative: readers poll Redis every 250 ms, then cancel the
execution context and release their cursors, connections and retained results.
A completed query or already-executed write returns `NOT_CANCELLABLE`;
cancellation never rolls back an autocommitted write. A racing active read can
return `CANCELLING` until cleanup finishes. Partial data already sent may have
reached the client; `CANCELLED` means the result is incomplete.

Cancellation records live under `arrowflight:query:{<id>}` (or `-key-prefix`)
and contain an opaque identity digest and lifecycle state, with no SQL or bound
values. Newly issued cancellable tickets expire after 10 minutes without an
active reader; readers renew that lease. Expired/unknown records give
`NOT_FOUND`; another Redis identity gets `PERMISSION_DENIED`. Repeated
cancellation is safe. Legacy tickets still work without explicit cancellation.
With `--auth=none`, clients share one Redis identity and its cancellation scope.

The Redis identity needs EVAL/GET/SET permissions on the query-metadata
keys to create a cancellable ticket. If metadata creation is denied, SQL still
works with a non-cancellable ticket. Other Redis errors remain errors. For
multiple replicas, use the same Redis, key prefix and Redis identity; cancellation
state is shared there. This does not make cookie sessions portable.

## Datasets

- **Name.** A dataset's name is its `PATH` descriptor's segments joined with
  `/`.
- **DoPut** stores the stream as the dataset's new *generation*, then switches
  to it atomically with a Lua script. Readers never see a half-written upload;
  generation numbers only go up, so of two uploads the one that started later
  wins. An older one that finishes last is replaced at once: its reply says
  `"superseded": true`, and its data expires after the grace period. The
  reply's app metadata is JSON: `dataset`, `generation`, `rows`, `batches`,
  `bytes`, `updated`, and `superseded` when set.
- **GetFlightInfo / GetSchema** return the schema, row and byte counts, and a
  ticket for the current generation. A ticket for a replaced generation keeps
  working for 5 minutes, so reads already under way finish.
- **DoGet** takes a ticket from GetFlightInfo, or one you write yourself. A
  ticket is JSON:

  | Field | Meaning |
  |-|-|
  | `dataset` | The name. Required |
  | `generation` | The generation to read. Leave it out for the current one |
  | `columns` | Read just these columns, in this order. A name the dataset doesn't have, or one given twice, is `InvalidArgument` |
  | `batches` | `[start, end]`: read the stored batches from `start` up to, not including, `end`, counting from 0. An `end` past the last batch reads to the last; `start` equal to `end` returns the schema alone. A negative `start`, or an `end` before `start`, is `InvalidArgument` |

  For example, `{"dataset": "features/v1"}` reads the current generation, and
  `{"dataset": "features/v1", "columns": ["id", "label"], "batches": [0, 4]}`
  reads two columns of its first four batches. The stored batches are the
  uploaded ones, split to about 2 MiB each; the PutResult and the FlightInfo's
  app metadata count them (`batches`). Each column of a batch is stored on
  its own, so a read takes only the ticket's columns from Redis. The
  [dataset benchmarks](../server/dataset_bench_test.go) measure full and
  one-column reads. A read takes the columns about 8 MiB at a time (up to
  64 batches), so it holds about that much of the dataset, whatever its size.
- **Parallel reads.** With `--dataset-endpoint-batches=N`, GetFlightInfo,
  PollFlightInfo and ListFlights return one endpoint per N batches. Clients can
  read the endpoints in parallel, and must read them all; together, in order,
  they are the dataset (the FlightInfo says `ordered`). Without the flag there
  is one endpoint, since a client that reads only the first would miss data.
- **ListFlights** lists datasets; the criteria expression, if given, is a name
  prefix.
- **`drop_dataset` action** deletes a dataset. Its body is the name, or
  `{"dataset": "<name>"}`.
- **Expiry.** `--dataset-ttl` makes uploaded datasets expire.

Keys (the prefix is set with `--key-prefix`, default `arrowflight:`). Every
key of one dataset shares a hash tag, so on a Redis Cluster it lives in one
slot:

| Key | Type | Holds |
|-|-|-|
| `arrowflight:ds:{<name>}` | HASH | `gen`, `schema` (IPC), `rows`, `batches`, `bytes`, `updated` |
| `arrowflight:ds:{<name>}:g<N>` | HASH | Generation N, by column: `layout` (`2`); `schema`, an IPC stream with the schema alone; `batches`, the number of batches (written last, so a generation without it is an unfinished upload); and for each batch `b` (from 0), `<b>:rows`, its row count, and `<b>:<c>`, an IPC stream of its column `c` alone (counted from 0, in the schema's order) |
| `arrowflight:ds:{<name>}:g<N>` | LIST | A generation stored by an earlier server: an IPC stream with the schema alone, then one IPC stream per batch. It stays readable, but a read of some columns takes them all from Redis |
| `arrowflight:ds:{<name>}:seq` | STRING | The last generation number. It never expires, not even with `--dataset-ttl` or a drop, so generation numbers never repeat |
| `arrowflight:result:{<id>}` | STRING | A statement result too big for its ticket (expires after 10 minutes) |

The name in a key is URL-path-escaped.

Storing each column of a batch on its own can use more Redis memory than
storing batches whole, since each column of each batch is a separate value.

**Upgrading.** Servers before the HASH layout fail to read the generations
newer ones store (Redis answers their `LLEN` with `WRONGTYPE`), so upgrade
every replica before uploading again. Generations they stored stay readable
by newer servers; upload a dataset again to read its columns separately.
Commits and drops work across versions.
