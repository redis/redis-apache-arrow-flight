# Redis Insight

[All integration guides](README.md). Redis Insight connects to the backing
Redis database. It does not use the Flight endpoint. No Redis Insight
application validation is recorded for this project.

## Connect to Redis

1. Install [Redis Insight](https://redis.io/docs/latest/develop/tools/insight/).
2. Find the Redis host, port and database used by the Flight server's
   `REDIS_URI` or `--redis-uri` setting.
3. Add that database in Redis Insight, supplying the corresponding Redis ACL
   credentials and TLS settings. For a default local Redis this is
   `localhost:6379`, database 0; Flight's default `8815` is a different service.
4. Open the key browser and inspect the database.

For Redis Cluster or a managed deployment, select the matching connection
configuration and use addresses reachable from Redis Insight. Flight login
tokens and JDBC URLs are not Redis connection details.

## Inspect SQL rows and datasets

Create the [sample SQL table](README.md#optional-sample-sql-table) through ADBC,
then locate its row HASHes and Search index in Redis Insight. Their layout
follows the Redis ADBC driver's [rows and indexes conventions](https://github.com/redis/redis-adbc-driver/blob/main/docs/driver-reference.md#rows-and-indexes).
Arbitrary existing Redis keys do not automatically become SQL tables.

Named datasets use the server's own keys (default prefix `arrowflight:`), with
Arrow IPC payloads. See [dataset storage](../data-access.md#datasets) for the
key layout and configurable prefix. Use a [native Flight client](python.md#upload-and-download-a-dataset)
to decode and inspect those values as Arrow tables.

## Verify and clean up

Confirm that a row ingested through Flight SQL is visible as a Redis HASH,
then query the table through Flight SQL to compare its values. Keep dataset
metadata and payloads consistent by using the Flight API for dataset changes.
Remove the sample table through the [fixture cleanup](README.md#optional-sample-sql-table).
These are suggested acceptance checks, not recorded Redis Insight passes.

## Write and read back a disposable Redis value

In Redis Insight's Workbench, use an unused key outside the Flight dataset
prefix. Execute one command at a time:

```text
SET guide:insight:example hello NX
GET guide:insight:example
DEL guide:insight:example
GET guide:insight:example
```

Continue only if SET returns OK; a nil response means the key already exists,
so choose a different key rather than deleting someone else's value. Expect
`hello`, deletion count `1`, then nil. This demonstrates a direct Redis write;
it does not create a Flight SQL table. The [live guide run](../validation/2026-11-03-guide-recipes/README.md) passed these commands directly
over verified Redis TLS. Redis Insight was not installed, so its UI remains
untested.
For SQL-compatible writes use the [Python SQL write example](python.md#write-and-verify-a-sql-table).
