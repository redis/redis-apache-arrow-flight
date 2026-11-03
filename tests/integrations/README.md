# Flight SQL tool workflows

Run the [integration harness](../../docs/integrations/testing.md). All database
connections here use ADBC Flight SQL or Flight SQL JDBC. No native Redis ADBC
client library is imported by these tests. The separately deployed server
loads its pinned Redis ADBC implementation.

The model, plotting, notebook, dataset, Streamlit, SciPy and local orchestrator
workflows are adapted from `redis/redis-adbc-driver`, published revision
`bb83527f305b75ea731696f55f78eb75121bbbc0`, `python/tests/`. Those upstream files
were changed to use Flight SQL, explicit autocommit, endpoint propagation into
child processes, and local fixtures. Spark now tests its JDBC datasource rather
than the native Arrow bridge. Resource, client, Airflow and document tests use
Flight-specific configuration. The upstream Apache 2.0 license is retained in
[LICENSE](LICENSE).

Tests assert actual values and schemas, NULLs/Unicode, persisted outputs or
application task results. Small local model workflows do not establish a
packaged database connector or production/distributed deployment support.

Hugging Face Datasets 5.1.0 has two strict expected failures for default empty
save/reload: an independent library reproduction and a Flight SQL reproduction.
Only its exact zero-shard error is expected. The explicit one-shard workaround
must pass; other errors fail the profile. A future unexpected pass also fails,
so the expectation must be reviewed when upgrading the dependency.

The two empty-save expected failures are tracked by [Flight #57](https://github.com/redis/redis-apache-arrow-flight/issues/57) and [Datasets #8912](https://github.com/huggingface/datasets/issues/8912).
