# Session refusal contract

The session must keep its existing ADBC connection for the direct native Go
`adbc.Error` with all of:

- `Code == StatusIO`, `VendorCode == 0`;
- `SqlState == 40001`;
- exact `Msg == "[redis] snapshot catalog changed concurrently; retry the statement"`.

No substring classification, arbitrary IO/40001 reuse, or unwrapping is allowed
for this exception. Wrappers, joined transport/cancellation errors, altered
diagnostics and gRPC status errors do not establish a safe native outcome.
Existing statement-local classifications (including NotFound) remain intact.
The original error still maps to Flight `UNAVAILABLE`. No automatic retry is
introduced. Classification does not make a multi-statement request atomic or
make a consumed Arrow reader replayable.

Shared read-snapshot attempts remain terminal on any failed execution. Generic
connection-error handling outside this exact exception is unchanged, including
its existing option restoration and loss of connection-local temporary state.

## Source-bound reasoning

Driver default branch checked 2026-11-16; requester-selected latest code is
[`640dbf1fa8809fb9e12e761c4edd11373a4233f1`](https://github.com/redis/redis-adbc-driver/commit/640dbf1fa8809fb9e12e761c4edd11373a4233f1),
Go replacement `v0.0.0-20261117055145-640dbf1fa880`.

- [snapshot.go](https://github.com/redis/redis-adbc-driver/blob/640dbf1fa8809fb9e12e761c4edd11373a4233f1/go/snapshot.go):
  `beginSnapshotWrite` emits the diagnostic after a definite reservation
  refusal; `publishPreparedSnapshot` emits it only when publication/rebase
  returns zero without an error. A failed/unknown outcome uses a different
  diagnostic. `executeSnapshot` aborts the failed workspace and refreshes
  temporary state from its base view; it does not close the connection or reset
  session settings.
- [snapshot_publication.go](https://github.com/redis/redis-adbc-driver/blob/640dbf1fa8809fb9e12e761c4edd11373a4233f1/go/snapshot_publication.go):
  `rebaseSnapshotPublication` retries only metadata publication, at most eight
  times; it never re-executes SQL or consumes Arrow input again.
- [sqlstate.go](https://github.com/redis/redis-adbc-driver/blob/640dbf1fa8809fb9e12e761c4edd11373a4233f1/go/sqlstate.go):
  `stateErrorf`/`codeStateErrorf` produce the direct Go error with StatusIO,
  SQLSTATE and Redis prefix. Its native VendorCode is zero; vendor14 observed by
  the downstream Flight client is a different transport layer.
- Flight `session.run` previously rejected all such IO errors, called
  `dropConn`, and reopened on the next operation. `session.opts` records Flight
  session options, not SQL SET statements or temporary objects. That explains
  why reopening can silently return UTC and lose the temporary relation.

The exact diagnostic is a pinned-driver compatibility dependency. A future
driver change must review this exception and run its native fault-injection
test; unfamiliar diagnostics continue through conservative connection cleanup.
