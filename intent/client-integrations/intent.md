# Client integration validation

Requested by the user on 2026-10-29: test DBeaver, Apache Superset,
`pd.read_sql`, Polars, DuckDB using Flight, and R integration.

Scope: exercise actual clients against a disposable Redis/Flight deployment,
retain repeatable smoke tests, and distinguish passes, dependency blockers and
application checks not performed. No server protocol changes are required.

Source baseline: `84ea021`. Status: local validation recorded; manual DBeaver
connection, navigator browsing and a three-row SELECT passed on 2026-10-30.
Accessibility automation crashed. Superset was excluded by the user's follow-up
instruction after upstream reporting. Maintainer review pending.
