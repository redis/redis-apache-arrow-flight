# Intent: update the Redis ADBC driver and track endpoint gaps

Requested by the repository user on 2026-10-20. Source baseline: `65fce39165583df9689d93f78d282d1bebb9d228`.
Status: implemented and locally validated; GitHub issues created; no maintainer review recorded.

This project is the Flight SQL and Flight RPC endpoint backed by Redis. Update its Redis ADBC driver to the latest verified upstream revision, always check upstream before future updates, and create GitHub issues for remaining endpoint gaps without duplicating existing work.

Preserve SQL/dataset routing, identity boundaries, ticket replay behavior, and storage compatibility. This change does not implement the separately tracked endpoint features.

See [spec.md](spec.md) and [plan.md](plan.md).
