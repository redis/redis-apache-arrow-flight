# Run locally from source

[Back to the README](../README.md). Run these commands from the repository root.

You need Go 1.26.10 or newer, Docker and, for the Python example,
[uv](https://docs.astral.sh/uv/). For tagged builds, see [Install a release](../README.md#install-a-release).

```bash
docker compose up --detach --wait redis
```

```bash
make run
```

The server listens on `localhost:8815`. In another terminal, run the Python
tour (SQL with the ADBC Flight SQL driver, then datasets with
`pyarrow.flight`):

```bash
uv run examples/python/quickstart.py
```

To run Redis and the server together in Docker instead:

```bash
docker compose up --detach --build
```

Compose waits for Redis health before starting Flight. Verify Flight readiness
with a SQL query; see [startup and readiness](build-profiles.md#startup-and-readiness)
for profile-specific behavior.

## Build artifacts

Source builds download the pinned [public Redis ADBC driver](https://github.com/redis/redis-adbc-driver).
No GitHub token or private dependency secret is required for local, Docker,
or GitHub Actions builds. Run `make build` for a local binary, or `make docker`
for a container image. Public module downloads use Go checksum verification
and the checked-in `go.sum`.
