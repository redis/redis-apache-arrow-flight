"""Flight SQL shared snapshot protocol v1 (requires independent deployment validation).

Use one fresh TLS/authenticated ADBC Flight SQL connection with cookie middleware
for each attempt. Close all cursors/readers inside this context. Never retry on a
new connection or treat partial records as a successful full refresh.
"""

from contextlib import contextmanager
import json

SESSION = "adbc.flight.sql.session.option."
HEADER = "adbc.flight.sql.rpc.call_header.x-redis-read-snapshot"


@contextmanager
def shared_snapshot(connection, retention_ms=900000):
    """Acquire before discovery, then release on success, error or generator close."""
    options = connection.adbc_connection
    # Acquisition is the feature probe. Unknown options, missing sessions, old
    # drivers, plaintext and unauthenticated deployments must raise here.
    options.set_options(**{
        SESSION + "redis.read_snapshot": json.dumps({
            "action": "acquire", "retention_ms": retention_ms,
        })
    })
    try:
        view = json.loads(options.get_option(SESSION + "redis.read_snapshot_status"))
        if view.get("state") != "active" or not view.get("handle"):
            raise RuntimeError("Flight SQL gateway did not acquire a shared snapshot")
        options.set_options(**{HEADER: view["handle"]})
        yield view
    finally:
        # Keep the guard installed: reuse after release must fail. Closing the
        # ADBC connection also closes its Flight session. If transport cleanup
        # fails, gateway idle expiry/native fixed leases bound retention.
        options.set_options(**{
            SESSION + "redis.read_snapshot": '{"action":"release"}'
        })
