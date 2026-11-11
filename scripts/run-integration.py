#!/usr/bin/env python3
"""Serialize cooperating backend validation commands for one managed catalog.

Use a shared --scope/FLIGHT_INTEGRATION_SCOPE for Redis/Flight aliases, and a
shared --lock-dir/FLIGHT_INTEGRATION_LOCK_DIR for containers on one host. Separate
hosts must use separate disposable databases. Locking never retries SQL writes.
"""
import argparse
import fcntl
import hashlib
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
from urllib.parse import parse_qs, urlsplit


def database_scope(uri):
    try:
        parsed = urlsplit(uri)
        if parsed.scheme != "rediss" or not parsed.hostname:
            raise ValueError()
        path_db = parsed.path.strip("/") or "0"
        database = int(parse_qs(parsed.query).get("db", [path_db])[-1])
        if database < 0:
            raise ValueError()
        return f"redis:{parsed.hostname.lower().rstrip('.')}:{parsed.port or 6379}/{database}"
    except ValueError:
        raise ValueError("REDIS_URI must identify a verified TLS backend") from None


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scope", default=os.environ.get("FLIGHT_INTEGRATION_SCOPE"))
    parser.add_argument("--lock-dir", default=os.environ.get(
        "FLIGHT_INTEGRATION_LOCK_DIR", str(Path(tempfile.gettempdir()) / "redis-flight-validation-locks")))
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    command = args.command[1:] if args.command[:1] == ["--"] else args.command
    if not command:
        parser.error("a validation command is required after --")
    scope = args.scope
    if not scope:
        if not os.environ.get("REDIS_URI"):
            parser.error("set FLIGHT_INTEGRATION_SCOPE (shared by all suites using this database) or REDIS_URI")
        try:
            scope = database_scope(os.environ["REDIS_URI"])
        except ValueError as exc:
            parser.error(str(exc))
    directory = Path(args.lock_dir)
    directory.mkdir(mode=0o700, parents=True, exist_ok=True)
    path = directory / (hashlib.sha256(scope.encode()).hexdigest() + ".lock")
    fd = os.open(path, os.O_CREAT | os.O_RDWR, 0o600)
    with os.fdopen(fd, "a") as lock:
        print("Waiting for backend validation scope lock...", flush=True)
        fcntl.flock(lock, fcntl.LOCK_EX)
        print("Acquired backend validation scope lock.", flush=True)
        process = subprocess.Popen(command, start_new_session=True)
        forwarded = None
        def forward(signum, _frame):
            nonlocal forwarded
            forwarded = signum
            try:
                os.killpg(process.pid, signum)
            except ProcessLookupError:
                pass
        previous = {sig: signal.signal(sig, forward) for sig in (signal.SIGTERM, signal.SIGINT)}
        try:
            code = process.wait()
        finally:
            for sig, handler in previous.items():
                signal.signal(sig, handler)
            # Keep the lock until the child has actually exited on failure too.
            if process.poll() is None:
                process.terminate()
                process.wait()
        if forwarded is not None:
            return 128 + forwarded
        return code if code >= 0 else 128 - code


if __name__ == "__main__":
    sys.exit(main())
