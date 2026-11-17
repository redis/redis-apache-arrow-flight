"""Cleanup must preserve a primary extraction/cancellation failure."""
import importlib.util
from pathlib import Path
from unittest.mock import Mock

import pytest

spec = importlib.util.spec_from_file_location(
    "snapshot_example", Path(__file__).resolve().parents[1] / "examples/python/shared_snapshot.py"
)
example = importlib.util.module_from_spec(spec)
spec.loader.exec_module(example)


def connection():
    conn = Mock()
    conn.adbc_connection.get_option.return_value = '{"state":"active","handle":"owned"}'
    def options(**kwargs):
        if kwargs.get(example.SESSION + "redis.read_snapshot") == '{"action":"release"}':
            raise OSError("cleanup transport failed")
    conn.adbc_connection.set_options.side_effect = options
    return conn


def test_cleanup_preserves_primary_error():
    primary = RuntimeError("extraction failed")
    with pytest.raises(RuntimeError) as raised:
        with example.shared_snapshot(connection()):
            raise primary
    assert raised.value is primary


def test_successful_extraction_still_fails_if_release_fails():
    with pytest.raises(OSError, match="cleanup transport failed"):
        with example.shared_snapshot(connection()):
            pass


def test_generator_close_releases_without_replacing_generator_exit():
    conn = connection()
    def records():
        with example.shared_snapshot(conn):
            yield 1
    generator = records()
    assert next(generator) == 1
    generator.close()
    assert conn.adbc_connection.set_options.call_args.kwargs == {
        example.SESSION + "redis.read_snapshot": '{"action":"release"}'
    }
