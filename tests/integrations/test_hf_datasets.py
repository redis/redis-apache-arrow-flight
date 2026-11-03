"""Custom Redis ADBC -> Arrow -> Hugging Face Dataset workflow, entirely local."""

import json

import pyarrow as pa
import pytest
import adbc_driver_flightsql.dbapi as dbapi


class EmptyDatasetPersistenceError(AssertionError):
    """Only the independently reproduced Datasets 5.1.0 zero-shard failure."""


known_empty_failure = pytest.mark.xfail(
    raises=EmptyDatasetPersistenceError, strict=True,
    reason="Datasets 5.1.0 cannot reload its default zero-shard empty save (#253)",
)


def reload_dataset(datasets, path):
    try:
        return datasets.load_from_disk(str(path))
    except IndexError as exc:
        state = json.loads((path / "state.json").read_text())
        if (datasets.__version__ == "5.1.0" and state["_data_files"] == []
                and str(exc) == "list index out of range"):
            raise EmptyDatasetPersistenceError(str(exc)) from exc
        raise


@pytest.mark.parametrize("empty", [False, pytest.param(True, marks=known_empty_failure)])
def test_local_arrow_dataset_default_save_reload(tmp_path, empty):
    """An independent dependency regression: no Redis, ADBC handles or network."""
    datasets = pytest.importorskip("datasets")
    schema = pa.schema([("id", pa.int64()), ("label", pa.string())])
    table = pa.Table.from_pylist([] if empty else [{"id": 1, "label": "雪"}], schema=schema)
    dataset = datasets.Dataset(table)
    path = tmp_path / "default_dataset"
    dataset.save_to_disk(str(path))
    restored = reload_dataset(datasets, path)
    assert restored.to_list() == table.to_pylist()
    assert restored.features == dataset.features
    assert restored.data.schema == dataset.data.schema


@pytest.mark.parametrize("empty, explicit_shard", [
    (False, False), (True, True),
    pytest.param(True, False, marks=known_empty_failure),
])
def test_arrow_dataset_save_reload_and_filter(
    uri, workflow_rows, tmp_path, monkeypatch, empty, explicit_shard
):
    name, expected = workflow_rows
    monkeypatch.setenv("HF_HUB_OFFLINE", "1")
    monkeypatch.setenv("HF_DATASETS_OFFLINE", "1")
    monkeypatch.setenv("HF_HOME", str(tmp_path / "cache"))
    datasets = pytest.importorskip("datasets")
    predicate = " WHERE FALSE" if empty else ""
    with dbapi.connect(uri, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(f'SELECT * FROM "{name}"{predicate} ORDER BY id')
        table = cur.fetch_arrow_table()
    expected = expected.slice(0, 0) if empty else expected

    dataset = datasets.Dataset(table)
    assert dataset.to_list() == expected.to_pylist()
    assert dataset.data.schema.remove_metadata() == expected.schema.remove_metadata()
    path = tmp_path / "saved_dataset"
    # datasets 5.1.0 defaults to zero shards for an empty table, which its
    # loader cannot reopen. One shard preserves even the empty result schema.
    dataset.save_to_disk(str(path), **({"num_shards": 1} if explicit_shard else {}))
    restored = reload_dataset(datasets, path)
    assert restored.to_list() == expected.to_pylist()
    assert restored.features == dataset.features
    filtered = restored.filter(lambda row: row["id"] >= 2, load_from_cache_file=False)
    assert filtered.to_list() == [row for row in expected.to_pylist() if row["id"] >= 2]
