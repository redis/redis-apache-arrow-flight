"""Small CPU model input workflows fed by the remote Redis Flight SQL server."""

import pytest
import adbc_driver_flightsql.dbapi as dbapi


def read_rows(uri, name):
    with dbapi.connect(uri, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(f'SELECT * FROM "{name}" ORDER BY id')
        return cur.fetch_arrow_table()


def test_xgboost_train_save_reload(uri, workflow_rows, tmp_path):
    np = pytest.importorskip("numpy")
    xgb = pytest.importorskip("xgboost")
    name, _ = workflow_rows
    table = read_rows(uri, name)
    features = np.asarray(table["amount"].to_pylist(), dtype=float).reshape(-1, 1)
    labels = table["id"].to_numpy().astype(float)
    data = xgb.DMatrix(features, label=labels, missing=np.nan)
    model = xgb.train(
        {"objective": "reg:squarederror", "max_depth": 2, "eta": 1,
         "min_child_weight": 0, "lambda": 0, "nthread": 1, "seed": 0},
        data, num_boost_round=4,
    )
    np.testing.assert_allclose(model.predict(data), labels, atol=0.01)
    path = tmp_path / "model.json"
    model.save_model(path)
    restored = xgb.Booster(model_file=path)
    np.testing.assert_array_equal(restored.predict(data), model.predict(data))


def test_pytorch_dataloader_and_forward(uri, workflow_rows):
    np = pytest.importorskip("numpy")
    torch = pytest.importorskip("torch")
    from torch.utils.data import DataLoader, TensorDataset

    name, _ = workflow_rows
    table = read_rows(uri, name)
    amounts = torch.tensor(np.asarray(table["amount"].to_pylist(), dtype=np.float32))
    valid = torch.isfinite(amounts)
    values = torch.nan_to_num(amounts).reshape(-1, 1)
    loader = DataLoader(TensorDataset(values, valid), batch_size=2, shuffle=False)
    batches = list(loader)
    torch.testing.assert_close(torch.cat([b[0] for b in batches]),
                               torch.tensor([[2.5], [0.0], [7.5]]))
    assert torch.cat([b[1] for b in batches]).tolist() == [True, False, True]
    model = torch.nn.Linear(1, 1)
    with torch.no_grad():
        model.weight.fill_(2)
        model.bias.fill_(1)
        actual = model(values)
    torch.testing.assert_close(actual, torch.tensor([[6.0], [1.0], [16.0]]))


def test_tensorflow_dataset_batches(uri, workflow_rows):
    np = pytest.importorskip("numpy")
    tf = pytest.importorskip("tensorflow")
    name, _ = workflow_rows
    table = read_rows(uri, name)
    raw = np.asarray(table["amount"].to_pylist(), dtype=np.float32)
    data = tf.data.Dataset.from_tensor_slices(
        {"id": table["id"].to_numpy(), "value": np.nan_to_num(raw), "valid": np.isfinite(raw)}
    ).batch(2)
    options = tf.data.Options()
    options.threading.private_threadpool_size = 1
    batches = list(data.with_options(options).as_numpy_iterator())
    np.testing.assert_array_equal(np.concatenate([b["id"] for b in batches]), [1, 2, 3])
    np.testing.assert_allclose(np.concatenate([b["value"] for b in batches]), [2.5, 0.0, 7.5])
    np.testing.assert_array_equal(np.concatenate([b["valid"] for b in batches]), [True, False, True])


def test_keras_model_input_and_persistence(uri, workflow_rows, tmp_path, monkeypatch):
    monkeypatch.setenv("KERAS_BACKEND", "tensorflow")
    np = pytest.importorskip("numpy")
    keras = pytest.importorskip("keras")
    name, _ = workflow_rows
    table = read_rows(uri, name)
    values = np.nan_to_num(np.asarray(table["amount"].to_pylist(), dtype=np.float32)).reshape(-1, 1)
    model = keras.Sequential([keras.layers.Input(shape=(1,)), keras.layers.Dense(1)])
    model.layers[0].set_weights([np.array([[2.0]], dtype=np.float32), np.array([1.0], dtype=np.float32)])
    expected = np.array([[6.0], [1.0], [16.0]], dtype=np.float32)
    np.testing.assert_allclose(model(values, training=False).numpy(), expected)
    path = tmp_path / "model.keras"
    model.save(path)
    restored = keras.models.load_model(path)
    np.testing.assert_allclose(restored(values, training=False).numpy(), expected)


def test_transformers_local_tokenizer_model(uri, workflow_rows, tmp_path, monkeypatch):
    monkeypatch.setenv("HF_HUB_OFFLINE", "1")
    monkeypatch.setenv("TRANSFORMERS_OFFLINE", "1")
    torch = pytest.importorskip("torch")
    pytest.importorskip("transformers")
    from tokenizers import Tokenizer
    from tokenizers.models import WordLevel
    from tokenizers.pre_tokenizers import Whitespace
    from transformers import BertConfig, BertModel, PreTrainedTokenizerFast

    name, _ = workflow_rows
    table = read_rows(uri, name)
    local = Tokenizer(WordLevel({"[UNK]": 0, "[PAD]": 1, "alpha": 2, "雪": 3}, unk_token="[UNK]"))
    local.pre_tokenizer = Whitespace()
    tokenizer = PreTrainedTokenizerFast(tokenizer_object=local, unk_token="[UNK]", pad_token="[PAD]")
    batch = tokenizer([x or "" for x in table["label"].to_pylist()], padding=True, return_tensors="pt")
    assert batch["input_ids"].tolist() == [[2], [1], [3]]
    assert batch["attention_mask"].tolist() == [[1], [0], [1]]
    config = BertConfig(vocab_size=4, hidden_size=16, num_hidden_layers=1,
                        num_attention_heads=2, intermediate_size=32)
    model = BertModel(config).eval()
    with torch.no_grad():
        expected = model(**batch).last_hidden_state
    assert expected.shape == (3, 1, 16)
    assert torch.isfinite(expected).all()
    model.save_pretrained(tmp_path / "local-model")
    restored = BertModel.from_pretrained(tmp_path / "local-model", local_files_only=True).eval()
    with torch.no_grad():
        torch.testing.assert_close(restored(**batch).last_hidden_state, expected)
