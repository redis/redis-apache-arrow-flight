"""Build and solve a sparse linear system from stored Redis ADBC query rows."""

from uuid import uuid4

import pyarrow as pa
import pytest


def test_sparse_system_from_redis_adbc(connection):
    np = pytest.importorskip("numpy")
    pytest.importorskip("scipy")
    from scipy.sparse import coo_matrix
    from scipy.sparse.linalg import spsolve

    name = "it_scipy_" + uuid4().hex
    coefficients = pa.table(
        {
            "row_id": [0, 0, 1, 1],
            "column_id": [0, 1, 0, 1],
            "value": [3.0, 1.0, 1.0, 2.0],
        }
    )
    with connection.cursor() as cur:
        try:
            cur.adbc_ingest(name, coefficients, mode="create")
            cur.execute(f'SELECT * FROM "{name}" ORDER BY row_id, column_id')
            table = cur.fetch_arrow_table()
        finally:
            cur.execute(f'DROP TABLE IF EXISTS "{name}"')

    matrix = coo_matrix(
        (
            table["value"].to_numpy(),
            (table["row_id"].to_numpy(), table["column_id"].to_numpy()),
        ),
        shape=(2, 2),
    ).tocsr()
    target = np.array([9.0, 8.0])
    solution = spsolve(matrix, target)
    np.testing.assert_allclose(solution, [2.0, 3.0])
    np.testing.assert_allclose(matrix @ solution, target)
