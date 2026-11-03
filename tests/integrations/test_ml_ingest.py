import pytest


def query_arrow(uri, sql):
    import adbc_driver_flightsql.dbapi as dbapi

    with dbapi.connect(uri, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(sql)
        return cur.fetch_arrow_table()


def test_numpy_from_flight(uri, workflow_rows):
    import numpy as np
    table = query_arrow(uri, f'SELECT id, amount FROM "{workflow_rows[0]}" ORDER BY id')
    np.testing.assert_array_equal(table['id'].to_numpy(), [1, 2, 3])
    np.testing.assert_allclose(table['amount'].to_numpy(), [2.5, np.nan, 7.5], equal_nan=True)


def test_scikit_learn_from_flight(uri, workflow_rows):
    import numpy as np
    from sklearn.tree import DecisionTreeRegressor
    table = query_arrow(uri, f'SELECT id, amount FROM "{workflow_rows[0]}" WHERE amount IS NOT NULL ORDER BY id')
    features = table['id'].to_numpy().reshape(-1, 1)
    labels = table['amount'].to_numpy()
    model = DecisionTreeRegressor(random_state=0).fit(features, labels)
    np.testing.assert_allclose(model.predict(features), [2.5, 7.5])
