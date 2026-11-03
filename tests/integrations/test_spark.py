"""Spark JDBC datasource with Flight SQL JDBC, including predicate partitions."""
import os
from pathlib import Path


def test_spark_jdbc(uri, workflow_rows, tmp_path, monkeypatch):
    from pyspark.sql import SparkSession
    jar = Path(os.environ['FLIGHT_JDBC_JAR']).resolve()
    assert jar.is_file(), jar
    monkeypatch.setenv('SPARK_LOCAL_IP', '127.0.0.1')
    host = uri.removeprefix('grpc://')
    spark = (SparkSession.builder.master('local[2]').appName('flight-integration')
             .config('spark.ui.enabled', 'false')
             .config('spark.jars', str(jar))
             .config('spark.driver.extraJavaOptions', '--add-opens=java.base/java.nio=ALL-UNNAMED')
             .config('spark.executor.extraJavaOptions', '--add-opens=java.base/java.nio=ALL-UNNAMED')
             .config('spark.driver.bindAddress', '127.0.0.1')
             .config('spark.driver.host', '127.0.0.1')
             .config('spark.sql.warehouse.dir', str(tmp_path/'warehouse')).getOrCreate())
    try:
        url = f'jdbc:arrow-flight-sql://{host}/?useEncryption=false'
        props = {'driver': 'org.apache.arrow.driver.jdbc.ArrowFlightJdbcDriver'}
        name, expected = workflow_rows
        frame = spark.read.jdbc(url, name, properties=props)
        assert [r.asDict() for r in frame.orderBy('id').collect()] == expected.to_pylist()
        assert frame.filter('id >= 2').count() == 2
        assert frame.groupBy().sum('amount').collect()[0][0] == 10.0
        split = spark.read.jdbc(url, name, predicates=['id < 2', 'id >= 2'], properties=props)
        assert [r.asDict() for r in split.orderBy('id').collect()] == expected.to_pylist()
        assert frame.filter('id < 0').count() == 0
    finally:
        spark.stop()
