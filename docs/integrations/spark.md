# Apache Spark

[All integration guides](README.md). This guide reads SQL rows through Flight
SQL and writes a separate output table, then verifies it through a new SELECT.

## Install

Start the [server and sample table](README.md#optional-sample-sql-table).
From the repository root, create a separate environment:

```sh
python3.12 -m venv .venv-spark
. .venv-spark/bin/activate
python -m pip install -r requirements/integrations/spark.txt
make bin/flight-sql-jdbc-driver-19.0.0.jar
export FLIGHT_JDBC_JAR="$PWD/bin/flight-sql-jdbc-driver-19.0.0.jar"
```

The profile pins the November 3 dependency versions. The examples default to
`grpc://localhost:8815` without client TLS/auth; use `FLIGHT_URI` for another
reachable local development endpoint. Use the [connection reference](../client-guide.md)
for secure deployment configuration.

## Read, write and verify

Save as `spark_example.py` and run `python spark_example.py`.

```python
import os
import uuid
import pyarrow as pa
import adbc_driver_flightsql.dbapi as sql
from pyspark.sql import SparkSession
uri = os.environ.get("FLIGHT_URI", "grpc://localhost:8815")
jar = os.path.abspath(os.environ["FLIGHT_JDBC_JAR"])
spark = (SparkSession.builder.master("local[2]").appName("flight-guide")
    .config("spark.jars", jar)
    .config("spark.driver.extraJavaOptions", "--add-opens=java.base/java.nio=ALL-UNNAMED")
    .config("spark.executor.extraJavaOptions", "--add-opens=java.base/java.nio=ALL-UNNAMED")
    .getOrCreate())
try:
    frame = spark.read.jdbc(
        "jdbc:arrow-flight-sql://" + uri.removeprefix("grpc://") + "/?useEncryption=false",
        "flight_tool_sample", properties={"driver": "org.apache.arrow.driver.jdbc.ArrowFlightJdbcDriver"})
    frame.show()
    transformed = frame.selectExpr("id", "amount * 2 AS amount").orderBy("id")
    # Small local bridge: collect rows before writing through ADBC.
    output = pa.Table.from_pylist([row.asDict() for row in transformed.collect()])
    # Write the tool's output through Flight SQL, then verify the stored values.
    name = "guide_" + uuid.uuid4().hex
    with sql.connect(uri, autocommit=True) as conn, conn.cursor() as cur:
        try:
            cur.adbc_ingest(name, output, mode="create")
            cur.execute(f'SELECT * FROM "{name}" ORDER BY id')
            received = cur.fetch_arrow_table()
            assert received.equals(output)
            print(received)
        finally:
            cur.execute(f'DROP TABLE IF EXISTS "{name}"')
            cur.fetch_arrow_table()
finally:
    spark.stop()
```

Install a full JDK 21 and set JAVA_HOME. The recorded route validated Spark
JDBC reads, including predicate partitions. This write example collects a small
result in the driver and uses ADBC; it does not establish Spark
`DataFrameWriter.jdbc` support or a distributed Flight SQL sink. Do not collect
unbounded production data.

## Validation and cleanup

The [November 3 results](../validation/2026-11-03-flight-integrations/README.md)
record the historical workflow for this tool. The new read/write recipe above
has not been run against a live endpoint; fixture writes through another
client and local artifact saves do not prove this tool's database write support.
SQL uses autocommit without transactions. The example drops its own unique
output table; remove the shared input with the
[fixture cleanup](README.md#optional-sample-sql-table) after finishing the guides.
The historical run does not establish Enterprise, TLS/auth or distributed
execution qualification.
