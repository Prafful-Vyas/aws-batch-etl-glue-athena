import pytest


@pytest.fixture(scope="session")
def spark():
    pyspark_sql = pytest.importorskip("pyspark.sql")  # needs pyspark + a local JDK
    session = (
        pyspark_sql.SparkSession.builder.master("local[1]")
        .appName("clean-taxi-trips-tests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    yield session
    session.stop()
