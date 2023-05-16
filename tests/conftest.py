import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark_session():
    spark_session = SparkSession.builder.config("spark.master", "local").appName("test").getOrCreate()
    return spark_session
