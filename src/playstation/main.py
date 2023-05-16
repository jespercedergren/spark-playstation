from pyspark.sql import SparkSession
from pipeline import Pipeline
from config import Config


if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .config("spark.master", "local")\
        .appName("playstation")\
        .getOrCreate()

    etl = Pipeline(spark, Config())
    etl.run()
