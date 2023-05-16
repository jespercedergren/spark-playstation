from pyspark.sql import DataFrame, SparkSession

from config import Config
from transform.songs import get_user_distinct_songs, get_most_popular_songs
from transform.sessions import get_sessions, get_top_user_sessions


class Pipeline:
    def __init__(self, spark: SparkSession, config: Config):
        self.spark = spark
        self.config = config

    @staticmethod
    def read(spark: SparkSession, input_path: str, input_schema: str) -> DataFrame:
        return spark.read\
            .option("delimiter", "\t")\
            .schema(input_schema)\
            .csv(input_path)\
            .select("userid", "timestamp", "artist_name", "track_name")

    @staticmethod
    def write(df: DataFrame, output_path: str):
        df.write\
            .format("parquet")\
            .mode("overwrite")\
            .save(output_path)

    def run(self):

        df = self.read(self.spark, self.config.input_path, self.config.input_schema)

        user_distinct_songs_df = get_user_distinct_songs(df)
        user_distinct_songs_df.printSchema()
        self.write(user_distinct_songs_df, self.config.output_path_distinct_songs)

        popular_songs_df = get_most_popular_songs(df, 100)
        popular_songs_df.printSchema()
        self.write(popular_songs_df, self.config.output_path_popular_songs)

        sessions_df = get_sessions(df)
        user_sessions_df = get_top_user_sessions(sessions_df, 10)
        user_sessions_df.printSchema()
        self.write(user_sessions_df, self.config.output_path_user_sessions)
