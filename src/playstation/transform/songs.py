import pyspark.sql.functions as F
from pyspark.sql import DataFrame


def get_user_distinct_songs(df: DataFrame) -> DataFrame:
    return df \
        .groupBy("userid") \
        .agg(F.countDistinct(F.col("artist_name"), F.col("track_name")).alias("distinct_song_count")) \
        .select("userid", "distinct_song_count")


def get_most_popular_songs(df: DataFrame, top_size: int) -> DataFrame:
    return df \
        .groupBy(F.col("artist_name"), F.col("track_name")) \
        .count() \
        .orderBy(F.desc(F.col("count")))\
        .limit(top_size)
