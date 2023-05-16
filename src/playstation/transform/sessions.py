import pyspark.sql.functions as F
from pyspark.sql import Window
from pyspark.sql import DataFrame


def get_sessions(df: DataFrame) -> DataFrame:

    window_user = Window \
        .partitionBy("userid") \
        .orderBy(F.col("timestamp").asc())

    window_cumulative = window_user.rangeBetween(
            Window.unboundedPreceding, 0
        )  # .rangeBetween(Window.unboundedPreceding, 0 : all previous values up to itself

    return df \
        .withColumn("ts_lag", F.lag(F.col("timestamp"), 1).over(window_user)) \
        .withColumn("ts_diff", F.col("timestamp") - F.col("ts_lag")) \
        .withColumn("session_start_indicator", F.coalesce((F.col("ts_diff") > F.expr("INTERVAL 20 MINUTES")).cast("int"), F.lit(0))) \
        .withColumn("session_indicator", F.sum(F.col("session_start_indicator")).over(window_cumulative)) \
        .select("userid", "timestamp", "artist_name", "track_name", "session_indicator")


def get_top_user_sessions(df: DataFrame, top_size: int) -> DataFrame:
    df_user_sessions = df \
        .groupBy("userid", "session_indicator") \
        .agg( \
        F.min(F.col("timestamp")).alias("session_start_timestamp"), \
        F.max(F.col("timestamp")).alias("session_end_timestamp"), \
        F.sort_array( \
            F.collect_list(F.struct(F.col("timestamp"), F.col("track_name"))), \
            asc=True \
            ).alias("sorted_ts_songs") \
        )

    return df_user_sessions.\
        withColumn("session_duration",  \
                       F.col("session_end_timestamp") - F.col("session_start_timestamp"),  \
              ) \
        .orderBy(F.col("session_duration").desc()) \
        .select( \
            F.col("userid"), \
            F.col("session_start_timestamp"),  \
            F.col("session_end_timestamp"),  \
            F.col("sorted_ts_songs.track_name").alias("sorted_songs") \
        )\
        .limit(top_size)
