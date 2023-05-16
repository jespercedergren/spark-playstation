from pyspark.sql.types import StructType, StructField, StringType, TimestampType


class Config:
    input_path = "/tests/resources/data/lastfm/userid-timestamp-artid-artname-traid-traname.tsv"
    output_path_distinct_songs = "/tests/resources/data/output/distinct_songs"
    output_path_popular_songs = "/tests/resources/data/output/most_popular_songs"
    output_path_user_sessions = "/tests/resources/data/output/top_user_sessions"

    input_schema = StructType(
        [
            StructField("userid", StringType()),
            StructField("timestamp", TimestampType()),
            StructField("musicbrainz_artist_id", StringType()),
            StructField("artist_name", StringType()),
            StructField("musicbrainz_track_id", StringType()),
            StructField("track_name", StringType())
        ]
    )
