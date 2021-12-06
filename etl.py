import configparser
from datetime import datetime
import os
from pyspark.sql import * #library imported
from pyspark.sql.types import * #library imported
import pyspark.sql.functions as F #library imported
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['KEYS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['KEYS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    This function extracts song data from S3, processes them to create 
    song, and artist dimensional tables. These dimensional tables
    are then written into parquet format ready for loading into S3.
    """
    # obtains filepath to song data file
    song_data = input_data + "song_data/A/A/A/*.json" #use "song_data/*/*/*/*.json" for final version
    
    # reads song data file
    df = spark.read.json(song_data)

    # extracts columns to create songs table
    songs_table = df.select("song_id", 
                            "artist_id", 
                            "title", 
                            "duration", 
                            "year")
    
    # writes songs table to parquet files partitioned by year and artist
    songs_table = songs_table.write.mode("overwrite")\
                                   .parquet(os.path.join(output_data, "songs"),\
                                            partitionBy=["year", "artist_id"])

    # extracts columns to create artists table
    artists_table = df.select("artist_id", 
                              "artist_name", 
                              "artist_latitude", 
                              "artist_longitude", 
                              "artist_location")
    
    # writes artists table to parquet files
    artists_table = artists_table.write.mode("overwrite")\
                                 .parquet(os.path.join(output_data, "artists"))


def process_log_data(spark, input_data, output_data):
    """
    This function extracts user log data from S3, processes them to create 
    users, and time dimensional tables.
    This function also processes both log and song data to create 
    songplays fact table.
    These tables are then written into parquet format ready for loading into S3.
    """
    # obtains filepath to log data file
    log_data = input_data + "log_data/*/*/*.json"

    # reads log data file
    df = spark.read.json(log_data)
    df = df.withColumn("user_id", df.userId.cast(IntegerType()))
    
    # filters by actions for song plays
    df = df.where(df.page == "Nextsong")

    # extracts columns for users table
    users_table = df.selectExpr(["user_id", \
                                "firstName as first_name", \
                                "lastName as last_name", \
                                "gender", "level", \
                                "ts"])
    
    users_window = Window.partitionBy("user_id").orderBy(F.desc("ts"))
    users_table = users_table.withColumn("row_number", F.row_number().over(users_window))
    users_table = users_table.where(users_table.row_number == 1).drop("ts", "row_number")
    
    users_table = users_table.drop_duplicates(subset=["user_id"])
    
    # writes users table to parquet files
    users_table = users_table.write.mode("overwrite")\
                                   .parquet(os.path.join(output_data, "users"))

    # creates timestamp column from original timestamp column
    get_timestamp = F.udf(lambda ts: datetime.fromtimestamp(ts/1000).isoformat())
    df = df.withColumn("start_time", get_timestamp("ts").cast(TimestampType()))
     
    # extract columns to create time table
    time_table = df.select("start_time")
    time_table = time_table.withColumn("hour", F.hour("start_time"))
    time_table = time_table.withColumn("day", F.dayofmonth("start_time"))
    time_table = time_table.withColumn("week", F.weekofyear("start_time"))
    time_table = time_table.withColumn("month", F.month("start_time"))
    time_table = time_table.withColumn("year", F.year("start_time"))
    time_table = time_table.withColumn("weekday", F.dayofweek("start_time"))
    
    # writes time table to parquet files partitioned by year and month
    time_table = time_table.write.mode("overwrite")\
                                 .parquet(os.path.join(output_data, "time"),\
                                          partitionBy=["year", "month"])

    # reads song data to use for songs table
    song_df = spark.read.json(os.path.join(input_data, "song_data", "A", "A", "A"))

    # extract columns from joined song and log datasets to create songplays table
    df = df.orderBy("ts")
    df = df.withColumn("songplay_id", F.monotonically_increasing_id())
    song_df.createOrReplaceTempView("songs")
    df.createOrReplaceTempView("events")

    songplays_table = spark.sql("""
        SELECT
            e.songplay_id,
            e.start_time,
            e.user_id,
            e.level,
            s.song_id,
            s.artist_id,
            e.sessionId AS session_id,
            e.location,
            e.userAgent AS user_agent,
            year(e.start_time) AS year,
            month(e.start_time) AS month
        FROM events AS e
        LEFT JOIN songs AS s ON
            e.song = s.title AND
            e.artist = s.artist_name AND
            ABS(e.length - s.duration) < 2
    """)

    # writes songplays table to parquet files partitioned by year and month
    songplays_table = songplays_table.write.mode("overwrite").parquet(os.path.join(output_data, "songplays"), partitionBy=["year", "month"])


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "./Results/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)
    spark.stop()
    print("successful! 😊")

if __name__ == "__main__":
    main()
