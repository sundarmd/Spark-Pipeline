import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, dayofweek, hour, weekofyear, date_format
from pyspark.sql.types import *


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config['DEFAULT']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['DEFAULT']['AWS_SECRET_ACCESS_KEY']
#os.environ['AWS_SESSION_TOKEN'] = config['DEFAULT']['AWS_SESSION_TOKEN']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    
    """
    Function :
    Process the songs data files and create extract songs table and artist table data from it.
    
    """ 
    # get filepath to song data file
    #song_data = input_data + "song_data/"
    song_data = os.path.join(input_data, 'song_data/*/*/*/*.json')
    
    # read song data file
    df = spark.read.json(song_data).drop_duplicates()

    # extract columns to create songs table
    song_cols = ["song_id", "title", "artist_id", "year", "duration"]
    songs_table = df.select(song_cols).dropDuplicates()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.parquet(output_data + "songs/", mode="overwrite", partitionBy=["year","artist_id"])

    # extract columns to create artists table
    artists_cols = ["artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude"]
    artists_table = df.select(artists_cols).dropDuplicates()
    
    # write artists table to parquet files
    artists_table.write.parquet(output_data + "artists/", mode="overwrite")


def process_log_data(spark, input_data, output_data):
    
    """
    Function:
    Process the event log file and extract data for table time, users and songplays from it.
    
    """
    # get filepath to log data file
    log_data = os.path.join(input_data, "log_data/")

    # read log data file
    df = spark.read.json(log_data).drop_duplicates()
    
    # filter by actions for song plays
    df = df.filter(df.page == "NextSong")

    # extract columns for users table
    users_cols = ["userId", "firstName", "lastName", "gender", "level"]
    users_table = df.select(users_cols).dropDuplicates()
    
    # write users table to parquet files
    users_table.write.parquet(os.path.join(output_data, "users/") , mode="overwrite")

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: x / 1000, TimestampType())
    df = df.withColumn("timestamp", get_timestamp(df.ts))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: datetime.fromtimestamp(x), TimestampType())
    df = df.withColumn("start_time", get_datetime(df.timestamp))
    
    
    # extract columns to create time table
    df = df.withColumn("hour", hour("start_time")) \
            .withColumn("day", dayofmonth("start_time")) \
            .withColumn("week", weekofyear("start_time")) \
            .withColumn("month", month("start_time")) \
            .withColumn("year", year("start_time")) \
            .withColumn("weekday", dayofweek("start_time"))
    
    time_table = df.select("start_time", "hour", "day", "week", "month", "year", "weekday")
    
    # write time table to parquet files partitioned by year and month
    time_table.write.parquet(os.path.join(output_data, "time_table/"), mode='overwrite', partitionBy=["year","month"])

    # read in song data to use for songplays table
    songs_df = spark.read.parquet(os.path.join(output_data, "songs/*/*/*"))
    
    # extract columns from joined song and log datasets to create songplays table 
    songs_logs = df.join(songs_df, (df.song == songs_df.title))
    
    # get artist logs too
    artists_df = spark.read.parquet(os.path.join(output_data, "artists/"))
    
    # combine both song and artist logs
    artists_songs_logs = songs_logs.join(artists_df, (songs_logs.artist == artists_df.artist_name))
    
    # join artist_song_logs with time_table for songplays table
    songplays = artists_songs_logs.join(time_table, artists_songs_logs.start_time == time_table.start_time, how="inner").drop(artists_songs_logs.year).drop(time_table.start_time).drop(time_table.month)

    songplays_table = songplays.select(
        col('start_time'),
        col('userId').alias('user_id'),
        col('level'),
        col('song_id'),
        col('artist_id'),
        col('sessionId').alias('session_id'),
        col('location'),
        col('userAgent').alias('user_agent'),
        col('year'),
        col('month')
        )
    
    # write songplays table to parquet files partitioned by year and month
    songplays_table.drop_duplicates().write.parquet(os.path.join(output_data, "songplays/"), mode="overwrite", partitionBy=["year","month"])


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://udacity-spark-test/"         #create your own bucket for outputs
    
    # for local testing
    
    #input_data = "data/"
    #output_data = "data/output_final_test/"

    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
