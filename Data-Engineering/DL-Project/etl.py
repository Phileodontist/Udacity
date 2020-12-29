import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import MapType, StringType, IntegerType, TimestampType
from pyspark.sql.functions import year, month, dayofmonth, dayofweek, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    Creates a spark connection
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark

def process_song_data(spark, input_data, output_data):
    """
    Reads in song data from S3, processes and transforms data into 
    the songs and artists table, storing them within another S3
    """
    # get filepath to song data file
    song_data = input_data + 'song_data/*/*/*/*.json' 
    
    print("--------------------------------------------------------")
    print("Started Processing Song Data")
    print("--------------------------------------------------------")
    
    # read song data file
    dfSong = spark.read.json(song_data)
    
    # Create temp view for songs
    dfSong.createOrReplaceTempView("songs")

    # extract columns to create songs table
    songs_table = spark.sql("SELECT song_id, \
                                title, \
                                artist_id, \
                                year, \
                                duration \
                         FROM songs WHERE song_id IS NOT NULL")
    
    # write songs table to parquet files partitioned by year and artist
    print("--------------------------------------------------------")
    print("Writing songs table to S3")
    print("--------------------------------------------------------")
    songs_table.write.mode('overwrite').partitionBy('year', 'artist_id').parquet(output_data + 'songs_table/')

    # extract columns to create artists table
    artists_table = spark.sql("SELECT artist_id, \
                                artist_name, \
                                artist_location, \
                                artist_latitude, \
                                artist_longitude \
                         FROM songs WHERE song_id IS NOT NULL")
    
    # write artists table to parquet files
    print("--------------------------------------------------------")
    print("Writing artists table to S3")
    print("--------------------------------------------------------")
    artists_table.write.mode('overwrite').parquet(output_data + 'artists_table/')

def process_log_data(spark, input_data, output_data):
    """
    Reads in log data from S3, processes and transforms data into 
    the time and songPlays table, storing them within another S3
    """
    # get filepath to log data file
    log_data = input_data + 'log_data/*/*/*.json'

    # read log data file
    dfLog = spark.read.json(log_data)
    
    # filter by actions for song plays
    dfLog = dfLog.filter(dfLog.page == 'NextSong')
    
    # Create temp view for logs
    dfLog.createOrReplaceTempView("logs")

    # extract columns for users table    
    users_table = spark.sql("SELECT userId AS user_id, \
                                firstName AS first_name, \
                                lastName AS last_name, \
                                gender, \
                                level \
                         FROM logs WHERE userId IS NOT NULL")
    
    # write users table to parquet files
    print("--------------------------------------------------------")
    print("Writing users table to S3")
    print("--------------------------------------------------------")
    users_table.write.mode('overwrite').parquet(output_data + 'users_table/')
    
    # create timestamp column from original timestamp column
    spark.udf.register("get_timestamp", lambda x : datetime.fromtimestamp(x/1000.0), TimestampType())

    # Temp table with logs and time data
    dfTSParsed = spark.sql("SELECT *, \
                               get_timestamp(ts) AS start_time, \
                               hour(get_timestamp(ts)) AS hour, \
                               dayofmonth(get_timestamp(ts)) AS day, \
                               weekofyear(get_timestamp(ts)) AS week, \
                               month(get_timestamp(ts)) AS month, \
                               year(get_timestamp(ts)) AS year, \
                               dayofweek(get_timestamp(ts)) AS weekday \
                            FROM logs WHERE ts IS NOT NULL")
    
    # Combination of the logs table + time table
    dfTSParsed.createOrReplaceTempView("logs_and_time")
    
    # extract columns to create time table
    time_table = spark.sql("SELECT start_time, \
                            hour, \
                            day, \
                            week, \
                            month, \
                            year, \
                            weekday \
                        FROM logs_and_time WHERE ts IS NOT NULL")
    
    # write time table to parquet files partitioned by year and month
    print("--------------------------------------------------------")
    print("Writing time table to S3")
    print("--------------------------------------------------------")
    time_table.write.mode('overwrite').partitionBy('year', 'month').parquet(output_data + 'time_table/')
    
    # Retrieve song data
    song_data = input_data + 'song_data/*/*/*/*.json'
    
    # Read song data file
    dfSong = spark.read.json(song_data)
    
    # Create temp view for songs
    dfSong.createOrReplaceTempView("songs")

    # extract columns from joined song and log datasets to create songplays table 
    songPlays_table = spark.sql("""
                            SELECT 
                                monotonically_increasing_id() AS songplay_id,
                                lt.start_time,
                                lt.month,
                                lt.year,
                                lt.userId AS user_id,
                                lt.level,
                                songs.song_id,
                                songs.artist_id,
                                lt.sessionId AS session_id,
                                lt.location,
                                lt.userAgent AS user_agent
                            FROM logs_and_time as lt
                            JOIN songs ON
                                lt.artist = songs.artist_name AND
                                lt.song = songs.title
                            """)

    # write songplays table to parquet files partitioned by year and month
    print("--------------------------------------------------------")
    print("Writing songPlays table to S3")
    print("--------------------------------------------------------")
    songPlays_table.write.mode('overwrite').partitionBy('year', 'month').parquet(output_data + 'songPlays_table/')


def main():
    spark = create_spark_session()
    
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://lp-dl-project/data/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
