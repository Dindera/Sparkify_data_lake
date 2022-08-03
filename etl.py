import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import TimestampType, DateType
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, dayofweek

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = os.path.join(input_data, "song_data/A/A/*/*.json")

    # read song data to file
    df = spark.read.json(song_data, mode='PERMISSIVE', columnNameOfCorruptRecord='corrupt_record')

    # extract columns to create songs table
    songs_table = df.select("song_id", "title", "artist_id", "year", "duration").distinct()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.parquet(output_data+'songs_table', mode="overwrite" , partitionBy=['year', 'artist_id'])

    # extract columns to create artists table
    artists_table = df.select([
       col("artist_id"),
       col("artist_name").alias("name"), 
       col("artist_location").alias("location"),
       col("artist_latitude").alias("latitude"), 
       col("artist_longitude").alias("longitude")
    ]).distinct()

    
    # write artists table to parquet files
    artists_table.write.parquet(output_data+'artists_table', mode="overwrite")

    return song_data

def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = os.path.join(input_data, "log_data/*/*")

    # read log data file
    df = spark.read.json(log_data, mode='PERMISSIVE', columnNameOfCorruptRecord='corrupt_record')
    
    # filter by actions for song plays
    df = df.where(df.page=="NextSong")

    # extract columns for users table    
    users_table = df.select([
      col("userId").alias("user_id"),
      col("firstName").alias("first_name"),
      col("lastName").alias("last_name"),
      col("gender"),
      col("level")
    ]).distinct()
    
    # write users table to parquet files
    users_table.write.parquet(output_data+'users_table', mode="overwrite")

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: x, TimestampType())

    df = df.withColumn("timestamp", get_timestamp("ts"))
    
    # create datetime column from original timestamp column
    get_datetime =  udf(lambda x: x, DateType())

    df = df.withColumn("start_time", get_datetime("timestamp"))
    
    # extract columns to create time table
    time_table = df.select("start_time").dropDuplicates()\
    .withColumn("hour", hour(col("start_time"))).withColumn("day", dayofmonth(col("start_time")))\
    .withColumn("week", weekofyear(col("start_time"))).withColumn("month", month(col("start_time")))\
    .withColumn("year", year(col("start_time"))).withColumn("weekday", dayofweek(col("start_time")))
    
    # write time table to parquet files partitioned by year and month
    time_table.write.parquet(output_data+'time_table', mode="overwrite")

    # read in song data to use for songplays table
    song_df = process_song_data(spark, input_data, output_data)

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df.join(song_df, df.artist==song_df.artist_name, 'inner')\
                  .withColumn("songplay_id", monotonically_increasing_id())\
                  .select([col("start_time"), 
                          col("userId").alias("user_id"), 
                          col("level"), 
                          col("song_id"),
                          col("artist_id"),
                          col("sessionId").alias("session_id"),
                          col("location"),
                          col("userAgent").alias("user_agent")])

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.parquet(output_data+'songplays_table', mode="overwrite" , partitionBy=['year', 'month'])



def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://sparkify-emr-output/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
