# import helper
import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.functions import unix_timestamp, from_unixtime
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.types import *

# initial the configparser and read the dl.cfg file
config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['aws']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['aws']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    '''
    get or create a spark session and load jars and the hodoop aws package
    
    arg {
    : non
    }
    
    return {
    : spark session
    }
    '''
        spark = SparkSession \
                .builder \
                .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
                .getOrCreate()
        return spark


def process_song_data(spark, input_data, output_data):
    '''
    Read the Json song_file from the S3 bucket extract the data
    for the analytics tables and write it back to the S3 as a parquet files.
    
    args {
    :spark = sparksession
    :input_date = input S3 json file path
    :output_data = output S3 bucket file path
    }
    return {
    : parquet files
    }
    '''
    # get filepath to song data file
    song_data = os.path.join(input_data, "song_data/A/A/A/*.json")

    # read song data file
    data_schema = [ StructField('num_songs', StringType(), True),\
                    StructField('artist_id', StringType(), True),\
                    StructField('artist_name', StringType(), True),\
                    StructField('artist_latitude', DoubleType(), True),\
                    StructField('artist_longitude', DoubleType(), True),\
                    StructField('artist_location', StringType(), True),\
                    StructField('song_id', StringType(), True),\
                    StructField('title', StringType(), True),\
                    StructField('duration', DecimalType(),True),\
                    StructField('year', IntegerType(), True)]

    song_schema = StructType(fields=data_schema)
    df = spark.read.json(song_data, schema=song_schema)
    
    # extract columns to create songs table
    songs_table = df.select(["song_id", "title", "artist_id", "year", "duration"])

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year", "artist_id").format('parquet').save(output_data+"songs.parquet", mode = 'overwrite')

    # extract columns to create artists table
    artists_table = df.select(["artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude"])\
                    .withColumnRenamed('artist_name', 'name')\
                    .withColumnRenamed('artist_location', 'location')\
                    .withColumnRenamed('artist_latitude', 'latitude')\
                    .withColumnRenamed('artist_longitude', 'longitude')

    # write artists table to parquet files
    artists_table.write.format('parquet').save(output_data + "artist.parquet", mode = 'overwrite')


def process_log_data(spark, input_data, output_data):
    '''
    Read both Json file from the S3 bucket extract the data
    for the analytics tables and write it back to the S3 bucket as a parquet files.
    
    args {
    :spark = sparksession
    :input_date = input S3 json file path
    :output_data = output S3 bucket file path
    }
    return {
    : parquet files
    }
    '''
    # get filepath to log data file
    log_data = os.path.join(input_data, 'log_data/*/*/*.json')

    # read log data file with a new schema
    data_log_schema = [StructField('artist', StringType(), True),\
                     StructField('auth', StringType(), True),\
                     StructField('firstName', StringType(), True),\
                     StructField('gender', StringType(), True),\
                     StructField('itemInSession', IntegerType(), True),\
                     StructField('lastName', StringType(), True),\
                     StructField('lenght', DecimalType(), True),\
                     StructField('level', StringType(), True),\
                     StructField('location', StringType(), True),\
                     StructField('method', StringType(), True),\
                     StructField('page', StringType(), True),\
                     StructField('registration', StringType(), True),\
                     StructField('sessionId', IntegerType(), True),\
                     StructField('song', StringType(), True),\
                     StructField('status', IntegerType(), True),\
                     StructField('ts', LongType(), True),\
                     StructField('userAgent', StringType(), True),\
                     StructField('userId', IntegerType(), True)]

    log_schema = StructType(fields=data_log_schema)

    df = spark.read.json(log_data, schema=log_schema)

    # filter by actions for song plays
    df = df.filter("page = 'NextSong'")

    # extract columns for users table and rename some of them
    users_table = df.select(["userId", "firstName", "lastName", "gender", "level"])\
        .withColumnRenamed("userId", "user_id")\
        .withColumnRenamed("firstName", "first_name")\
        .withColumnRenamed("lastName", "last_name")

    # write users table to parquet files
    users_table.write.format('parquet').save(output_data + "users.parquet", mode = 'overwrite')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: x/1000, IntegerType())
    df = df.withColumn('start_time', get_timestamp(df.ts))

    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: from_unixtime(x), TimestampType())
    df = df.withColumn('datetime', from_unixtime('start_time'))

    # extract columns to create time table
    time_table = df.select("start_time",
                hour('datetime').alias("hour"),
                dayofmonth("datetime").alias("day"),
                weekofyear("datetime").alias("week"),
                month("datetime").alias("month"),
                year("datetime").alias("year"),
                date_format("datetime", "EEEE").alias("weekday")
                )

    # write time table to parquet files partitioned by year and month
    time_table= time_table.write.partitionBy("year", "month").format('parquet').save(output_data+"time.parquet", mode = 'overwrite')

    # read in song data to use for songplays table
    song_df = spark.read.json('s3a://udacity-dend/song_data/A/A/A/*.json')

    # extract columns from joined song and log datasets to create songplays table
    joins = [df['artist'] == song_df['artist_name'], df['song'] == song_df['title'], df['lenght'] == song_df['duration']]
    songplays_table = df.join(song_df, on=joins).select(
                monotonically_increasing_id().alias('songplay_id'),
                df.start_time,
                df.userId,
                df.level,
                song_df.song_id,
                song_df.artist_id,
                df.sessionId,
                df.location,
                df.userAgent,
                month('datetime').alias("month"),
                year("datetime").alias("year")
    ).withColumnRenamed('userId', 'user_id')\
     .withColumnRenamed('sessionId', 'session_id')\
     .withColumnRenamed('userAgent', 'user_agent')

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("year", "month").format('parquet').save(output_data + "songplays.parquet", mode = 'overwrite')


def main():
    '''
    call the spark session and the process_data functions
    contain the input- and output filepath
    '''
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://"

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
