import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.types import StructType, StructField, DoubleType, StringType, IntegerType, DateType, TimestampType


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    '''
    Parameters none
    Description - this function creates a connection to the AWS spark cluster
    Returns spark - a spark sessions
    '''
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    '''
    Parameters: spark - the spark context
                input_data - path to the input data directory
                output_data - path to the output data directory
    Description: This function reads the song data into the Spark AWS cluster
    Returns: None
    '''
    # get filepath to song data file
    song_data = os.path.join(input_data,'song_data/*/*/*/*.json')
    
    # read song data file
    songSchema = StructType([
        StructField("artist_id",StringType()),
        StructField("artist_latitude",DoubleType()),
        StructField("artist_location",StringType()),
        StructField("artist_longitude",DoubleType()),
        StructField("artist_name",StringType()),
        StructField("duration",DoubleType()),
        StructField("num_songs",IntegerType()),
        StructField("title",StringType()),
        StructField("year",IntegerType()),
    ])

    df = spark.read.json(song_data, schema=songSchema)

    # extract columns to create songs table
    songs_table = df.select(["title", "artist_id","year", "duration"]).dropDuplicates().withColumn("song_id", monotonically_increasing_id())
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year", "artist_id").parquet(os.path.join(output_data,'songs/'),mode = 'overwrite')

    # extract columns to create artists table
    artists_table = df.selectExpr(["artist_id", 
                                   "artist_name as name", 
                                   "artist_location as location", 
                                   "artist_latitude as latitude", 
                                   "artist_longitude as longitude"]).dropDuplicates()
    
    # write artists table to parquet files
    artists_table = df.selectExpr(artists_fields).dropDuplicates()
    artists_table.write.parquet(os.path.join(output_data,'artists/'),mode = 'overwrite')
    



def process_log_data(spark, input_data, output_data):
    '''
    Parameters: spark - the spark context
                input_data - path to the input data directory
                output_data - path to the output data directory
    Description: This function reads the log data data into the Spark AWS cluster,
                 joins it with the song data, and outputs it to parquet files
    Returns: None
    '''
    # get filepath to log data file
    log_data = os.path.join(input_data, 'log_data/*/*/*.json')

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')

    # extract columns for users table    
    users_table = df.selectExpr(["userdId as user_id", 
                             "firstName as first_name", 
                             "lastName as last_name", 
                             "gender", 
                             "level"]).dropDuplicates()
    
    # write users table to parquet files
    users_table.write.parquet(os.path.join(output_data,'users/'),mode='overwrite')

    # create timestamp column from original timestamp column
    df = df.withColumn("start_time",F.to_timestamp(df["ts"] / 1000)) 
    
    # extract columns to create time table
    time_table = df.select("start_time").dropDuplicates() \
        .withColumn("hour", hour(col("start_time"))).withColumn("day", dayofmonth(col("start_time"))) \
        .withColumn("week", weekofyear(col("start_time"))).withColumn("month", month(col("start_time"))) \
        .withColumn("year", year(col("start_time"))).withColumn("weekday", date_format(col("start_time"), 'E'))
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year", "month").parquet(os.path.join(output_data,'time/'))

    # read in song data to use for songplays table
    df_songs = spark.read.parquet(os.path.join(output_data,'songs/*/*/*'))

    df_artists = spark.read.parquet(os.path.join(output_data,'artists/*'))
    
    songs_logs = df.join(df_songs, (df.song == df_songs.title))
    artists_songs_logs = songs_logs.join(df_artists, (songs_logs.artist == df_artists.name))

    artists_songs_logs = songs_logs.withColumn("ts",F.to_timestamp(songs_logs["ts"] / 1000))


    # extract columns from joined song and log datasets to create songplays table 
    songplays = artists_songs_logs.join(
        time_table,
        artists_songs_logs.ts == time_table.start_time, 'left'
    )#.drop(artists_songs_logs.year)

    songplays_table = songplays.select(
        col('start_time').alias('start_time'),
        col('userId').alias('user_id'),
        col('level').alias('level'),
        col('song_id').alias('song_id'),
        col('artist_id').alias('artist_id'),
        col('sessionId').alias('session_id'),
        col('location').alias('location'),
        col('userAgent').alias('user_agent'),
        col('year').alias('year'),
        col('month').alias('month'),
    ).repartition("year", "month")

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("year", "month").parquet(os.path.join(output_data,'songplays/'))

def main():
    '''
    Parameters: None
    Description: The main function run when the script is executed
    Returns: None
    '''
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://udacity-tfento1/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
