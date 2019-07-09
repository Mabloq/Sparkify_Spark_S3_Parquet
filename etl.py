import configparser
import os
from pyspark.sql import SparkSession
from utils.schemas import songSchema, logSchema
from utils.udfs import year_part_udf
from pyspark.sql.functions import col, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear,dayofweek, from_unixtime


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
        Creates a spark session

        Returns:
        object: spark session
    """
    sc = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()

    return sc


def process_song_data(sc, input_data):
    """
      Reads song data from s3 saves raw data to staging table in parquet format and saves process data to
      songs and artists tables

      Parameters:
      spark (object): spark session
      input_data (string): s3 path to read from

      """
    # read song data file
    songs = sc.read.json(input_data,schema=songSchema)

    y = col("year")
    fname = [(year_part_udf, "year_part")]
    exprs = [col("*")] + [f(y).alias(name) for f, name in fname]

    # extract columns and write to parquet staging_songs table
    staging_song_table = songs.select(*exprs)
    staging_song_table.write\
        .partitionBy('year_part','year', 'artist_id')\
        .format('parquet')\
        .mode('overwrite')\
        .save('local-data/parquet/staging_songs/')

    # extract columns to create songs table
    # write songs to parquet songs_tablex
    song_table = songs.select(*exprs)
    song_table.write.mode('overwrite')\
        .partitionBy('year_part', 'year', 'artist_id')\
        .format('parquet').mode('overwrite')\
        .save('local-data/parquet/songs/')

    # extract columns to create artists table
    artists_table = songs.selectExpr('artist_id', 'artist_name as name', 'artist_location as location', 'artist_latitude as latitude','artist_longitude as longitude')

    artists_table.dropDuplicates(['artist_id']).count()
    # write artists table to parquet files
    artists_table.write.mode('overwrite').parquet("local-data/parquet/artists/")


def process_log_data(spark, input_data):
    """
      Reads event data from s3 saves raw data to staging table in parquet format and saves process data to
      users, time and songplays tables

      Parameters:
      spark (object): spark session
      input_data (string): s3 path to read from

      """

    # read log data file
    df = spark.read.json(input_data, schema=logSchema)

    df.write.mode('overwrite').parquet("local-data/parquet/staging_events/")
    # filter by actions for song plays
    df = df.select("*").where(df.page == 'NextSong')

    # extract columns for users table
    users_table = df.selectExpr("userID as user_id", "firstName as first_name", "lastName as last_name", "gender", "level")

    # drop duplicate use
    users_table = users_table.dropDuplicates(["user_id"])
    # write users table to parquet files
    users_table.write.mode('overwrite').parquet('local-data/parquet/users/')

    time_table = df.select(from_unixtime(df.ts.cast('bigint') / 1000).alias('start_time'))
    # add hour, day, week, month, year, weekday
    time_table = time_table.withColumn('hour', hour('start_time'))
    time_table = time_table.withColumn('day', dayofmonth('start_time'))
    time_table = time_table.withColumn('week', weekofyear('start_time'))
    time_table = time_table.withColumn('month', month('start_time'))
    time_table = time_table.withColumn('year', year('start_time'))
    time_table = time_table.withColumn('weekday', dayofweek('start_time'))

    # write time table to parquet files partitioned by year and month
    time_table.write.mode('overwrite').partitionBy('year', 'month').parquet('local-data/parquet/time/')

    # read in song data to use for songplays table
    songs_df = spark.read.parquet('local-data/parquet/staging_songs/')
    # read in event data to use for songplays table
    events_df = spark.read.parquet('local-data/parquet/staging_events/')

    # extract columns from joined song and log datasets to create songplays table
    songplays = events_df.join(songs_df, (events_df.artist == songs_df.artist_name) & (events_df.song == songs_df.title))

    # Add monotonicall increasingId
    songplays = songplays.withColumn("songplay_id", monotonically_increasing_id())
    #create aliases
    songplays = songplays.select('songplay_id', from_unixtime(songplays.ts.cast('bigint') / 1000).alias('start_time'),
                                 songplays.userId.alias('user_id'), 'level', 'song_id', 'artist_id',
                                 songplays.sessionId.alias('session_id'),
                                 songplays.artist_location.alias('location'), songplays.userAgent.alias('user_agent'))

    dt = col("start_time")
    fname = [(year, "year"), (month, "month")]
    exprs = [col("*")] + [f(dt).alias(name) for f, name in fname]

    songplays.select(*exprs) \
        .write \
        .mode('overwrite') \
        .partitionBy(*(name for _, name in fname)) \
        .parquet('local-data/parquet/songplays/')


if __name__ == "__main__":
    spark = create_spark_session()
    process_song_data(spark, 'local-data/songs/*/*.json')
    process_log_data(spark, 'local-data/logs/*.json')
    spark.stop()
