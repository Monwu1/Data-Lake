import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
import pyspark.sql.types as TS
from pyspark.sql.functions import udf, col, to_timestamp, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    creates spark session
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    loads song data from S3 and transfer it into dimensional tables with spark, and then load it back s3 
    spark       : created spark Session
    input_data  : location of song_data s3 bucket
    output_data : location of the S3 bucket where the dimensional tables will be stored
            
    """
    # get filepath to song data file
    song_data = input_data + "song_data/A/*/*/*.json"
    #song_data = input_data + "song_data/A/A/A/*.json" 
    
    
    # read song data file
    df = spark.read.json(song_data)
    
 
    # extract columns to create songs table
    df.createOrReplaceTempView("songs")
    songs_table = spark.sql("""SELECT DISTINCT s.song_id,
                                            s.title,
                                            s.artist_id,
                                            s.year,
                                            s.duration
                                            FROM songs s
                                            WHERE s.song_id IS NOT NULL
                            """)
    
    songs_table = songs_table.dropDuplicates(['song_id'])
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode('overwrite').partitionBy("year", "artist_id").parquet(output_data+'song_table/')
   
    
    # extract columns to create artists table
    artists_table = spark.sql("""SELECT DISTINCT s.artist_id,
                                        s.artist_name,
                                        s.artist_location,
                                        s.artist_latitude,
                                        s.artist_longitude
                                        FROM songs s
                                        WHERE s.artist_id IS NOT NULL
                              """)
    
    artists_table = artists_table.dropDuplicates(['artist_id'])
    
    # write artists table to parquet files
    artists_table.write.mode('overwrite').parquet(output_data+'artist_table/')


def process_log_data(spark, input_data, output_data):
    """
    loads log data from S3 and transfer it into dimensional tables with spark, and then load it back s3 
    spark       : created spark Session
    input_data  : location of log_data s3 bucket
    output_data : location of the S3 bucket where the dimensional tables will be stored
            
    """
    # get filepath to log data file
    log_data = input_data + "log_data/*/*/*.json"

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')
    
    # extract columns for users table
    df.createOrReplaceTempView("logs")
    users_table = spark.sql("""SELECT DISTINCT lo.userId as user_id,
                                                 lo.firstName as first_name,
                                                 lo.lastName as last_name,
                                                 lo.gender,
                                                 lo.level
                                FROM logs lo
                                WHERE lo.userId IS NOT NULL
                            """)
    
    users_table = users_table.dropDuplicates(['user_id'])
    
    # write users table to parquet files
    users_table.write.mode('overwrite').parquet(output_data+'user_table/')
    
    
    # create start_time column from original timestamp column  
    df = df.withColumn('start_time', to_timestamp(df.ts / 1000))
    
    
    # create view as logs_table_view
    df.createOrReplaceTempView("logs_table_view")
    
    
    # extract columns from logs_table_view to create time table
    time_table = spark.sql("""SELECT DISTINCT lt.start_time as start_time,
                            hour(lt.start_time) as hour,
                            dayofmonth(lt.start_time) as day,
                            weekofyear(lt.start_time) as week,
                            month(lt.start_time) as month,
                            year(lt.start_time) as year,
                            dayofweek(lt.start_time) as weekday
                            FROM logs_table_view lt
                            WHERE lt.start_time IS NOT NULL           
                        """)
    
    time_table = time_table.dropDuplicates(['start_time'])
    
    
    # write time table to parquet files partitioned by year and month
    time_table.write.mode('overwrite').partitionBy("year", "month").parquet(output_data+'time_table/')
    

    # read in song data and artist data to use for songplays table
    song_df = spark.read.parquet(output_data + "/song_table/")
    artists_df = spark.read.parquet(output_data + "/artist_table/")
    
    # extract columns from joined song_table and artist_table to create songs_info_table 
    song_df.createOrReplaceTempView("song_table_view")
    artists_df.createOrReplaceTempView("artists_table_view")
    
    
    songs_info_table = spark.sql("""SELECT s.song_id, 
                                      s.title, 
                                      s.artist_id, 
                                      at.artist_name
                              FROM song_table_view s
                              JOIN artists_table_view at
                              ON s.artist_id = at.artist_id
                            """)
    
    songs_info_table.createOrReplaceTempView("song_artist_table_view")
 
    
    # extract columns from joined song_artist_table_view table and logs_table_view table to create songplays table 
    songplays_table = spark.sql("""SELECT monotonically_increasing_id() as songplay_id,
                                          logs_table_view.start_time as start_time,
                                          month(logs_table_view.start_time) as month,
                                          year(logs_table_view.start_time) as year,
                                          logs_table_view.userId as user_id,
                                          logs_table_view.level,
                                          song_artist_table_view.song_id,
                                          song_artist_table_view.artist_id,
                                          logs_table_view.sessionId as session_id,
                                          logs_table_view.location,
                                          logs_table_view.userAgent as user_agent
                                    FROM logs_table_view 
                                    FULL JOIN song_artist_table_view 
                                    ON logs_table_view.song = song_artist_table_view.title 
                                    AND logs_table_view.artist = song_artist_table_view.artist_name
                            """)
  
    songplays_table.write.mode('overwrite').partitionBy("year", "month").parquet(output_data+'songplays_table/')
    

    
def main():
    """
    create spark session
    call function process_song_data and process_log_dagta to extract data from s3, and transer it into dimensional tables, and load it back to s3
    """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://music-data-project3/"
   
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)
    

if __name__ == "__main__":
    main()
