package com.musicanalysis.exec
import java.io.File
import org.apache.hadoop.hive.serde2.`lazy`.LazySimpleSerDe
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkException

object MusicAnalysis {

  def main(args: Array[String]): Unit = {
    
    val warehouseLocation = new File("/user/hive/warehouse").getAbsolutePath
    
    val spark = SparkSession.builder()
      .appName("Data Analysis")
      .config("spark.sql.warehouse.dir",warehouseLocation)
      .config("spark.sql.shuffle.partitions","1")
      .enableHiveSupport()
      .getOrCreate()

    val batchId = args(0)

    
    //Determine top 10 station_id(s) where maximum number of songs were played, which were liked by unique users.

    try {
    val set_properties = spark.sql("set hive.auto.convert.join=false")

    val use_musicproject_database = spark.sql("USE musicproject")

    val create_hive_table_top_10_stations = spark.sql("""CREATE TABLE IF NOT EXISTS musicproject.top_10_stations
      (
       station_id STRING,
       total_songs_played_liked_by_unique_user INT
      )
       PARTITIONED BY (batchid INT)
       ROW FORMAT DELIMITED
       FIELDS TERMINATED BY ','
       STORED AS TEXTFILE""")


    val insert_into_top_10_stations = spark.sql(s"""INSERT OVERWRITE TABLE musicproject.top_10_stations
       PARTITION (batchid=$batchId)
       SELECT
       station_id,
       COUNT(DISTINCT user_id,song_id) AS total_songs_played_liked_by_unique_user
       FROM musicproject.enriched_data
       WHERE status='pass'
       AND (batchid=$batchId)
       AND like=1
       GROUP BY station_id
       ORDER BY total_songs_played_liked_by_unique_user DESC
       LIMIT 10""")

    
    /*Determine total duration of songs played by each type of user, where type of user can be 'subscribed' or 'unsubscribed'.
    An unsubscribed user is the one whose record is either not present in Subscribed_users lookup table or has subscription_end_date
    earlier than the timestamp of the song played by him.*/

    val create_hive_table_song_duration = spark.sql("""CREATE TABLE IF NOT EXISTS musicproject.user_song_duration
      (
       user_id STRING,
       user_type STRING,
       total_duration_in_minutes DOUBLE
      )
       PARTITIONED BY (batchid INT)
       ROW FORMAT DELIMITED
       FIELDS TERMINATED BY ','
       STORED AS TEXTFILE""")


    val insert_into_song_duration = spark.sql(s"""INSERT OVERWRITE TABLE musicproject.user_song_duration
       PARTITION (batchid=$batchId)
       SELECT
        e.user_id,
       IF(e.user_id!=s.user_id
       OR (CAST(s.subscn_end_dt as BIGINT) < CAST(e.start_ts as BIGINT)),'unsubscribed','subscribed') AS user_type,
       (cast(e.end_ts as BIGINT)-cast(e.start_ts as BIGINT))/60 AS total_duration_in_minutes
       FROM musicproject.enriched_data e
       LEFT OUTER JOIN musicproject.subscribed_users s
       ON e.user_id=s.user_id
       WHERE e.status='pass'
       AND (batchid=$batchId)""")
       
           val create_hive_user_type_duration = spark.sql("""CREATE TABLE IF NOT EXISTS musicproject.user_type_duration
      (
       user_type STRING,
       total_duration_played  DOUBLE
      )
       PARTITIONED BY (batchid INT)
       ROW FORMAT DELIMITED
       FIELDS TERMINATED BY ','
       STORED AS TEXTFILE""")
       
           val insert_into_user_type_duration = spark.sql(s"""INSERT OVERWRITE TABLE musicproject.user_type_duration
       PARTITION (batchid=$batchId)
       SELECT
       user_type,
       sum(total_duration_in_minutes) as total_duration_played
       FROM musicproject.user_song_duration
       WHERE (batchid=$batchId)
        GROUP BY user_type""")


    
    //Determine top 10 connected artists.
    //Connected artists are those whose songs are most listened by the unique users who follow them.

    val create_hive_table_top_10_connected_artists = spark.sql("""CREATE TABLE IF NOT EXISTS musicproject.connected_artists
      (
       artist_id STRING,
       unique_followers INT
      )
       PARTITIONED BY (batchid INT)
       ROW FORMAT DELIMITED
       FIELDS TERMINATED BY ','
       STORED AS TEXTFILE""")


    val insert_into_top_10_connected_artists = spark.sql(s"""INSERT OVERWRITE TABLE musicproject.connected_artists
       PARTITION (batchid=$batchId)
       SELECT
       f.artist_id,
       COUNT(DISTINCT f.user_id) AS unique_followers
       FROM musicproject.enriched_data f join (select user_id, artist_id["artist_1"] as artist_id from  musicproject.user_artist_map union all select user_id, artist_id["artist_2"] as artist_id  from musicproject.user_artist_map union all select user_id, artist_id["artist_3"] as artist_id  from musicproject.user_artist_map) i
       on i.user_id=f.user_id
       and (f.artist_id= i.artist_id )
       WHERE 
       status='pass'
       AND (batchid=$batchId)
       GROUP BY f.artist_id
       ORDER BY unique_followers desc
       LIMIT 10""")


    
    //Determine top 10 songs who have generated the maximum revenue.
    //NOTE: Royalty applies to a song only if it was liked or was completed successfully or both.

    val create_hive_table_top_10_songs_maxrevenue = spark.sql("""CREATE TABLE IF NOT EXISTS musicproject.top_10_songs_maxrevenue
      (
       song_id STRING,
       total_duration_played_in_minutes DOUBLE
       )
       PARTITIONED BY (batchid INT)
       ROW FORMAT DELIMITED
       FIELDS TERMINATED BY ','
       STORED AS TEXTFILE""")


    val insert_into_top_10_songs_maxrevenue = spark.sql(s"""INSERT OVERWRITE TABLE musicproject.top_10_songs_maxrevenue
       PARTITION (batchid=$batchId)
       SELECT
       song_id,
       SUM(cast(end_ts as BIGINT)-cast(start_ts as BIGINT))/60 AS total_duration_played_in_minutes
       FROM musicproject.enriched_data
       WHERE status='pass'
       AND (batchid=$batchId)
       AND (like=1 OR song_end_type=0)
       GROUP BY song_id
       ORDER BY total_duration_played_in_minutes desc
       LIMIT 10""")


    
    //Determine top 10 unsubscribed users who listened to the songs for the longest duration.

    val create_hive_table_top_10_unsubscribed_users = spark.sql("""CREATE TABLE IF NOT EXISTS musicproject.top_10_unsubscribed_users
      (
       user_id STRING,
       total_duration_played DOUBLE
      )
       PARTITIONED BY (batchid INT)
       ROW FORMAT DELIMITED
       FIELDS TERMINATED BY ','
       STORED AS TEXTFILE""")


    val insert_into_unsubscribed_users = spark.sql(s"""INSERT OVERWRITE TABLE musicproject.top_10_unsubscribed_users
       PARTITION (batchid=$batchId)
       SELECT
       user_id,
       SUM(total_duration_in_minutes) as total_duration_played
       FROM musicproject.user_song_duration
       WHERE user_type='unsubscribed'
       AND total_duration_in_minutes>=0
       AND (batchid=$batchId)
       GROUP BY user_id
       ORDER BY total_duration_played desc
       LIMIT 10""")
    }
    
    catch {
      case ex2: Exception => ex2.printStackTrace()
          throw new SparkException("Sql Exception")
    }
    
    finally{
      spark.stop()
    }


  }
}
