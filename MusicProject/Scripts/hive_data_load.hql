set hive.support.sql11.reserved.keywords=false;

USE musicproject;

DROP TABLE IF EXISTS WEB_FORMATTED_DATA;
CREATE EXTERNAL TABLE WEB_FORMATTED_DATA (
user_id STRING,
song_id STRING,
artist_id STRING,
timestamp STRING,
start_ts STRING,
end_ts STRING,
geo_cd STRING,
station_id STRING,
song_end_type INT,
like INT,
dislike INT)
 ROW FORMAT SERDE 'com.ibm.spss.hive.serde2.xml.XmlSerDe'
 WITH SERDEPROPERTIES (
 "column.xpath.user_id"="/record/user_id/text()",
 "column.xpath.song_id"="/record/song_id/text()",
  "column.xpath.artist_id"="/record/artist_id/text()",
 "column.xpath.timestamp"="/record/timestamp/text()",
  "column.xpath.start_ts"="/record/start_ts/text()",
 "column.xpath.end_ts"="/record/end_ts/text()",
  "column.xpath.geo_cd"="/record/geo_cd/text()",
 "column.xpath.station_id"="/record/station_id/text()",
  "column.xpath.song_end_type"="/record/song_end_type/text()",
 "column.xpath.like"="/record/like/text()",
  "column.xpath.dislike"="/record/dislike/text()")
 STORED AS INPUTFORMAT 'com.ibm.spss.hive.serde2.xml.XmlInputFormat'
 OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.IgnoreKeyTextOutputFormat'
 LOCATION '/user/acadgild/project/batch${hiveconf:batchid}/web/'
 TBLPROPERTIES ("xmlinput.start"="<record>","xmlinput.end"= "</record>");

 CREATE TABLE IF NOT EXISTS MERGED_DATA
(
user_id STRING,
song_id STRING,
artist_id STRING,
timestamp STRING,
start_ts STRING,
end_ts STRING,
geo_cd STRING,
station_id STRING,
song_end_type INT,
like INT,
dislike INT
)
PARTITIONED BY
(batchid INT)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

INSERT OVERWRITE TABLE MERGED_DATA
PARTITION(batchid=${hiveconf:batchid})
SELECT 
user_id ,
song_id ,
artist_id ,
unix_timestamp(timestamp , 'yyyy-MM-dd HH:mm:ss') ,
unix_timestamp(start_ts , 'yyyy-MM-dd HH:mm:ss') ,
unix_timestamp(end_ts , 'yyyy-MM-dd HH:mm:ss'),
geo_cd ,
station_id ,
song_end_type ,
like ,
dislike 
from WEB_FORMATTED_DATA;


LOAD DATA INPATH '/user/acadgild/project/batch${hiveconf:batchid}/mob/'
INTO TABLE merged_data PARTITION (batchid=${hiveconf:batchid});

