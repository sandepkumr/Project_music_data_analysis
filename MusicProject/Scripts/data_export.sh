#!/bin/bash

cycleid=`cat /home/acadgild/MusicProject/logs/cycle.txt`
LOGFILE=/home/acadgild/MusicProject/logs/log_batch_$cycleid
echo "Exporting data to MYSQL using sqoop export..." >> $LOGFILE
echo "Creating mysql tables if not present..." >> $LOGFILE

mysql -u root < /home/acadgild/MusicProject/Scripts/mysql_schema.sql

echo "Running sqoop job for data export..." >> $LOGFILE

hdfs=/user/hive/warehouse/musicproject.db/top_10_stations/batchid=${cycleid}/part-00000

sqoop export \
--connect jdbc:mysql://localhost/musicproject \
--username 'root' \
--table 'top_10_stations' \
--export-dir ${hdfs} \
--input-fields-terminated-by ',' \
-m 1

hdfs=/user/hive/warehouse/musicproject.db/user_type_duration/batchid=$cycleid/part-00000

sqoop export \
--connect jdbc:mysql://localhost/musicproject \
--username 'root' \
--table 'user_type_duration' \
--export-dir ${hdfs} \
--input-fields-terminated-by ',' \
-m 1

hdfs=/user/hive/warehouse/musicproject.db/connected_artists/batchid=$cycleid/part-00000
sqoop export \
--connect jdbc:mysql://localhost/musicproject \
--username 'root' \
--table 'connected_artists' \
--export-dir ${hdfs} \
--input-fields-terminated-by ',' \
-m 1

hdfs=/user/hive/warehouse/musicproject.db/top_10_songs_maxrevenue/batchid=$cycleid/part-00000
sqoop export \
--connect jdbc:mysql://localhost/musicproject \
--username 'root' \
--table 'top_10_songs_maxrevenue' \
--export-dir ${hdfs} \
--input-fields-terminated-by ',' \
-m 1

hdfs=/user/hive/warehouse/musicproject.db/top_10_unsubscribed_users/batchid=$cycleid/part-00000
sqoop export \
--connect jdbc:mysql://localhost/musicproject \
--username 'root' \
--table 'top_10_unsubscribed_users' \
--export-dir ${hdfs} \
--input-fields-terminated-by ',' \
-m 1
