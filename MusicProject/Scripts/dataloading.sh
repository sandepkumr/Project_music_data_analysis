#!/bin/bash
cycleid=`cat /home/acadgild/MusicProject/logs/cycle.txt`
LOGFILE=/home/acadgild/MusicProject/logs/log_batch_$cycleid

echo "Placing data files from local to HDFS..." >> $LOGFILE

hadoop fs -rm -r /user/acadgild/project/batch${cycleid}/web/
#hadoop fs -rm -r /user/acadgild/project/batch${batchid}/formattedweb/
hadoop fs -rm -r /user/acadgild/project/batch${cycleid}/mob/

hadoop fs -mkdir -p  /user/acadgild/project/batch${cycleid}/web/
hadoop fs -mkdir -p  /user/acadgild/project/batch${cycleid}/mob/

hadoop fs -put /home/acadgild/MusicProject/Data/Web/file.xml /user/acadgild/project/batch${cycleid}/web/
hadoop fs -put /home/acadgild/MusicProject/Data/Mob/file.txt /user/acadgild/project/batch${cycleid}/mob/

echo "Loading data using xml formatter into external table and merging mob and web.." >> $LOGFILE

hive --auxpath /home/acadgild/MusicProject/lib/hivexmlserde-1.0.5.3.jar -hiveconf batchid=$cycleid -f /home/acadgild/MusicProject/Scripts/hive_data_load.hql 






