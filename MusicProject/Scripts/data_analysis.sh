#!/bin/bash

cycleid=`cat /home/acadgild/MusicProject/logs/cycle.txt`

LOGFILE=/home/acadgild/MusicProject/logs/log_batch_$cycleid

echo "Running script for data analysis using spark..." >> $LOGFILE
chmod 775 /home/acadgild/MusicProject/spark/musicanalysis_2.11-0.1.jar


spark-submit    \
--class com.musicanalysis.exec.MusicAnalysis \
--master local[2] \
--driver-class-path /usr/local/hive/lib/hive-hbase-handler-1.2.1.jar:/usr/local/hbase/lib/* \
/home/acadgild/MusicProject/spark/musicanalysis_2.11-0.1.jar $cycleid
