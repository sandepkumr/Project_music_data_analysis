#!/bin/bash

cycleid=`cat /home/acadgild/MusicProject/logs/cycle.txt`
LOGFILE=/home/acadgild/MusicProject/logs/log_batch_$cycleid

echo "Creating hive tables on top of hbase tables for data enrichment and filtering..." >> $LOGFILE

hive -f /home/acadgild/MusicProject/Scripts/create_hive_hbase_lookup.hql
