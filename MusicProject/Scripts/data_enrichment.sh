#!/bin/bash

batchid=`cat /home/acadgild/MusicProject/logs/cycle.txt`
LOGFILE=/home/acadgild/MusicProject/logs/log_batch_$batchid
VALIDDIR=/home/acadgild/MusicProject/processed_dir/validated/batch_$batchid
INVALIDDIR=/home/acadgild/MusicProject/processed_dir/invalid/batch_$batchid

echo "Running hive script for data enrichment and filtering..." >> $LOGFILE

hive -hiveconf batchid=$batchid -f /home/acadgild/MusicProject/Scripts/data_enrichment.hql

if [ ! -d "$VALIDDIR" ]; then 
	mkdir -p "$VALIDDIR"
else
	rm -rf "$VALIDDIR"
	mkdir -p "$VALIDDIR"
fi

if [ ! -d "$INVALIDDIR" ]; then
	mkdir -p "$INVALIDDIR"
else
	rm -rf "$INVALIDDIR"
	mkdir -p "$INVALIDDIR"
fi


echo "Copying valid and invalid records in local file system..." >> $LOGFILE

hadoop fs -get /user/hive/warehouse/musicproject.db/enriched_data/batchid=$batchid/status=pass/* $VALIDDIR
hadoop fs -get /user/hive/warehouse/musicproject.db/enriched_data/batchid=$batchid/status=fail/* $INVALIDDIR


echo "Copying data to HDFS processed_dir"

hadoop fs -rm -r /hadoop/processed_dir/
hadoop fs -mkdir -p /hadoop/processed_dir/
hadoop fs -cp /user/hive/warehouse/musicproject.db/enriched_data/batchid=$batchid/status=pass/* /hadoop/processed_dir/

echo "Deleting older valid and invalid records from local file system..." >> $LOGFILE

find /home/acadgild/MusicProject/processed_dir/ -mtime +7 -exec rm {} \;
