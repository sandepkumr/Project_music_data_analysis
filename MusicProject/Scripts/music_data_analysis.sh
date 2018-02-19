#!/bin/bash

set -e

#All the below scripts will work based on the data provided by acadgild as data/web/file.xml and data/mob/file.txt

cycleid=`cat /home/acadgild/MusicProject/logs/cycle.txt`

LOGFILE=/home/acadgild/MusicProject/logs/log_batch_$cycleid

sh /home/acadgild/MusicProject/Scripts/data_enrichment_filtering_hive_schema.sh

sh /home/acadgild/MusicProject/Scripts/dataloading.sh

sh /home/acadgild/MusicProject/Scripts/data_enrichment.sh

sh /home/acadgild/MusicProject/Scripts/data_analysis.sh

sh /home/acadgild/MusicProject/Scripts/data_export.sh || true

echo "Incrementing cycleid..." >> $LOGFILE
cycleid=`expr $cycleid + 1`
echo -n $cycleid > /home/acadgild/MusicProject/logs/cycle.txt
