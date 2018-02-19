#!/bin/bash

cycleid=`cat /home/acadgild/MusicProject/logs/cycle.txt`

LOGFILE=/home/acadgild/MusicProject/logs/log_batch_$batchid

echo "Creating LookUp Tables" >> $LOGFILE

echo "create 'station_geo_map', 'geo'" | hbase shell
echo "create 'subscribed_users', 'subscn'" | hbase shell
echo "create 'song_artist_map', 'artist'" | hbase shell
echo "create 'user_artist_map', 'user'" | hbase shell


echo "Populating LookUp Tables" >> $LOGFILE

file="/home/acadgild/MusicProject/Data/LookUp/stn-geocd.txt"
while IFS= read -r line
do
 stnid=`echo $line | cut -d',' -f1`
 geocd=`echo $line | cut -d',' -f2`
 echo "put 'station_geo_map', '$stnid', 'geo:geo_cd', '$geocd'" | hbase shell
done <"$file"
 
file="/home/acadgild/MusicProject/Data/LookUp/song-artist.txt"
while IFS= read -r line
do
 songid=`echo $line | cut -d',' -f1`
 artistid=`echo $line | cut -d',' -f2`
 echo "put 'song_artist_map', '$songid', 'artist:artistid', '$artistid'" | hbase shell
done <"$file"

file="/home/acadgild/MusicProject/Data/LookUp/user-subscn.txt"
while IFS= read -r line
do
 userid=`echo $line | cut -d',' -f1`
 startdt=`echo $line | cut -d',' -f2`
 enddt=`echo $line | cut -d',' -f3`
 echo "put 'subscribed_users', '$userid', 'subscn:startdt', '$startdt'" | hbase shell
 echo "put 'subscribed_users', '$userid', 'subscn:enddt', '$enddt'" | hbase shell
done <"$file"

file="/home/acadgild/MusicProject/Data/LookUp/user-artist.txt"
touch "/home/acadgild/MusicProject/Data/LookUp/user-artist1.txt"
chmod 775 "/home/acadgild/MusicProject/Data/LookUp/user-artist1.txt"
file1="/home/acadgild/MusicProject/Data/LookUp/user-artist1.txt"
awk '$1=$1' FS="&" OFS=" " $file > $file1
num=1
while IFS= read -r line
do
 userid=`echo $line | cut -d',' -f1`
 artists=`echo $line | cut -d',' -f2`
 for row in $artists
 do
 echo "put 'user_artist_map', '$userid', 'user:artist_$num', '$row'" | hbase shell
 let "num=num+1"
done
num=1
done <"$file1"
rm "/home/acadgild/MusicProject/Data/LookUp/user-artist1.txt"

