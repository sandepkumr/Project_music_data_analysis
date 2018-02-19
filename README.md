# Project_music_data_analysis

MusicProject folder to be downloaded and placed in /home/acadgild/ folder.

Directory Structure under MusicProject <br/>
==> logs ( Logs from running the script and current batch number ) <br/>
==> lib ( hive xml serde jar ) <br/>
==> spark ( Spark jar for music analysis ) <br/>
==> Scripts ( All the scripts ) <br/>
==> Data ( Location of loopup and incoming files ) <br/>
==> processed_dir ( valid and invalid data after enrichment from each batch) <br/>

Process to run:

1. First run hbase-lookup.sh to populate the lookup tables in hbase
2. Schedule music_data_analysis.sh to run for every 3 hours to music analysis


--Sandeep
