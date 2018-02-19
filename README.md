# Project_music_data_analysis

MusicProject folder to be downloaded and placed in /home/acadgild/ folder.

Directory Structure
==> logs ( Logs from running the script and current batch number )
==> lib ( hive xml serde jar )
==> spark ( Spark jar for music analysis )
==> Scripts ( All the scripts )
==> Data ( Location of loopup and incoming files )
==> processed_dir ( valid and invalid data after enrichment from each batch)

Process to run:

1. First run hbase-lookup.sh to populate the lookup tables in hbase
2. Schedule music_data_analysis.sh to run for every 3 hours to music analysis


--Sandeep
