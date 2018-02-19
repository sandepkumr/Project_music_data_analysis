# Project_music_data_analysis

**USAGE**

MusicProject folder to be downloaded and placed in /home/acadgild/ folder.

Directory Structure under MusicProject <br/>
> * logs ( Logs from running the script and current batch number ) <br/> 
> * lib ( hive xml serde jar ) <br/>
> * spark ( Spark jar for music analysis ) <br/>
> * Scripts ( All the scripts ) <br/>
> * Data ( Location of loopup and incoming files ) <br/>
> * processed_dir ( valid and invalid data after enrichment from each batch) <br/>
> * ScalaClass ( scala code for spark analysis) <br/>

**PROCESS TO RUN**

> 1. First run hbase-lookup.sh to populate the lookup tables in hbase
> 2. Schedule music_data_analysis.sh to run for every 3 hours to music analysis

**SOFTWARE REQUIREMENT**
>Softwares OS: Centos <br/>
>Hadoop 2.6 <br/>
>Hive 1.2.1 <br/>
>Sqoop 1.4.5 <br/>
>Mysql 5.1.73 <br/>
>Hbase 0.99.14-hadoop2 <br/> 
>Spark 2.0.0 <br/>


--Sandeep
