edw-HEXHadoopETL
================

HEX ETL migration to Hadoop.

Assumptions
===========

1. assumes presence of valid hdpenv.conf and hexdbconf files in /home/hwwetl
2. assumes presence of valid segmentations.txt and HEX2_REPORTING_INPUT.csv at the location specified in install.sh
3. assumes db2 permissions and configurations for load of data to db2
4. assumes correct values to be set for environment variables ETLCOMMONSCR and DB2INSTANCE directly or indirectly from /home/hwwetl/.bashrc
