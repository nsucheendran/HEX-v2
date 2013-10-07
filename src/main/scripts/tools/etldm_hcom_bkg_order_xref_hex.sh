#!/bin/bash
 
export DB2_TABLE_NAME="ETL.V_ETLDM_HCOM_BKG_ORDER_XREF_HEX"
export SPLIT_COLUMN="ETL_UPDATE_DATETM"
export SPLIT_SIZE=2000000
export NUM_MAPPERS=4
export PARTITION_BY_FIELD="TRANS_DATE"
export PARTITION_FIELD_FORMAT="yyyy-MM"
export EXCLUDE_PARTITION_KEY=1
export BOOKMARK_FIELD="ETL_UPDATE_DATETM"
export WHERE_CLAUSE=
export HIVE_SCHEMA="ETLDATA"
export HIVE_TABLE_NAME="ETLDM_HCOM_BKG_ORDER_XREF_HEX"
export HIVE_PARTITION_KEY="YEAR_MONTH"
export PRIMARY_KEY="BK_DATETM,TPID,TRL,TRANS_SEQUENCE"
 
export HWW_HOME=/usr/etl/HWW
sudo -E -u hwwetl $HWW_HOME/hdp_hww_hex_etl/tools/check_entity_hex.sh "Omniture Day and Booking XREF Complete" || exit 0
sudo -E -u platetl $HWW_HOME/hdp_hww_hex_etl/tools/run_dbsync_hex.sh || exit 1


