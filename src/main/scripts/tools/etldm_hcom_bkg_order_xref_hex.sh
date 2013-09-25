#!/bin/bash

export START_DATE=$1
export END_DATE=$2
export DB2_TABLE_NAME="ETL.V_ETLDM_HCOM_BKG_ORDER_XREF_HEX"
export SPLIT_COLUMN="ETL_UPDATE_DATETM"
export SPLIT_SIZE=1000000
export NUM_MAPPERS=4
export PARTITION_BY_FIELD=
export PARTITION_FIELD_FORMAT=
export EXCLUDE_PARTITION_KEY=
export BOOKMARK_FIELD=
export WHERE_CLAUSE="trans_date between '$START_DATE' and '$END_DATE'"
export HIVE_SCHEMA="ETLDATA"
export HIVE_TABLE_NAME="ETLDM_HCOM_BKG_ORDER_XREF_HEX"
export HIVE_PARTITION_KEY=
export PRIMARY_KEY=

export HWW_HOME=/usr/etl/HWW
sudo -E -u hwwetl $HWW_HOME/hww_dbsync_db2_to_hadoop/check_entity_hex.sh "Omniture Day and Booking XREF Complete" || exit 0
sudo -E -u platetl $HWW_HOME/hww_dbsync_db2_to_hadoop/run_dbsync_hex.sh || exit 1
