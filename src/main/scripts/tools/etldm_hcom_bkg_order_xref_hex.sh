#!/bin/bash

export PLAT_HOME=/usr/local/edw/platform

source $PLAT_HOME/common/sh_helpers.sh
source $PLAT_HOME/common/sh_metadata_storage.sh

export DB2_TABLE_NAME="ETL.V_ETLDM_HCOM_BKG_ORDER_XREF_HEX"
export SPLIT_COLUMN="TRANS_DATE"
export SPLIT_SIZE=2000000
export NUM_MAPPERS=4
export PARTITION_BY_FIELD="TRANS_DATE"
export PARTITION_FIELD_FORMAT="yyyy-MM"
export EXCLUDE_PARTITION_KEY=0
export BOOKMARK_FIELD="TRANS_DATE"
export WHERE_CLAUSE=
export HIVE_SCHEMA="ETLDATA"
export HIVE_TABLE_NAME="ETLDM_HCOM_BKG_ORDER_XREF_HEX"
export HIVE_PARTITION_KEY="YEAR_MONTH"
export HIVE_PARTITION_PATTERN='+%Y-%m'
export DB2_UNIT_PATTERN='+%Y-%m-%d'
export PROCESS_BOOKMARK_FIELD='TRANS_DATE'
export PRIMARY_KEY="BK_DATETM,TPID,TRL,TRANS_SEQUENCE"

export HWW_HOME=/usr/etl/HWW

sudo -E -u hwwetl $HWW_HOME/hdp_hww_hex_etl/tools/check_entity_hex.sh "Omniture Day and Booking XREF Complete"
if [ $? -ne 0 ]; then
  exit -1
else
  sudo -E -u platetl $HWW_HOME/hdp_hww_hex_etl/tools/run_dbsync_hex.sh
  ERROR_CODE=$?
  if [ $? -ne 0 ]; then
    exit 1
  fi;
fi;

