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

PROCESS_ID=$(_GET_PROCESS_ID "ETL_HCOM_HEX_TRANSACTIONS_BKG");
RETURN_CODE="$?"

EMAIL_TO=`_READ_PROCESS_CONTEXT $PROCESS_ID "EMAIL_TO"`
EMAIL_CC=`_READ_PROCESS_CONTEXT $PROCESS_ID "EMAIL_CC"`

EMAIL_RECIPIENTS=$EMAIL_TO
if [ $EMAIL_CC ]
then
  EMAIL_RECIPIENTS="-c $EMAIL_CC $EMAIL_RECIPIENTS"
fi

sudo -E -u hwwetl $HWW_HOME/hdp_hww_hex_etl/tools/check_entity_hex.sh "Omniture Day and Booking XREF Complete"
if [ $? -ne 0 ]; then
  echo -e "check_entity_hex.sh failed: no new data available in source.\n -- $0" | mailx -s "[HEXv2] WARN: no incremental data in source" $EMAIL_RECIPIENTS
  exit -1
else
  sudo -E -u platetl $HWW_HOME/hdp_hww_hex_etl/tools/run_dbsync_hex.sh
  ERROR_CODE=$?
  if [ $? -ne 0 ]; then
    echo -e "run_dbsync_hex.sh failed with ERROR_CODE=$ERROR_CODE.\n -- $0" | mailx -s "[HEXv2] ERROR: failed to load incremental data from source" $EMAIL_RECIPIENTS
    exit 1
  fi;
fi;

