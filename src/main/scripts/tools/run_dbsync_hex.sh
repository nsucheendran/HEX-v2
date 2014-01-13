#!/bin/bash

#Platform variables
export ETL_USER=platetl
export PLAT_HOME=/usr/local/edw/platform
export HWW_HOME=/usr/etl/HWW
SCRIPT_PATH=$HWW_HOME/hdp_hww_hex_etl
PROCESS_NAME="ETL_HCOM_HEX_TRANSACTIONS_BKG"

# DBSYNC variables
export DS_NAME="EDW_DB2"
export FILE_LAYOUT="SEQUENCE"
export COMPRESSION_TYPE="snappy"
export HIVE_TABLE="$HIVE_SCHEMA.$HIVE_TABLE_NAME"
export META_DATA_DIR="/etl/common/$HIVE_SCHEMA/meta-inf"
export IMPORT_TARGET_DIR="/raw/common/$HIVE_SCHEMA/$HIVE_TABLE_NAME"
export HIVE_TABLE_DIR="/data/common/$HIVE_SCHEMA/$HIVE_TABLE_NAME"
export TEMP_DIR="/tmp/common/$HIVE_SCHEMA/$HIVE_TABLE_NAME"
export DB2_TABLE=$DB2_TABLE_NAME

PROCESS_ID=$(_GET_PROCESS_ID "$PROCESS_NAME");
RETURN_CODE="$?"

if [ "$PROCESS_ID" == "" ] || (( $RETURN_CODE != 0 )); then
  _LOG "ERROR: Process [$PROCESS_NAME] does not exist in HEMS"
  ERROR_CODE=1
  _FREE_LOCK $HWW_TRANS_BKG_LOCK_NAME
  exit 1;
else
  RUN_ID=$(_RUN_PROCESS $PROCESS_ID "$PROCESS_NAME")
  _LOG "PROCESS_ID=[$PROCESS_ID]"
  _LOG "RUN_ID=[$RUN_ID]"
fi

EMAIL_TO=`_READ_PROCESS_CONTEXT $PROCESS_ID "EMAIL_TO"`
EMAIL_CC=`_READ_PROCESS_CONTEXT $PROCESS_ID "EMAIL_CC"`

EMAIL_RECIPIENTS=$EMAIL_TO
if [ $EMAIL_CC ]
then
  EMAIL_RECIPIENTS="-c $EMAIL_CC $EMAIL_RECIPIENTS"
fi

cd /tmp
source $PLAT_HOME/tools/sh_dbsync/dbsync_helpers.sh
_SYNCHRONIZE_TABLE
ERROR_CODE=$?
if [ $ERROR_CODE -ne 0 ]; then
  echo -e "====================================================================================================================================================================\nrun_dbsync_hex.sh failed with ERROR_CODE=$ERROR_CODE: No new data available from BOOKMARK=[${BOOKMARK}] in source.\nCheck source data availability (Refer to documentation : https://confluence/pages/viewpage.action?pageId=420855780)\n\nScript Name : $0\n====================================================================================================================================================================" | mailx -s "HWW HEX Alert (ETL_HCOM_HEX_TRANSACTIONS_BKG) ERROR: failed to load incremental data from source (Last Bookmark Date - ${BOOKMARK})" $EMAIL_RECIPIENTS
  exit 1
fi;

export DS_NAME="EDW_DB2_CLI"
$SCRIPT_PATH/tools/db2hive_counts_validation.sh
ERROR_CODE=$?
if [ $ERROR_CODE -ne 0 ]; then
  echo -e "====================================================================================================================================================================\nrun_dbsync_hex.sh failed with ERROR_CODE=$ERROR_CODE: No new data available from BOOKMARK=[${BOOKMARK}] in source.\nCheck source data availability (Refer to documentation : https://confluence/pages/viewpage.action?pageId=420855780)\n\nScript Name : $0\n====================================================================================================================================================================" | mailx -s "HWW HEX Alert (ETL_HCOM_HEX_TRANSACTIONS_BKG) ERROR: failed to load incremental data from source (Last Bookmark Date - ${BOOKMARK})" $EMAIL_RECIPIENTS
  exit 1
fi;
