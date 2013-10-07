#!/bin/bash

#Platform variables
export ETL_USER=platetl
export PLAT_HOME=/usr/local/edw/platform
export HWW_HOME=/usr/etl/HWW
SCRIPT_PATH=$HWW_HOME/hdp_hww_hex_etl

# DBSYNC variables
export DS_NAME=EDW_DB2
export FILE_LAYOUT="SEQUENCE"
export COMPRESSION_TYPE="snappy"
export HIVE_TABLE="$HIVE_SCHEMA.$HIVE_TABLE_NAME"
export META_DATA_DIR="/etl/common/$HIVE_SCHEMA/meta-inf"
export IMPORT_TARGET_DIR="/raw/common/$HIVE_SCHEMA/$HIVE_TABLE_NAME"
export HIVE_TABLE_DIR="/data/common/$HIVE_SCHEMA/$HIVE_TABLE_NAME"
export TEMP_DIR="/tmp/common/$HIVE_SCHEMA/$HIVE_TABLE_NAME"
export DB2_TABLE=$DB2_TABLE_NAME
export BOOKMARK_PATTERN='+%Y-%m-%d'

cd /tmp
source $PLAT_HOME/tools/sh_dbsync/dbsync_helpers.sh
_SYNCHRONIZE_TABLE || exit 1

source $PLAT_HOME/common/sh_helpers.sh
source $PLAT_HOME/common/sh_metadata_storage.sh

PROCESS_NAME="ETL_HCOM_HEX_TRANSACTIONS_BKG"

ERROR_CODE=0

PROCESS_ID=$(_GET_PROCESS_ID "$PROCESS_NAME");
RETURN_CODE="$?"

if [ "$PROCESS_ID" == "" ] || (( $RETURN_CODE != 0 )); then
  _LOG "ERROR: Process [$PROCESS_NAME] does not exist in HEMS"
  ERROR_CODE=1
  exit 1;
fi

BOOKMARK=`_READ_PROCESS_CONTEXT $PROCESS_ID "BOOKMARK"`
export BOOKMARK=`date --date="${BOOKMARK}" +1 days "${BOOKMARK_PATTERN}"`
# validation doesn't work as expected (always validates only the latest partition)

$SCRIPT_PATH/tools/db2hive_counts_validation.sh || exit 1
