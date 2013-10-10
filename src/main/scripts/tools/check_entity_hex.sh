#!/bin/bash


export PLAT_HOME=/usr/local/edw/platform

source $PLAT_HOME/common/sh_helpers.sh
source $PLAT_HOME/common/sh_metadata_storage.sh

PROCESS_NAME="ETL_HCOM_HEX_TRANSACTIONS_BKG"
_LOG "PROCESS_NAME=[$PROCESS_NAME]"

ERROR_CODE=0

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
  _LOG_PROCESS_DETAIL $RUN_ID "Started" "$ERROR_CODE"
fi

BOOKRMARK=`_READ_PROCESS_CONTEXT $PROCESS_ID "BOOKMARK"`
PROCESSING_TYPE=`_READ_PROCESS_CONTEXT $PROCESS_ID "PROCESSING_TYPE"`

_LOG "PROCESSING_TYPE=$PROCESSING_TYPE"
_LOG "LAST_DT=$LAST_DT"

if [ $PROCESSING_TYPE = "R" ]; then
  export END_DATE=$BOOKMARK
else
  export END_DATE=`date --date="${BOOKMARK} +1 days" '+%Y-%m-%d'`
fi

export HWW_HOME=/usr/etl/HWW
source $HWW_HOME/common/sh_hww_helpers.sh

export ENTITY=$1

_DBCONNECT

STATUS=$($DB2_HOME/sqllib/bin/db2 -x "select count(*) from etl.etl_fileprocess where entity = '$ENTITY' and data = '$END_DATE' with ur")
ERROR_CODE=$?

_DBDISCONNECT

if [[ $ERROR_CODE -eq 0 && $STATUS -eq 1 ]]; then
  echo "[$ENTITY] has completed"
  exit 0
else
  echo "[$ENTITY] has not completed"
  exit 1
fi
