#!/bin/bash
#
# LNX-HCOM_HEX_TRANSACTIONS_BKG.sh
# Wrapper to load first assignment hit data for HEX
#
# Usage:
#  LNX-HCOM_HEX_TRANSACTIONS_BKG.sh
#
# Error codes:
#   0 Success
#   1 General error
#
# user          date            comment
# ############# ############### ####################
# achadha       2013-09-23      Wrapper to incrementally load/reprocess transactions data from bookings dm

set -m
export PLAT_HOME=/usr/local/edw/platform
export HWW_HOME=/usr/etl/HWW
SCRIPT_PATH=$HWW_HOME/hdp_hww_hex_etl/hql/BKG_TRANS
SCRIPT_PATH_TRANS=$HWW_HOME/hdp_hww_hex_etl/hql/TRANS
HEX_LOGS=/usr/etl/HWW/log

source $PLAT_HOME/common/sh_helpers.sh
source $PLAT_HOME/common/sh_metadata_storage.sh

HWW_TRANS_BKG_LOCK_NAME="hdp_hww_hex_transactions_bkg.lock"
MESSAGE="LNX-HCOM_HEX_TRANSACTIONS_BKG.sh failed: Previous script still running"
_ACQUIRE_LOCK $HWW_TRANS_BKG_LOCK_NAME "$MESSAGE" 30

STAGE_NAME="BKG_TRANS: HEX Booking Transactions"
_LOG_START_STAGE "$STAGE_NAME"

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
  _LOG_PROCESS_DETAIL $RUN_ID "STATUS" "STARTED"
fi

TRANS_BKG_TABLE=`_READ_PROCESS_CONTEXT $PROCESS_ID "TRANS_TABLE"`
TRANS_BKG_DB=`_READ_PROCESS_CONTEXT $PROCESS_ID "TRANS_DB"`
JOB_QUEUE=`_READ_PROCESS_CONTEXT $PROCESS_ID "JOB_QUEUE"`
LAST_DT=`_READ_PROCESS_CONTEXT $PROCESS_ID "BOOKMARK"`
PROCESSING_TYPE=`_READ_PROCESS_CONTEXT $PROCESS_ID "PROCESSING_TYPE"`
EMAIL_TO=`_READ_PROCESS_CONTEXT $PROCESS_ID "EMAIL_TO"`
EMAIL_CC=`_READ_PROCESS_CONTEXT $PROCESS_ID "EMAIL_CC"`

EMAIL_RECIPIENTS=$EMAIL_TO
if [ $EMAIL_CC ]
then
  EMAIL_RECIPIENTS="-c $EMAIL_CC $EMAIL_RECIPIENTS"
fi

_LOG "PROCESSING_TYPE=$PROCESSING_TYPE"
_LOG "BOOKMARK=$LAST_DT"

_LOG_PROCESS_DETAIL $RUN_ID "BEFORE_BOOKMARK" "$LAST_DT"
_LOG_PROCESS_DETAIL $RUN_ID "PROCESSING_TYPE" "$PROCESSING_TYPE"

if [ $PROCESSING_TYPE = "R" ];
then

  START_YEAR=`_READ_PROCESS_CONTEXT $PROCESS_ID "REPROCESS_START_YEAR"`
  START_MONTH=`_READ_PROCESS_CONTEXT $PROCESS_ID "REPROCESS_START_MONTH"`

  END_YEAR=`date --date="${LAST_DT}" '+%Y'`
  END_MONTH=`date --date="${LAST_DT}" '+%m'`

  _LOG "Starting Reprocessing for period: $START_YEAR-$START_MONTH to $END_YEAR-$END_MONTH (BOOKMARK=[$LAST_DT])"

  CURR_YEAR=$START_YEAR
  CURR_MONTH=$START_MONTH
  while [ "${CURR_YEAR}${CURR_MONTH}" \< "${END_YEAR}${END_MONTH}" -o "${CURR_YEAR}${CURR_MONTH}" = "${END_YEAR}${END_MONTH}" ]
  do

    LOG_FILE_NAME="hdp_transactions_bkg_drop_partition_${CURR_YEAR}-${CURR_MONTH}.log"
    _LOG "Dropping partition [$CURR_YEAR-$CURR_MONTH] from target: $TRANS_BKG_DB.$TRANS_BKG_TABLE"
    hive -hiveconf part.year="${CURR_YEAR}" -hiveconf part.month="${CURR_MONTH}" -hiveconf part.source="booking" -hiveconf job.queue="${JOB_QUEUE}" -hiveconf hex.fah.db="${TRANS_BKG_DB}" -hiveconf hex.trans.table="${TRANS_BKG_TABLE}" -f $SCRIPT_PATH_TRANS/delete_ETL_HCOM_HEX_TRANSACTIONS.hql >> $HEX_LOGS/$LOG_FILE_NAME 2>&1
    ERROR_CODE=$?
    if [[ $ERROR_CODE -ne 0 ]]; then
      _LOG "ERROR while dropping partition [ERROR_CODE=$ERROR_CODE]. See [$HEX_LOGS/$LOG_FILE_NAME] for more information."
    fi

    NEW_YEAR=`date --date="${CURR_YEAR}-${CURR_MONTH}-01 00 +1 months" '+%Y'`
    CURR_MONTH=`date --date="${CURR_YEAR}-${CURR_MONTH}-01 00 +1 months" '+%m'`
    CURR_YEAR=$NEW_YEAR
  done

  # reprocess data in monthly chunks upto and including the bookmark date, do not change bookmark in HEMS
  CURR_YEAR=$START_YEAR
  CURR_MONTH=$START_MONTH

  while [ "${CURR_YEAR}${CURR_MONTH}" \< "${END_YEAR}${END_MONTH}" -o "${CURR_YEAR}${CURR_MONTH}" = "${END_YEAR}${END_MONTH}" ]
  do
    START_DT=`date --date="${CURR_YEAR}-${CURR_MONTH}-01" '+%Y-%m-%d'`
    if [ "${CURR_YEAR}${CURR_MONTH}" \< "${END_YEAR}${END_MONTH}" ]
    then
      END_DT=`date --date="${CURR_YEAR}-${CURR_MONTH}-01 +1 months -1 days" '+%Y-%m-%d'`
    else
      END_DT=`date --date="${LAST_DT}" '+%Y-%m-%d'`
    fi
    
    MONTH=`date --date="${START_DT}" '+%Y-%m'`
    LOG_FILE_NAME="hdp_transactions_bkg_reprocess_${START_DT}-${END_DT}.log"

    _LOG "Reprocessing Booking Transactions data between [$START_DT to $END_DT] in target: $TRANS_BKG_DB.$TRANS_BKG_TABLE"

    hive -hiveconf into.overwrite="overwrite" -hiveconf month="${MONTH}" -hiveconf start.date="${START_DT}" -hiveconf end.date="${END_DT}" -hiveconf job.queue="${JOB_QUEUE}" -hiveconf hex.fah.db="${TRANS_BKG_DB}" -hiveconf hex.trans.table="${TRANS_BKG_TABLE}" -f $SCRIPT_PATH/insert_ETL_HCOM_HEX_TRANSACTIONS_BOOKING.hql >> $HEX_LOGS/$LOG_FILE_NAME 2>&1 
    ERROR_CODE=$?
    if [[ $ERROR_CODE -ne 0 ]]; then
      _LOG "BKG_TRANS: Booking Transactions load FAILED [ERROR_CODE=$ERROR_CODE]. See [$HEX_LOGS/$LOG_FILE_NAME] for more information."
      _END_PROCESS $RUN_ID $ERROR_CODE
      _LOG_PROCESS_DETAIL $RUN_ID "STATUS" "ERROR: $ERROR_CODE"
      _FREE_LOCK $HWW_TRANS_BKG_LOCK_NAME
      exit 1
    fi

    NEW_YEAR=`date --date="${CURR_YEAR}-${CURR_MONTH}-01 +1 months" '+%Y'`
    CURR_MONTH=`date --date="${CURR_YEAR}-${CURR_MONTH}-01 +1 months" '+%m'`
    CURR_YEAR=$NEW_YEAR
  done
  
  _LOG "Done Reprocessing"

  if [ -z "$LAST_DT" ]; then
    _LOG "Updating BOOKMARK (since none existed) as $END_DT"
    _WRITE_PROCESS_CONTEXT "$PROCESS_ID" "BOOKMARK" "$END_DT"
  fi
  _LOG_PROCESS_DETAIL $RUN_ID "AFTER_BOOKMARK" "$END_DT"
  
  _LOG "Setting PROCESSING_TYPE to [D] for next run"
  _WRITE_PROCESS_CONTEXT "$PROCESS_ID" "PROCESSING_TYPE" "D"
else
  # daily incremental load

  _LOG "Incremental Booking Transactions data load (BOOKMARK=[$LAST_DT])"
  START_DT=`date --date="${LAST_DT} +1 days" '+%Y-%m-%d'`
  END_DT=$START_DT


  LOG_FILE_NAME="hdp_transactions_bkg_incremental_${START_DT}.log"
  MONTH=`date --date="${START_DT}" '+%Y-%m'`

  hive -hiveconf into.overwrite="into" -hiveconf month="${MONTH}" -hiveconf start.date="${START_DT}" -hiveconf end.date="${END_DT}" -hiveconf job.queue="${JOB_QUEUE}" -hiveconf hex.fah.db="${TRANS_BKG_DB}" -hiveconf hex.trans.table="${TRANS_BKG_TABLE}" -f $SCRIPT_PATH/insert_ETL_HCOM_HEX_TRANSACTIONS_BOOKING.hql >> $HEX_LOGS/$LOG_FILE_NAME 2>&1 
  ERROR_CODE=$?
  if [[ $ERROR_CODE -ne 0 ]]; then
    _LOG "BKG_TRANS: Booking Transactions load FAILED [ERROR_CODE=$ERROR_CODE]. See [$HEX_LOGS/$LOG_FILE_NAME] for more information."
    _END_PROCESS $RUN_ID $ERROR_CODE
    _LOG_PROCESS_DETAIL $RUN_ID "STATUS" "ERROR: $ERROR_CODE"
    _FREE_LOCK $HWW_TRANS_BKG_LOCK_NAME
    exit 1
  fi

  _WRITE_PROCESS_CONTEXT "$PROCESS_ID" "BOOKMARK" "$END_DT"
  if [[ $ERROR_CODE -ne 0 ]]; then
    _LOG "HEMS ERROR! Unable to update bookmark. [ERROR_CODE=$ERROR_CODE]. Manually Update Bookmark before next run or reprocess!"
    _END_PROCESS $RUN_ID $ERROR_CODE
    _LOG_PROCESS_DETAIL $RUN_ID "STATUS" "ERROR: $ERROR_CODE"
    _FREE_LOCK $HWW_TRANS_BKG_LOCK_NAME
    exit 1
  fi
  _LOG_PROCESS_DETAIL $RUN_ID "AFTER_BOOKMARK" "$END_DT"
  _LOG "Updated Bookmark to [$END_DT]"
fi

_LOG_PROCESS_DETAIL $RUN_ID "STATUS" "SUCCESS"
_END_PROCESS $RUN_ID $ERROR_CODE
_FREE_LOCK $HWW_TRANS_BKG_LOCK_NAME

_LOG "Job completed successfully"

