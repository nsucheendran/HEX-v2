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
SCRIPT_PATH_TOOLS=$HWW_HOME/hdp_hww_hex_etl/tools
SCRIPT_PATH=$HWW_HOME/hdp_hww_hex_etl/hql/R3
HEX_LOGS=/usr/etl/HWW/log
ETL_USER='hwwetl'
PLT_USER='platetl'

source $PLAT_HOME/common/sh_helpers.sh
source $PLAT_HOME/common/sh_metadata_storage.sh

HWW_TRANS_BKG_LOCK_NAME="hdp_hww_hex_transactions_bkg.lock"
MESSAGE="LNX-HCOM_HEX_TRANSACTIONS_BKG.sh failed: Previous script still running"
sudo -E -u $ETL_USER _ACQUIRE_LOCK $HWW_TRANS_BKG_LOCK_NAME "$MESSAGE" 30

STAGE_NAME="R3: HEX Booking Transactions"
sudo -E -u $ETL_USER _LOG_START_STAGE "$STAGE_NAME"

PROCESS_NAME="ETL_HCOM_HEX_TRANSACTIONS_BKG"
sudo -E -u $ETL_USER _LOG "PROCESS_NAME=[$PROCESS_NAME]"

ERROR_CODE=0

PROCESS_ID=$(sudo -E -u $ETL_USER _GET_PROCESS_ID "$PROCESS_NAME");
RETURN_CODE="$?"

if [ "$PROCESS_ID" == "" ] || (( $RETURN_CODE != 0 )); then
  sudo -E -u $ETL_USER _LOG "ERROR: Process [$PROCESS_NAME] does not exist in HEMS"
  ERROR_CODE=1
  sudo -E -u $ETL_USER _FREE_LOCK $HWW_TRANS_BKG_LOCK_NAME
  exit 1;
else
  RUN_ID=$(sudo -E -u $ETL_USER _RUN_PROCESS $PROCESS_ID "$PROCESS_NAME")
  sudo -E -u $ETL_USER _LOG "PROCESS_ID=[$PROCESS_ID]"
  sudo -E -u $ETL_USER _LOG "RUN_ID=[$RUN_ID]"
  sudo -E -u $ETL_USER _LOG_PROCESS_DETAIL $RUN_ID "Started" "$ERROR_CODE"
fi

TRANS_BKG_TABLE=`sudo -E -u $ETL_USER _READ_PROCESS_CONTEXT $PROCESS_ID "TRANS_TABLE"`
TRANS_BKG_DB=`sudo -E -u $ETL_USER _READ_PROCESS_CONTEXT $PROCESS_ID "TRANS_DB"`
JOB_QUEUE=`sudo -E -u $ETL_USER _READ_PROCESS_CONTEXT $PROCESS_ID "JOB_QUEUE"`
LAST_DT=`sudo -E -u $ETL_USER _READ_PROCESS_CONTEXT $PROCESS_ID "BOOKMARK"`
PROCESSING_TYPE=`sudo -E -u $ETL_USER _READ_PROCESS_CONTEXT $PROCESS_ID "PROCESSING_TYPE"`

_LOG "PROCESSING_TYPE=$PROCESSING_TYPE"
_LOG "LAST_DT=$LAST_DT"

if [ $PROCESSING_TYPE = "R" ];
then
  START_YEAR=`sudo -E -u $ETL_USER _READ_PROCESS_CONTEXT $PROCESS_ID "REPROCESS_START_YEAR"`
  START_MONTH=`sudo -E -u $ETL_USER _READ_PROCESS_CONTEXT $PROCESS_ID "REPROCESS_START_MONTH"`

  END_YEAR=`date --date="${LAST_DT}" '+%Y'`
  END_MONTH=`date --date="${LAST_DT}" '+%m'`

  _LOG "Starting Reprocessing for period: $START_YEAR-$START_MONTH to $END_YEAR-$END_MONTH (BOOKMARK=[$LAST_DT])"

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
   
    LOG_FILE_NAME="hdp_transactions_bkg_reprocess_${START_DT}-${END_DT}.log"

    sudo -E -u $ETL_USER _LOG "Reprocessing Booking Transactions data between [$START_DT to $END_DT] in target: $TRANS_BKG_DB.$TRANS_BKG_TABLE"

    $SCRIPT_PATH_TOOLS/etldm_hcom_bkg_order_xref_hex.sh "$START_DT" "$END_DT"
    ERROR_CODE=$?
    if [[ $ERROR_CODE -ne 0 ]]; then
      _LOG "R3: Booking Transactions load FAILED [ERROR_CODE=$ERROR_CODE]. [see $HEX_LOGS/$LOG_FILE_NAME] for more information."
      _END_PROCESS $RUN_ID $ERROR_CODE
      _FREE_LOCK $HWW_TRANS_BKG_LOCK_NAME
      exit 1
    fi
    sudo -E -u $ETL_USER hive -hiveconf into.overwrite="insert" -hiveconf start.date="${START_DT}" -hiveconf end.date="${END_DT}" -hiveconf job.queue="${JOB_QUEUE}" -hiveconf hex.fah.db="${TRANS_BKG_DB}" -hiveconf hex.trans.table="${TRANS_BKG_TABLE}" -f $SCRIPT_PATH_R3/insert_ETL_HEX_TRANSACTIONS_BOOKING.hql >> $HEX_LOGS/$LOG_FILE_NAME 2>&1 
    ERROR_CODE=$?
    if [[ $ERROR_CODE -ne 0 ]]; then
      sudo -E -u $ETL_USER _LOG "R3: Booking Transactions load FAILED [ERROR_CODE=$ERROR_CODE]. [see $HEX_LOGS/$LOG_FILE_NAME] for more information."
      sudo -E -u $ETL_USER _END_PROCESS $RUN_ID $ERROR_CODE
      sudo -E -u $ETL_USER _FREE_LOCK $HWW_TRANS_BKG_LOCK_NAME
      exit 1
    fi

    NEW_YEAR=`date --date="${CURR_YEAR}-${CURR_MONTH}-01 +1 months" '+%Y'`
    CURR_MONTH=`date --date="${CURR_YEAR}-${CURR_MONTH}-01 +1 months" '+%m'`
    CURR_YEAR=$NEW_YEAR
  done
  sudo -E -u $ETL_USER _LOG "Done Reprocessing"

  if [ -z "$LAST_DT" ]; then
    sudo -E -u $ETL_USER _LOG "Updating BOOKMARK (since none existed) as $END_DT"
    sudo -E -u $ETL_USER _WRITE_PROCESS_CONTEXT "$PROCESS_ID" "BOOKMARK" "$END_DT"
  fi
  sudo -E -u $ETL_USER _LOG "Setting PROCESSING_TYPE to [D] for next run"
  sudo -E -u $ETL_USER _WRITE_PROCESS_CONTEXT "$PROCESS_ID" "PROCESSING_TYPE" "D"
  
else
  # daily incremental load

  sudo -E -u $ETL_USER _LOG "Incremental Booking Transactions data load (BOOKMARK=[$LAST_DT])"
  START_DT=`date --date="${LAST_DT} +1 days" '+%Y-%m-%d'`
  END_DT=$START_DT

  $SCRIPT_PATH_TOOLS/etldm_hcom_bkg_order_xref_hex.sh "$START_DT" "$END_DT"
  ERROR_CODE=$?
  if [[ $ERROR_CODE -ne 0 ]]; then
    sudo -E -u $ETL_USER _LOG "R3: Booking Transactions load FAILED [ERROR_CODE=$ERROR_CODE]. [see $HEX_LOGS/$LOG_FILE_NAME] for more information."
    sudo -E -u $ETL_USER _END_PROCESS $RUN_ID $ERROR_CODE
    sudo -E -u $ETL_USER _FREE_LOCK $HWW_TRANS_BKG_LOCK_NAME
    exit 1
  fi
  sudo -E -u $ETL_USER hive -hiveconf into.overwrite="insert" -hiveconf start.date="${START_DT}" -hiveconf end.date="${END_DT}" -hiveconf job.queue="${JOB_QUEUE}" -hiveconf hex.fah.db="${TRANS_BKG_DB}" -hiveconf hex.trans.table="${TRANS_BKG_TABLE}" -f $SCRIPT_PATH_R3/insert_ETL_HEX_TRANSACTIONS_BOOKING.hql >> $HEX_LOGS/$LOG_FILE_NAME 2>&1 
  ERROR_CODE=$?
  if [[ $ERROR_CODE -ne 0 ]]; then
    sudo -E -u $ETL_USER _LOG "R3: Booking Transactions load FAILED [ERROR_CODE=$ERROR_CODE]. [see $HEX_LOGS/$LOG_FILE_NAME] for more information."
    sudo -E -u $ETL_USER _END_PROCESS $RUN_ID $ERROR_CODE
    sudo -E -u $ETL_USER _FREE_LOCK $HWW_TRANS_BKG_LOCK_NAME
    exit 1
  fi

  sudo -E -u $ETL_USER _WRITE_PROCESS_CONTEXT "$PROCESS_ID" "BOOKMARK" "$END_DT"
  if [[ $ERROR_CODE -ne 0 ]]; then
    sudo -E -u $ETL_USER _LOG "HEMS ERROR! Unable to update bookmark. [ERROR_CODE=$ERROR_CODE]. Manually Update Bookmark before next run or reprocess!"
    sudo -E -u $ETL_USER _END_PROCESS $RUN_ID $ERROR_CODE
    sudo -E -u $ETL_USER _FREE_LOCK $HWW_TRANS_BKG_LOCK_NAME
    exit 1
  fi
  sudo -E -u $ETL_USER _LOG "Updated Bookmark to [$END_DT]"
fi

sudo -E -u $ETL_USER _END_PROCESS $RUN_ID $ERROR_CODE
sudo -E -u $ETL_USER _FREE_LOCK $HWW_TRANS_BKG_LOCK_NAME

sudo -E -u $ETL_USER _LOG "Job completed successfully"

