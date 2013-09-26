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

sudo -E -u $ETL_USER source $SCRIPT_PATH_TOOLS/helper_1_LNX-HCOM_HEX_TRANSACTION_BKG.sh
if [ $PROCESSING_TYPE = "R" ];
then
  sudo -E -u $ETL_USER source $SCRIPT_PATH_TOOLS/helper_2_LNX-HCOM_HEX_TRANSACTION_BKG.sh
  while [ "${CURR_YEAR}${CURR_MONTH}" \< "${END_YEAR}${END_MONTH}" -o "${CURR_YEAR}${CURR_MONTH}" = "${END_YEAR}${END_MONTH}" ]
  do
    sudo -E -u $ETL_USER source $SCRIPT_PATH_TOOLS/helper_3_LNX-HCOM_HEX_TRANSACTION_BKG.sh

    $SCRIPT_PATH_TOOLS/etldm_hcom_bkg_order_xref_hex.sh "$START_DT" "$END_DT"
    ERROR_CODE=$?
    if [[ $ERROR_CODE -ne 0 ]]; then
      sudo -E -u $ETL_USER source $SCRIPT_PATH_TOOLS/helper_4_LNX-HCOM_HEX_TRANSACTION_BKG.sh
      exit 1
    fi
    hive -hiveconf into.overwrite="insert" -hiveconf start.date="${START_DT}" -hiveconf end.date="${END_DT}" -hiveconf job.queue="${JOB_QUEUE}" -hiveconf hex.fah.db="${TRANS_BKG_DB}" -hiveconf hex.trans.table="${TRANS_BKG_TABLE}" -f $SCRIPT_PATH_R3/insert_ETL_HEX_TRANSACTIONS_BOOKING.hql >> $HEX_LOGS/$LOG_FILE_NAME 2>&1 
    ERROR_CODE=$?
    if [[ $ERROR_CODE -ne 0 ]]; then
      sudo -E -u $ETL_USER source $SCRIPT_PATH_TOOLS/helper_4_LNX-HCOM_HEX_TRANSACTION_BKG.sh
      exit 1
    fi

    NEW_YEAR=`date --date="${CURR_YEAR}-${CURR_MONTH}-01 +1 months" '+%Y'`
    CURR_MONTH=`date --date="${CURR_YEAR}-${CURR_MONTH}-01 +1 months" '+%m'`
    CURR_YEAR=$NEW_YEAR
  done
  
  sudo -E -u $ETL_USER source $SCRIPT_PATH_TOOLS/helper_5_LNX-HCOM_HEX_TRANSACTION_BKG.sh
else
  # daily incremental load

  _LOG "Incremental Booking Transactions data load (BOOKMARK=[$LAST_DT])"
  START_DT=`date --date="${LAST_DT} +1 days" '+%Y-%m-%d'`
  END_DT=$START_DT

  $SCRIPT_PATH_TOOLS/etldm_hcom_bkg_order_xref_hex.sh "$START_DT" "$END_DT"
  ERROR_CODE=$?
  if [[ $ERROR_CODE -ne 0 ]]; then
    _LOG "R3: Booking Transactions load FAILED [ERROR_CODE=$ERROR_CODE]. [see $HEX_LOGS/$LOG_FILE_NAME] for more information."
    _END_PROCESS $RUN_ID $ERROR_CODE
    _FREE_LOCK $HWW_TRANS_BKG_LOCK_NAME
    exit 1
  fi
  hive -hiveconf into.overwrite="insert" -hiveconf start.date="${START_DT}" -hiveconf end.date="${END_DT}" -hiveconf job.queue="${JOB_QUEUE}" -hiveconf hex.fah.db="${TRANS_BKG_DB}" -hiveconf hex.trans.table="${TRANS_BKG_TABLE}" -f $SCRIPT_PATH_R3/insert_ETL_HEX_TRANSACTIONS_BOOKING.hql >> $HEX_LOGS/$LOG_FILE_NAME 2>&1 
  ERROR_CODE=$?
  if [[ $ERROR_CODE -ne 0 ]]; then
    _LOG "R3: Booking Transactions load FAILED [ERROR_CODE=$ERROR_CODE]. [see $HEX_LOGS/$LOG_FILE_NAME] for more information."
    _END_PROCESS $RUN_ID $ERROR_CODE
    _FREE_LOCK $HWW_TRANS_BKG_LOCK_NAME
    exit 1
  fi

  _WRITE_PROCESS_CONTEXT "$PROCESS_ID" "BOOKMARK" "$END_DT"
  if [[ $ERROR_CODE -ne 0 ]]; then
    _LOG "HEMS ERROR! Unable to update bookmark. [ERROR_CODE=$ERROR_CODE]. Manually Update Bookmark before next run or reprocess!"
    _END_PROCESS $RUN_ID $ERROR_CODE
    _FREE_LOCK $HWW_TRANS_BKG_LOCK_NAME
    exit 1
  fi
  _LOG "Updated Bookmark to [$END_DT]"
fi

_END_PROCESS $RUN_ID $ERROR_CODE
_FREE_LOCK $HWW_TRANS_BKG_LOCK_NAME

_LOG "Job completed successfully"

