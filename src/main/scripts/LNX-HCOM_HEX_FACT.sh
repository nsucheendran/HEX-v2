#!/bin/bash
#
# LNX-HCOM_HEX_FACT.sh
# Wrapper to load hex fact staging and final fact data for HEX
#
# Usage:
#  LNX-HCOM_HEX_FACT.sh
#
# Error codes:
#   0 Success
#   1 General error
#
# user          date            comment
# ############# ############### ####################
# achadha       2013-10-21      Wrapper to incrementally load/reprocess Fact Staging and Fact data 

set -m
export PLAT_HOME=/usr/local/edw/platform
export HWW_HOME=/usr/etl/HWW
SCRIPT_PATH=$HWW_HOME/hdp_hww_hex_etl/hql/FACT
SCRIPT_PATH_REP=$HWW_HOME/hdp_hww_hex_etl/hql/REP
HEX_LOGS=/usr/etl/HWW/log

source $PLAT_HOME/common/sh_helpers.sh
source $PLAT_HOME/common/sh_metadata_storage.sh

HWW_LOCK_NAME="hdp_hww_hex_fact.lock"
MESSAGE="LNX-HCOM_HEX_FACT.sh failed: Previous script still running"
_ACQUIRE_LOCK $HWW_LOCK_NAME "$MESSAGE" 30

STAGE_NAME="HEX_FACT_STAGE: HEX Fact Staging"
_LOG_START_STAGE "$STAGE_NAME"

PROCESS_NAME="ETL_HCOM_HEX_FACT"
_LOG "PROCESS_NAME=[$PROCESS_NAME]"

ERROR_CODE=0

PROCESS_ID=$(_GET_PROCESS_ID "$PROCESS_NAME");
RETURN_CODE="$?"

if [ "$PROCESS_ID" == "" ] || (( $RETURN_CODE != 0 )); then
  _LOG "ERROR: Process [$PROCESS_NAME] does not exist in HEMS"
  ERROR_CODE=1
  _FREE_LOCK $HWW_LOCK_NAME
  exit 1;
else
  RUN_ID=$(_RUN_PROCESS $PROCESS_ID "$PROCESS_NAME")
  _LOG "PROCESS_ID=[$PROCESS_ID]"
  _LOG "RUN_ID=[$RUN_ID]"
  _LOG_PROCESS_DETAIL $RUN_ID "Started" "$ERROR_CODE"
fi

STAGE_TABLE=`_READ_PROCESS_CONTEXT $PROCESS_ID "FACT_STAGE_TABLE"`
ACTIVE_FAH_TABLE=`_READ_PROCESS_CONTEXT $PROCESS_ID "ACTIVE_FAH_TABLE"`
REPORT_TABLE=`_READ_PROCESS_CONTEXT $PROCESS_ID "REPORT_TABLE"`
REPORT_FILE=`_READ_PROCESS_CONTEXT $PROCESS_ID "REPORT_FILE"`
STAGE_DB=`_READ_PROCESS_CONTEXT $PROCESS_ID "STAGE_DB"`
JOB_QUEUE=`_READ_PROCESS_CONTEXT $PROCESS_ID "JOB_QUEUE"`
SRC_BOOKMARK_OMNI=`_READ_PROCESS_CONTEXT $PROCESS_ID "SRC_BOOKMARK_OMNI"`
SRC_BOOKMARK_BKG=`_READ_PROCESS_CONTEXT $PROCESS_ID "SRC_BOOKMARK_BKG"`
PROCESSING_TYPE=`_READ_PROCESS_CONTEXT $PROCESS_ID "PROCESSING_TYPE"`

if [ "${SRC_BOOKMARK_OMNI}" \< "${SRC_BOOKMARK_BKG}" ]
then
  MIN_SRC_BOOKMARK=$SRC_BOOKMARK_OMNI
else
  MIN_SRC_BOOKMARK=$SRC_BOOKMARK_BKG
fi

_LOG "PROCESSING_TYPE=$PROCESSING_TYPE"
_LOG "LAST_DT=$LAST_DT"


LOG_FILE_NAME="hdp_hex_fact_populate_reporting_table_${SRC_BOOKMARK_OMNI}-${SRC_BOOKMARK_BKG}.log"
_LOG "loading reporting requirements table $REPORT_TABLE"
hive -hiveconf hex.report.file="${REPORT_FILE}" -hiveconf mapred.job.queue.name="${JOB_QUEUE}" -hiveconf hex.db="${STAGE_DB}" -hiveconf hex.report.table="${REPORT_TABLE}" -f $SCRIPT_PATH_REP/createTable_HEX_REPORTING_REQUIREMENTS.hql >> $HEX_LOGS/$LOG_FILE_NAME 2>&1
ERROR_CODE=$?
if [[ $ERROR_CODE -ne 0 ]]; then
  _LOG "HEX_FACT_STAGE: Reporting table load FAILED [ERROR_CODE=$ERROR_CODE]. See [$HEX_LOGS/$LOG_FILE_NAME] for more information."
  _END_PROCESS $RUN_ID $ERROR_CODE
  _FREE_LOCK $HWW_LOCK_NAME
  exit 1
fi

hive -hiveconf min_src_bookmark="${MIN_SRC_BOOKMARK}" -hiveconf mapred.job.queue.name="${JOB_QUEUE}" -hiveconf hex.db="${STAGE_DB}" -hiveconf hex.report.table="${REPORT_TABLE}" -f $SCRIPT_PATH_REP/insert_HEX_REPORTING_REQUIREMENTS.hql >> $HEX_LOGS/$LOG_FILE_NAME 2>&1
ERROR_CODE=$?
if [[ $ERROR_CODE -ne 0 ]]; then
  _LOG "HEX_FACT_STAGE: Reporting table load FAILED [ERROR_CODE=$ERROR_CODE]. See [$HEX_LOGS/$LOG_FILE_NAME] for more information."
  _END_PROCESS $RUN_ID $ERROR_CODE
  _FREE_LOCK $HWW_LOCK_NAME
  exit 1
fi
_LOG "Done loading reporting requirements table $REPORT_TABLE"

exit 0;

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
    LOG_FILE_NAME="hdp_hex_fact_drop_partition_${CURR_YEAR}-${CURR_MONTH}.log"
    _LOG "Dropping partition [$CURR_YEAR-$CURR_MONTH] from target: $DB.$TABLE"
    hive -hiveconf part.year="${CURR_YEAR}" -hiveconf part.month="${CURR_MONTH}" -hiveconf job.queue="${JOB_QUEUE}" -hiveconf hex.db="${DB}" -hiveconf hex.table="${TABLE}" -f $SCRIPT_PATH_TRANS/delete_ETL_HCOM_HEX_FACT_STAGE.hql >> $HEX_LOGS/$LOG_FILE_NAME 2>&1
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
    LOG_FILE_NAME="hdp_hex_fact_reprocess_${START_DT}-${END_DT}.log"

    _LOG "Reprocessing Booking Fact Staging data between [$START_DT to $END_DT] in target: $DB.$TABLE"

    hive -hiveconf into.overwrite="overwrite" -hiveconf month="${MONTH}" -hiveconf start.date="${START_DT}" -hiveconf end.date="${END_DT}" -hiveconf job.queue="${JOB_QUEUE}" -hiveconf hex.db="${DB}" -hiveconf hex.table="${TABLE}" -f $SCRIPT_PATH/insert_ETL_HCOM_HEX_FACT_STAGE.hql >> $HEX_LOGS/$LOG_FILE_NAME 2>&1 
    ERROR_CODE=$?
    if [[ $ERROR_CODE -ne 0 ]]; then
      _LOG "HEX_FACT_STAGE: Booking Fact Staging load FAILED [ERROR_CODE=$ERROR_CODE]. See [$HEX_LOGS/$LOG_FILE_NAME] for more information."
      _END_PROCESS $RUN_ID $ERROR_CODE
      _FREE_LOCK $HWW_LOCK_NAME
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
  _LOG "Setting PROCESSING_TYPE to [D] for next run"
  _WRITE_PROCESS_CONTEXT "$PROCESS_ID" "PROCESSING_TYPE" "D"
else
  # daily incremental load
  _LOG "Incremental Booking Fact Staging data load (SRC_BOOKMARK_OMNI=[$SRC_BOOKMARK_OMNI], SRC_BOOKMARK_BKG=[$SRC_BOOKMARK_BKG])"
  #START_DT=`date --date="${LAST_DT} +1 days" '+%Y-%m-%d'`
  #END_DT=$START_DT

  LOG_FILE_NAME="hdp_hex_fact_stage_incremental_${SRC_BOOKMARK_OMNI}-${SRC_BOOKMARK_BKG}.log"
  MONTH=`date --date="${START_DT}" '+%Y-%m'`

  hive -hiveconf into.overwrite="into" -hiveconf month="${MONTH}" -hiveconf start.date="${START_DT}" -hiveconf end.date="${END_DT}" -hiveconf job.queue="${JOB_QUEUE}" -hiveconf hex.db="${DB}" -hiveconf hex.table="${TABLE}" -f $SCRIPT_PATH/insert_ETL_HCOM_HEX_FACT_STAGE.hql >> $HEX_LOGS/$LOG_FILE_NAME 2>&1 
  ERROR_CODE=$?
  if [[ $ERROR_CODE -ne 0 ]]; then
    _LOG "HEX_FACT_STAGE: Booking Fact Staging load FAILED [ERROR_CODE=$ERROR_CODE]. See [$HEX_LOGS/$LOG_FILE_NAME] for more information."
    _END_PROCESS $RUN_ID $ERROR_CODE
    _FREE_LOCK $HWW_LOCK_NAME
    exit 1
  fi

  _WRITE_PROCESS_CONTEXT "$PROCESS_ID" "BOOKMARK" "$END_DT"
  if [[ $ERROR_CODE -ne 0 ]]; then
    _LOG "HEMS ERROR! Unable to update bookmark. [ERROR_CODE=$ERROR_CODE]. Manually Update Bookmark before next run or reprocess!"
    _END_PROCESS $RUN_ID $ERROR_CODE
    _FREE_LOCK $HWW_LOCK_NAME
    exit 1
  fi
  _LOG "Updated Bookmark to [$END_DT]"
fi

_END_PROCESS $RUN_ID $ERROR_CODE
_FREE_LOCK $HWW_LOCK_NAME

_LOG "Job completed successfully"

