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

FAH_PROCESS_ID=`_READ_PROCESS_CONTEXT $PROCESS_ID "FAH_PROCESS_ID"`
FAH_BOOKMARK_DATE=`_READ_PROCESS_CONTEXT $FAH_PROCESS_ID "BOOKMARK"`
FAH_BOOKMARK_DATE=`date --date="$FAH_BOOKMARK_DATE" '+%Y-%m-%d'`

BKG_PROCESS_ID=`_READ_PROCESS_CONTEXT $PROCESS_ID "BKG_PROCESS_ID"`
BKG_BOOKMARK_DATE=`_READ_PROCESS_CONTEXT $BKG_PROCESS_ID "BOOKMARK"`

if [ "${SRC_BOOKMARK_OMNI}" \< "${SRC_BOOKMARK_BKG}" ]
then
  MIN_SRC_BOOKMARK=$SRC_BOOKMARK_OMNI
else
  MIN_SRC_BOOKMARK=$SRC_BOOKMARK_BKG
fi

if [ "${SRC_BOOKMARK_OMNI}" \< "${SRC_BOOKMARK_BKG}" ]
then
  MAX_SRC_BOOKMARK=$SRC_BOOKMARK_BKG
else
  MAX_SRC_BOOKMARK=$SRC_BOOKMARK_OMNI
fi

_LOG "PROCESSING_TYPE=$PROCESSING_TYPE"
_LOG "MIN_SRC_BOOKMARK=$MIN_SRC_BOOKMARK"
_LOG "MAX_SRC_BOOKMARK=$MAX_SRC_BOOKMARK"

LOG_FILE_NAME="hdp_hex_fact_populate_reporting_table_${SRC_BOOKMARK_OMNI}-${SRC_BOOKMARK_BKG}.log"
_LOG "loading raw reporting requirements table ${REPORT_TABLE}_RAW"
hive -hiveconf hex.report.file="${REPORT_FILE}" -hiveconf mapred.job.queue.name="${JOB_QUEUE}" -hiveconf hex.db="${STAGE_DB}" -hiveconf hex.report.table="${REPORT_TABLE}" -f $SCRIPT_PATH_REP/createTable_HEX_REPORTING_REQUIREMENTS.hql >> $HEX_LOGS/$LOG_FILE_NAME 2>&1
ERROR_CODE=$?
if [[ $ERROR_CODE -ne 0 ]]; then
  _LOG "HEX_FACT_STAGE: Reporting table load FAILED [ERROR_CODE=$ERROR_CODE]. See [$HEX_LOGS/$LOG_FILE_NAME] for more information."
  _END_PROCESS $RUN_ID $ERROR_CODE
  _FREE_LOCK $HWW_LOCK_NAME
  exit 1
fi
_LOG "Done loading raw reporting requirements table ${REPORT_TABLE}_RAW"

_LOG "loading reporting requirements table $REPORT_TABLE"
hive -hiveconf min_src_bookmark="${MIN_SRC_BOOKMARK}" -hiveconf mapred.job.queue.name="${JOB_QUEUE}" -hiveconf hex.db="${STAGE_DB}" -hiveconf hex.report.table="${REPORT_TABLE}" -f $SCRIPT_PATH_REP/insert_HEX_REPORTING_REQUIREMENTS.hql >> $HEX_LOGS/$LOG_FILE_NAME 2>&1
ERROR_CODE=$?
if [[ $ERROR_CODE -ne 0 ]]; then
  _LOG "HEX_FACT_STAGE: Reporting table load FAILED [ERROR_CODE=$ERROR_CODE]. See [$HEX_LOGS/$LOG_FILE_NAME] for more information."
  _END_PROCESS $RUN_ID $ERROR_CODE
  _FREE_LOCK $HWW_LOCK_NAME
  exit 1
fi
_LOG "Done loading reporting requirements table $REPORT_TABLE"


if [ $PROCESSING_TYPE = "R" ];
then
  START_YEAR=`_READ_PROCESS_CONTEXT $PROCESS_ID "REPROCESS_START_YEAR"`
  START_MONTH=`_READ_PROCESS_CONTEXT $PROCESS_ID "REPROCESS_START_MONTH"`

  END_YEAR=`date --date="${MAX_SRC_BOOKMARK}" '+%Y'`
  END_MONTH=`date --date="${MAX_SRC_BOOKMARK}" '+%m'`

  _LOG "Starting Reprocessing for period: $START_YEAR-$START_MONTH to $END_YEAR-$END_MONTH"

  CURR_YEAR=$START_YEAR
  CURR_MONTH=$START_MONTH
  while [ "${CURR_YEAR}${CURR_MONTH}" \< "${END_YEAR}${END_MONTH}" -o "${CURR_YEAR}${CURR_MONTH}" = "${END_YEAR}${END_MONTH}" ]
  do
    LOG_FILE_NAME="hdp_hex_fact_drop_partition_${CURR_YEAR}-${CURR_MONTH}.log"
    _LOG "Dropping partition [year_month='$CURR_YEAR-$CURR_MONTH', source='omniture'] from target: $STAGE_DB.$STAGE_TABLE"
    hive -hiveconf mapred.job.queue.name="${JOB_QUEUE}" -e "alter table ${STAGE_DB}.${STAGE_TABLE} drop if exists partition (year_month='${CURR_YEAR}-${CURR_MONTH}', source='omniture')" >> $HEX_LOGS/$LOG_FILE_NAME 2>&1
    ERROR_CODE=$?
    if [[ $ERROR_CODE -ne 0 ]]; then
      _LOG "ERROR while dropping partition [ERROR_CODE=$ERROR_CODE]. See [$HEX_LOGS/$LOG_FILE_NAME] for more information."
      _END_PROCESS $RUN_ID $ERROR_CODE
      _FREE_LOCK $HWW_LOCK_NAME
      exit 1
    fi
    
    _LOG "Dropping partition [year_month='$CURR_YEAR-$CURR_MONTH', source='booking'] from target: $STAGE_DB.$STAGE_TABLE"
    hive -hiveconf mapred.job.queue.name="${JOB_QUEUE}" -e "alter table ${STAGE_DB}.${STAGE_TABLE} drop if exists partition (year_month='${CURR_YEAR}-${CURR_MONTH}', source='booking')" >> $HEX_LOGS/$LOG_FILE_NAME 2>&1
    ERROR_CODE=$?
    if [[ $ERROR_CODE -ne 0 ]]; then
      _LOG "ERROR while dropping partition [ERROR_CODE=$ERROR_CODE]. See [$HEX_LOGS/$LOG_FILE_NAME] for more information."
      _END_PROCESS $RUN_ID $ERROR_CODE
      _FREE_LOCK $HWW_LOCK_NAME
      exit 1
    fi

    NEW_YEAR=`date --date="${CURR_YEAR}-${CURR_MONTH}-01 00 +1 months" '+%Y'`
    CURR_MONTH=`date --date="${CURR_YEAR}-${CURR_MONTH}-01 00 +1 months" '+%m'`
    CURR_YEAR=$NEW_YEAR
  done
  
  NEW_BOOKMARK=`date --date="${START_YEAR}-${START_MONTH}-01 00 -1 days" '+%Y-%m-%d'`
  _WRITE_PROCESS_CONTEXT "$PROCESS_ID" "SRC_BOOKMARK_OMNI" "$NEW_BOOKMARK"
  if [[ $ERROR_CODE -ne 0 ]]; then
    _LOG "HEMS ERROR! Unable to update bookmark. [ERROR_CODE=$ERROR_CODE]. Manually Update Bookmark before next run or reprocess!"
    _END_PROCESS $RUN_ID $ERROR_CODE
    _FREE_LOCK $HWW_LOCK_NAME
    exit 1
  fi
  _LOG "Updated Omniture source bookmark to to [$NEW_BOOKMARK]"
  
   _WRITE_PROCESS_CONTEXT "$PROCESS_ID" "SRC_BOOKMARK_BKG" "$NEW_BOOKMARK"
  if [[ $ERROR_CODE -ne 0 ]]; then
    _LOG "HEMS ERROR! Unable to update bookmark. [ERROR_CODE=$ERROR_CODE]. Manually Update Bookmark before next run or reprocess!"
    _END_PROCESS $RUN_ID $ERROR_CODE
    _FREE_LOCK $HWW_LOCK_NAME
    exit 1
  fi
  _LOG "Updated Transactions source bookmark to to [$NEW_BOOKMARK]"
  
  _LOG "Done Reprocessing"

  _LOG "Setting PROCESSING_TYPE to [D] for next run"
  _WRITE_PROCESS_CONTEXT "$PROCESS_ID" "PROCESSING_TYPE" "D"
else
  # daily incremental load
  _LOG "Incremental Booking Fact Staging data load (SRC_BOOKMARK_OMNI=[$SRC_BOOKMARK_OMNI], SRC_BOOKMARK_BKG=[$SRC_BOOKMARK_BKG], MIN_SRC_BOOKMARK=[$MIN_SRC_BOOKMARK])"
  
  LOG_FILE_NAME="hdp_hex_fact_stage_active_hits_${SRC_BOOKMARK_OMNI}-${SRC_BOOKMARK_BKG}.log"
  #MONTH=`date --date="${START_DT}" '+%Y-%m'`

  MIN_REPORT_DATE=`hive -hiveconf mapred.job.queue.name="${JOB_QUEUE}" -e "select min(report_start_date) from ${STAGE_DB}.${REPORT_TABLE};"`
  MIN_REPORT_DATE_YM=`date --date="${MIN_REPORT_DATE}" '+%Y-%m'`
  
  
  MAX_REPORT_DATE=`hive -hiveconf mapred.job.queue.name="${JOB_QUEUE}" -e "select max(report_end_date) from ${STAGE_DB}.${REPORT_TABLE};"`
  if [ "${FAH_BOOKMARK_DATE}" \< "${MAX_REPORT_DATE}" ]
  then 
    MAX_OMNI_DATE=${FAH_BOOKMARK_DATE}
  else
    MAX_OMNI_DATE=${MAX_REPORT_DATE}
  fi
  MAX_OMNI_DATE_YM=`date --date="$MAX_OMNI_DATE" '+%Y-%m'`
  
  
  
  _LOG "MIN_REPORT_DATE=$MIN_REPORT_DATE, MIN_REPORT_DATE_YM=$MIN_REPORT_DATE_YM, MAX_OMNI_DATE=$MAX_OMNI_DATE, MAX_OMNI_DATE_YM=$MAX_OMNI_DATE_YM"
  
  _LOG "loading first assignment hits for active reporting requirements into $ACTIVE_FAH_TABLE ..."
  hive -hiveconf max_omniture_record_yr_month="${MAX_OMNI_DATE_YM}" -hiveconf max_omniture_record_date="${MAX_OMNI_DATE}" -hiveconf min_report_date="${MIN_REPORT_DATE}" -hiveconf min_report_date_yrmonth="${MIN_REPORT_DATE_YM}" -hiveconf hex.rep.table="${REPORT_TABLE}" -hiveconf job.queue="${JOB_QUEUE}" -hiveconf hex.db="${STAGE_DB}" -hiveconf hex.table="${ACTIVE_FAH_TABLE}" -f $SCRIPT_PATH/insertTable_ETL_HCOM_HEX_ACTIVE_FIRST_ASSIGNMENT_HITS.hql >> $HEX_LOGS/$LOG_FILE_NAME 2>&1 
  ERROR_CODE=$?
  if [[ $ERROR_CODE -ne 0 ]]; then
    _LOG "HEX_FACT_STAGE: Booking Fact Staging load FAILED [ERROR_CODE=$ERROR_CODE]. See [$HEX_LOGS/$LOG_FILE_NAME] for more information."
    _END_PROCESS $RUN_ID $ERROR_CODE
    _FREE_LOCK $HWW_LOCK_NAME
    exit 1
  fi
  _LOG "Done loading first assignment hits for active reporting requirements into $ACTIVE_FAH_TABLE"

  _LOG "loading incremental first_assignment_hits into $STAGE_TABLE ..."
  hive -hiveconf src_bookmark_omni="${SRC_BOOKMARK_OMNI}" -hiveconf hex.active.hits.table="${ACTIVE_FAH_TABLE}" -hiveconf job.queue="${JOB_QUEUE}" -hiveconf hex.db="${STAGE_DB}" -hiveconf hex.table="${STAGE_TABLE}" -f $SCRIPT_PATH/insertTable_ETL_HCOM_HEX_FACT_STAGE_OMNITURE.hql >> $HEX_LOGS/$LOG_FILE_NAME 2>&1 
  ERROR_CODE=$?
  if [[ $ERROR_CODE -ne 0 ]]; then
    _LOG "HEX_FACT_STAGE: Booking Fact Staging load FAILED [ERROR_CODE=$ERROR_CODE]. See [$HEX_LOGS/$LOG_FILE_NAME] for more information."
    _END_PROCESS $RUN_ID $ERROR_CODE
    _FREE_LOCK $HWW_LOCK_NAME
    exit 1
  fi
  _LOG "Done loading incremental first_assignment_hits into $STAGE_TABLE"

  if [ "${BKG_BOOKMARK_DATE}" \< "${MAX_REPORT_DATE}" ]
  then 
    MAX_BKG_DATE=${BKG_BOOKMARK_DATE}
  else
    MAX_BKG_DATE=${MAX_REPORT_DATE}
  fi
  
  if [ "${MAX_BKG_DATE}" \< "${MAX_OMNI_DATE}" ]
  then
    MAX_TRANS_DATE=${MAX_OMNI_DATE}
  else
    MAX_TRANS_DATE=${MAX_BKG_DATE}
  fi
  MAX_TRANS_YM=`date --date="${MAX_TRANS_DATE}" '+%Y-%m'`
  
  _LOG "loading incremental booking data into $STAGE_TABLE ..."
  hive -hiveconf max_trans_record_date_yr_month="${MAX_TRANS_DATE}" -hiveconf max_booking_record_date="${MAX_BKG_DATE}" -hiveconf max_omniture_record_date="${MAX_OMNI_DATE}" -hiveconf min_report_date="${MIN_REPORT_DATE}" -hiveconf min_report_date_yrmonth="${MIN_REPORT_DATE_YM}" -hiveconf min_src_bookmark="${MIN_SRC_BOOKMARK}" -hiveconf src_bookmark_bkg="${SRC_BOOKMARK_BKG}" -hiveconf src_bookmark_omni="${SRC_BOOKMARK_OMNI}" -hiveconf hex.active.hits.table="${ACTIVE_FAH_TABLE}" -hiveconf job.queue="${JOB_QUEUE}" -hiveconf hex.db="${STAGE_DB}" -hiveconf hex.table="${STAGE_TABLE}" -f $SCRIPT_PATH/insertTable_ETL_HCOM_HEX_FACT_STAGE_BOOKING.hql >> $HEX_LOGS/$LOG_FILE_NAME 2>&1 
  ERROR_CODE=$?
  if [[ $ERROR_CODE -ne 0 ]]; then
    _LOG "HEX_FACT_STAGE: Booking Fact Staging load FAILED [ERROR_CODE=$ERROR_CODE]. See [$HEX_LOGS/$LOG_FILE_NAME] for more information."
    _END_PROCESS $RUN_ID $ERROR_CODE
    _FREE_LOCK $HWW_LOCK_NAME
    exit 1
  fi
  _LOG "Done loading incremental booking data into $STAGE_TABLE"
  

  _WRITE_PROCESS_CONTEXT "$PROCESS_ID" "SRC_BOOKMARK_OMNI" "$FAH_BOOKMARK_DATE"
  if [[ $ERROR_CODE -ne 0 ]]; then
    _LOG "HEMS ERROR! Unable to update bookmark. [ERROR_CODE=$ERROR_CODE]. Manually Update Bookmark before next run or reprocess!"
    _END_PROCESS $RUN_ID $ERROR_CODE
    _FREE_LOCK $HWW_LOCK_NAME
    exit 1
  fi
  _LOG "Updated Omniture source bookmark to to [$FAH_BOOKMARK_DATE]"
  
   _WRITE_PROCESS_CONTEXT "$PROCESS_ID" "SRC_BOOKMARK_BKG" "$BKG_BOOKMARK_DATE"
  if [[ $ERROR_CODE -ne 0 ]]; then
    _LOG "HEMS ERROR! Unable to update bookmark. [ERROR_CODE=$ERROR_CODE]. Manually Update Bookmark before next run or reprocess!"
    _END_PROCESS $RUN_ID $ERROR_CODE
    _FREE_LOCK $HWW_LOCK_NAME
    exit 1
  fi
  _LOG "Updated Transactions source bookmark to to [$BKG_BOOKMARK_DATE]"
fi

_END_PROCESS $RUN_ID $ERROR_CODE
_FREE_LOCK $HWW_LOCK_NAME

_LOG "Job completed successfully"

