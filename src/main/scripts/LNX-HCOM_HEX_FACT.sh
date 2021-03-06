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
SCRIPT_PATH_AGG=$HWW_HOME/hdp_hww_hex_etl/hql/AGG
SCRIPT_PATH_SEG=$HWW_HOME/hdp_hww_hex_etl/hql/SEG
SCRIPT_PATH_DB2=$HWW_HOME/hdp_hww_hex_etl/sql
SCRIPT_PATH_PARTED=$HWW_HOME/hdp_hww_hex_etl/tools
HEX_LOGS=/usr/etl/HWW/log

source $PLAT_HOME/common/sh_helpers.sh
source $PLAT_HOME/common/sh_metadata_storage.sh

HWW_LOCK_NAME="hdp_hww_hex_fact.lock"
MESSAGE="LNX-HCOM_HEX_FACT.sh failed: Previous script still running"
_ACQUIRE_LOCK $HWW_LOCK_NAME "$MESSAGE" 30

STAGE_NAME="HEX_FACT_STAGE: HEX Fact Staging"
_LOG_START_STAGE "$STAGE_NAME"

PROCESS_NAME="ETL_HCOM_HEX_FACT"
_LOG "PROCESS_NAME=[$PROCESS_NAME]" $HEX_LOGS/LNX-HCOM_HEX_FACT.log

ERROR_CODE=0

PROCESS_ID=$(_GET_PROCESS_ID "$PROCESS_NAME");
RETURN_CODE="$?"

if [ "$PROCESS_ID" == "" ] || (( $RETURN_CODE != 0 )); then
  _LOG "ERROR: Process [$PROCESS_NAME] does not exist in HEMS" $HEX_LOGS/LNX-HCOM_HEX_FACT.log
  ERROR_CODE=1
  _FREE_LOCK $HWW_LOCK_NAME
  exit 1;
else
  RUN_ID=$(_RUN_PROCESS $PROCESS_ID "$PROCESS_NAME")
  _LOG "PROCESS_ID=[$PROCESS_ID]" $HEX_LOGS/LNX-HCOM_HEX_FACT.log
  _LOG "RUN_ID=[$RUN_ID]" $HEX_LOGS/LNX-HCOM_HEX_FACT.log
  _LOG_PROCESS_DETAIL $RUN_ID "STATUS" "STARTED"
fi

STAGE_TABLE=`_READ_PROCESS_CONTEXT $PROCESS_ID "FACT_STAGE_TABLE"`
ACTIVE_FAH_TABLE=`_READ_PROCESS_CONTEXT $PROCESS_ID "ACTIVE_FAH_TABLE"`
REPORT_TABLE=`_READ_PROCESS_CONTEXT $PROCESS_ID "REPORT_TABLE"`
REPORT_FILE=`_READ_PROCESS_CONTEXT $PROCESS_ID "REPORT_FILE"`
STAGE_DB=`_READ_PROCESS_CONTEXT $PROCESS_ID "STAGE_DB"`
AGG_DB=`_READ_PROCESS_CONTEXT $PROCESS_ID "AGG_DB"`
JOB_QUEUE=`_READ_PROCESS_CONTEXT $PROCESS_ID "JOB_QUEUE"`
SRC_BOOKMARK_OMNI_FULL=`_READ_PROCESS_CONTEXT $PROCESS_ID "SRC_BOOKMARK_OMNI"`
SRC_BOOKMARK_BKG=`_READ_PROCESS_CONTEXT $PROCESS_ID "SRC_BOOKMARK_BKG"`
PROCESSING_TYPE=`_READ_PROCESS_CONTEXT $PROCESS_ID "PROCESSING_TYPE"`
FACT_REDUCERS=`_READ_PROCESS_CONTEXT $PROCESS_ID "FACT_REDUCERS"`
FACT_LOAD_SPLIT_SIZE=`_READ_PROCESS_CONTEXT $PROCESS_ID "FACT_LOAD_SPLIT_SIZE"`
FACT_TABLE=`_READ_PROCESS_CONTEXT $PROCESS_ID "FACT_TABLE"`
FACT_TABLE_UNPARTED=`_READ_PROCESS_CONTEXT $PROCESS_ID "FACT_TABLE_UNPARTED"`
JAR_PATH=`_READ_PROCESS_CONTEXT $PROCESS_ID "JAR_PATH"`
AGG_TABLE=`_READ_PROCESS_CONTEXT $PROCESS_ID "AGG_TABLE"`
SUP_MAP_TABLE=`_READ_PROCESS_CONTEXT $PROCESS_ID "SUP_MAP_TABLE"`
FACT_AGG_UNPARTED_TABLE=`_READ_PROCESS_CONTEXT $PROCESS_ID "FACT_AGG_UNPARTED_TABLE"`
KEYS_COUNT_LIMIT=`_READ_PROCESS_CONTEXT $PROCESS_ID "KEYS_COUNT_LIMIT"`
AGG_NUM_REDUCERS=`_READ_PROCESS_CONTEXT $PROCESS_ID "AGG_NUM_REDUCERS"`
REP_BATCH_SIZE=`_READ_PROCESS_CONTEXT $PROCESS_ID "REP_BATCH_SIZE"`
SEG_NUM_REDUCERS=`_READ_PROCESS_CONTEXT $PROCESS_ID "SEG_NUM_REDUCERS"`
SEG_UNPARTED_TABLE=`_READ_PROCESS_CONTEXT $PROCESS_ID "SEG_UNPARTED_TABLE"`
SEG_INPUT_FILE_PATH=`_READ_PROCESS_CONTEXT $PROCESS_ID "SEG_INPUT_FILE_PATH"`
SEG_TABLE=`_READ_PROCESS_CONTEXT $PROCESS_ID "SEG_TABLE"`
SEG_EXP_LIST_TABLE=`_READ_PROCESS_CONTEXT $PROCESS_ID "SEG_EXP_LIST_TABLE"`
STEP_LOAD_REPORTING_REQUIREMENTS=`_READ_PROCESS_CONTEXT $PROCESS_ID "STEP_LOAD_REPORTING_REQUIREMENTS"`
STEP_LOAD_STAGING_DATA=`_READ_PROCESS_CONTEXT $PROCESS_ID "STEP_LOAD_STAGING_DATA"`
STEP_LOAD_FACT_DATA=`_READ_PROCESS_CONTEXT $PROCESS_ID "STEP_LOAD_FACT_DATA"`
STEP_LOAD_AGG_DATA=`_READ_PROCESS_CONTEXT $PROCESS_ID "STEP_LOAD_AGG_DATA"`
STEP_LOAD_SEG_DATA=`_READ_PROCESS_CONTEXT $PROCESS_ID "STEP_LOAD_SEG_DATA"`
STEP_LOAD_DB2_DATA=`_READ_PROCESS_CONTEXT $PROCESS_ID "STEP_LOAD_DB2_DATA"`
STEP_LOAD_DB2_SP=`_READ_PROCESS_CONTEXT $PROCESS_ID "STEP_LOAD_DB2_SP"`
STEP_LOAD_PARTITIONED_DATA=`_READ_PROCESS_CONTEXT $PROCESS_ID "STEP_LOAD_PARTITIONED_DATA"`
STEP_TO_PROCESS_FROM=`_READ_PROCESS_CONTEXT $PROCESS_ID "STEP_TO_PROCESS_FROM"`
FAH_PROCESS_ID=`_READ_PROCESS_CONTEXT $PROCESS_ID "FAH_PROCESS_ID"`
FAH_BOOKMARK_DATE_FULL=`_READ_PROCESS_CONTEXT $FAH_PROCESS_ID "BOOKMARK"`
PARTED_SEG_LOAD=`_READ_PROCESS_CONTEXT $FAH_PROCESS_ID "PARTED_SEG_LOAD"`
PARTED_AGG_LOAD=`_READ_PROCESS_CONTEXT $FAH_PROCESS_ID "PARTED_AGG_LOAD"`
PARTED_FACT_LOAD=`_READ_PROCESS_CONTEXT $FAH_PROCESS_ID "PARTED_FACT_LOAD"`
EMAIL_TO=`_READ_PROCESS_CONTEXT $PROCESS_ID "EMAIL_TO"`
EMAIL_CC=`_READ_PROCESS_CONTEXT $PROCESS_ID "EMAIL_CC"`
EMAIL_SUCCESS_TO=`_READ_PROCESS_CONTEXT $PROCESS_ID "EMAIL_SUCCESS_TO"`
EMAIL_SUCCESS_CC=`_READ_PROCESS_CONTEXT $PROCESS_ID "EMAIL_SUCCESS_CC"`

EMAIL_RECIPIENTS=$EMAIL_TO
if [ $EMAIL_CC ]
then
  EMAIL_RECIPIENTS="-c $EMAIL_CC $EMAIL_RECIPIENTS"
fi

EMAIL_SUCCESS_RECIPIENTS=$EMAIL_SUCCESS_TO
if [ $EMAIL_SUCCESS_CC ]
then
  EMAIL_SUCCESS_RECIPIENTS="-c $EMAIL_SUCCESS_CC $EMAIL_SUCCESS_RECIPIENTS"
fi

FAH_BOOKMARK_DATE=`date --date="$FAH_BOOKMARK_DATE_FULL" '+%Y-%m-%d'`

BKG_PROCESS_ID=`_READ_PROCESS_CONTEXT $PROCESS_ID "BKG_PROCESS_ID"`
BKG_BOOKMARK_DATE=`_READ_PROCESS_CONTEXT $BKG_PROCESS_ID "BOOKMARK"`

SRC_BOOKMARK_OMNI=`date --date="${SRC_BOOKMARK_OMNI_FULL}" '+%Y-%m-%d'`
SRC_BOOKMARK_OMNI_HOUR=`date --date="${SRC_BOOKMARK_OMNI_FULL}" '+%H'`

#####################
# DB2 Load Variables
#####################
LOAD_DB2=`_READ_PROCESS_CONTEXT $PROCESS_ID "LOAD_DB2"`
TOGGLE_DB2=`_READ_PROCESS_CONTEXT $PROCESS_ID "TOGGLE_DB2"`

REP_REQ_SRC_HDFS_PATH=`_READ_PROCESS_CONTEXT $PROCESS_ID "REP_REQ_SRC_HDFS_PATH"`
REP_REQ_TGT_DB2_TABLE=`_READ_PROCESS_CONTEXT $PROCESS_ID "REP_REQ_TGT_DB2_TABLE"`
REP_REQ_INPUT_TYPE=`_READ_PROCESS_CONTEXT $PROCESS_ID "REP_REQ_INPUT_TYPE"`

SEG_SRC_HDFS_PATH=`_READ_PROCESS_CONTEXT $PROCESS_ID "SEG_SRC_HDFS_PATH"`
SEG_TGT_DB2_TABLE=`_READ_PROCESS_CONTEXT $PROCESS_ID "SEG_TGT_DB2_TABLE"`
SEG_INPUT_TYPE=`_READ_PROCESS_CONTEXT $PROCESS_ID "SEG_INPUT_TYPE"`

EXP_SRC_HDFS_PATH=`_READ_PROCESS_CONTEXT $PROCESS_ID "EXP_SRC_HDFS_PATH"`
EXP_TGT_DB2_TABLE=`_READ_PROCESS_CONTEXT $PROCESS_ID "EXP_TGT_DB2_TABLE"`
EXP_INPUT_TYPE=`_READ_PROCESS_CONTEXT $PROCESS_ID "EXP_INPUT_TYPE"`

LOADERPATH=/usr/etl/HWW/hdp_hww_hex_etl/tools
DB2LOGIN="/home/hwwetl/hexdbconf"
HDPENV="/home/hwwetl/hdpenv.conf"
LOADERSCRIPT=$LOADERPATH/HWW_pipeloader_str.bash

source $HDPENV
source $DB2LOGIN
export DB_NAME=$DBNAME
export DB_USER=$USERID
export DB_PASS=$PASSWD

if [ "$FAH_BOOKMARK_DATE_FULL" == "$SRC_BOOKMARK_OMNI_FULL" ] && [ "$BKG_BOOKMARK_DATE" == "$SRC_BOOKMARK_OMNI" ]
then
  echo -e "====================================================================================================================================================================\nHEX Foundation data for Omniture & Booking not updated from BOOKMARKs=[$FAH_BOOKMARK_DATE_FULL, $BKG_BOOKMARK_DATE].\n\nProcessing will continue & generate data for any new experiments in input file.\n\nScript Name : $0\n====================================================================================================================================================================\n" | mailx -s "HWW HEX Warning (ETL_HCOM_HEX_FACT): No incremental data in source to process (BOOKMARK Dates -[$FAH_BOOKMARK_DATE_FULL, $BKG_BOOKMARK_DATE])" $EMAIL_RECIPIENTS
fi

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

_LOG "PROCESSING_TYPE=$PROCESSING_TYPE" $HEX_LOGS/LNX-HCOM_HEX_FACT.log
_LOG "MIN_SRC_BOOKMARK=$MIN_SRC_BOOKMARK" $HEX_LOGS/LNX-HCOM_HEX_FACT.log
_LOG "MAX_SRC_BOOKMARK=$MAX_SRC_BOOKMARK" $HEX_LOGS/LNX-HCOM_HEX_FACT.log
_LOG "STEP_TO_PROCESS_FROM=$STEP_TO_PROCESS_FROM" $HEX_LOGS/LNX-HCOM_HEX_FACT.log


_LOG_PROCESS_DETAIL $RUN_ID "BEFORE_SRC_BOOKMARK_OMNI" "$SRC_BOOKMARK_OMNI_FULL"
_LOG_PROCESS_DETAIL $RUN_ID "BEFORE_SRC_BOOKMARK_BKG" "$SRC_BOOKMARK_BKG"
_LOG_PROCESS_DETAIL $RUN_ID "PROCESSING_TYPE" "$PROCESSING_TYPE"
_LOG_PROCESS_DETAIL $RUN_ID "FAH_BOOKMARK_DATE" "$FAH_BOOKMARK_DATE_FULL"
_LOG_PROCESS_DETAIL $RUN_ID "BKG_BOOKMARK_DATE" "$BKG_BOOKMARK_DATE"

if [ $PROCESSING_TYPE = "R" ];
then
  _LOG "re-creating table $FACT_STAGE_TABLE for reprocessing..." 
  _LOG "disable nodrop - OK if errors here." 
  hive -e "use $STAGE_DB; alter table $STAGE_TABLE disable NO_DROP;" 
  set -o errexit 
  _LOG "disable nodrop ended." 
  if hdfs dfs -test -e /data/HWW/$STAGE_DB/$STAGE_TABLE; then 
    _LOG "removing existing table files ... " 
    hdfs dfs -rm -R /data/HWW/$STAGE_DB/$STAGE_TABLE 
    if [ $? -ne 0 ]; then
    _LOG "Error deleting table files. Installation FAILED."
    _END_PROCESS $RUN_ID $ERROR_CODE
    _LOG_PROCESS_DETAIL $RUN_ID "STATUS" "ERROR: $ERROR_CODE"
    _FREE_LOCK $HWW_LOCK_NAME
      exit 1
    fi
  fi 
  hive -hiveconf job.queue="${JOB_QUEUE}" -hiveconf hex.db="${STAGE_DB}" -hiveconf hex.table="${STAGE_TABLE}" -f $SCRIPT_PATH/createTable_ETL_HCOM_HEX_FACT_STAGE.hql
  if [ $? -ne 0 ]; then
    _LOG "Error re-creating table. Installation FAILED."
    _END_PROCESS $RUN_ID $ERROR_CODE
    _LOG_PROCESS_DETAIL $RUN_ID "STATUS" "ERROR: $ERROR_CODE"
    _FREE_LOCK $HWW_LOCK_NAME
    exit 1
  fi
  _LOG "re-creating table $FACT_STAGE_TABLE Done." 
  
  NEW_BOOKMARK=`hive -hiveconf mapred.job.queue.name="${JOB_QUEUE}" -e "select min(report_start_date) from ${STAGE_DB}.${REPORT_TABLE};"`
  _WRITE_PROCESS_CONTEXT "$PROCESS_ID" "SRC_BOOKMARK_OMNI" "$NEW_BOOKMARK"
  ERROR_CODE=$?
  if [[ $ERROR_CODE -ne 0 ]]; then
    _LOG "HEMS ERROR! Unable to update bookmark. [ERROR_CODE=$ERROR_CODE]. Manually Update Bookmark before next run or reprocess!" $HEX_LOGS/LNX-HCOM_HEX_FACT.log
    _END_PROCESS $RUN_ID $ERROR_CODE
    _LOG_PROCESS_DETAIL $RUN_ID "STATUS" "ERROR: $ERROR_CODE"
    _FREE_LOCK $HWW_LOCK_NAME
    exit 1
  fi
  _LOG "Updated Omniture source bookmark to [$NEW_BOOKMARK]" $HEX_LOGS/LNX-HCOM_HEX_FACT.log
  
   _WRITE_PROCESS_CONTEXT "$PROCESS_ID" "SRC_BOOKMARK_BKG" "$NEW_BOOKMARK"
   ERROR_CODE=$?
   if [[ $ERROR_CODE -ne 0 ]]; then
    _LOG "HEMS ERROR! Unable to update bookmark. [ERROR_CODE=$ERROR_CODE]. Manually Update Bookmark before next run or reprocess!" $HEX_LOGS/LNX-HCOM_HEX_FACT.log
    _END_PROCESS $RUN_ID $ERROR_CODE
    _LOG_PROCESS_DETAIL $RUN_ID "STATUS" "ERROR: $ERROR_CODE"
    _FREE_LOCK $HWW_LOCK_NAME
    exit 1
  fi
  _LOG "Updated Transactions source bookmark to [$NEW_BOOKMARK]" $HEX_LOGS/LNX-HCOM_HEX_FACT.log
  
  _LOG "Done Reprocessing" $HEX_LOGS/LNX-HCOM_HEX_FACT.log

  _LOG_PROCESS_DETAIL $RUN_ID "AFTER_SRC_BOOKMARK_OMNI" "$NEW_BOOKMARK"
  _LOG_PROCESS_DETAIL $RUN_ID "AFTER_SRC_BOOKMARK_BKG" "$NEW_BOOKMARK"
  
  _LOG "Setting PROCESSING_TYPE to [D] for next run" $HEX_LOGS/LNX-HCOM_HEX_FACT.log
  _WRITE_PROCESS_CONTEXT "$PROCESS_ID" "PROCESSING_TYPE" "D"
  _LOG "Next run will start from Staging data processing step" $HEX_LOGS/LNX-HCOM_HEX_FACT.log
  _WRITE_PROCESS_CONTEXT "$PROCESS_ID" "STEP_TO_PROCESS_FROM" "$STEP_LOAD_STAGING_DATA"
else
  if [[ "$STEP_TO_PROCESS_FROM" -le "$STEP_LOAD_REPORTING_REQUIREMENTS" ]]; then
    LOG_FILE_NAME="hdp_hex_fact_populate_reporting_table_${SRC_BOOKMARK_OMNI}-${SRC_BOOKMARK_BKG}.log"
    _LOG "loading raw reporting requirements table LZ.${REPORT_TABLE}" $HEX_LOGS/LNX-HCOM_HEX_FACT.log
    hive -hiveconf hex.report.file="${REPORT_FILE}" -hiveconf mapred.job.queue.name="${JOB_QUEUE}" -hiveconf lz.db="LZ" -hiveconf hex.db="${STAGE_DB}" -hiveconf hex.report.table="${REPORT_TABLE}" -f $SCRIPT_PATH_REP/createTable_HEX_REPORTING_REQUIREMENTS.hql >> $HEX_LOGS/$LOG_FILE_NAME 2>&1
    ERROR_CODE=$?
    if [[ $ERROR_CODE -ne 0 ]]; then
      _LOG "HEX_FACT_STAGE: Reporting table load FAILED [ERROR_CODE=$ERROR_CODE]. See [$HEX_LOGS/$LOG_FILE_NAME] for more information." $HEX_LOGS/LNX-HCOM_HEX_FACT.log
      _END_PROCESS $RUN_ID $ERROR_CODE
      _LOG_PROCESS_DETAIL $RUN_ID "STATUS" "ERROR: $ERROR_CODE"
      _FREE_LOCK $HWW_LOCK_NAME
      exit 1
    fi
    _LOG "Done loading raw reporting requirements table LZ.${REPORT_TABLE}" $HEX_LOGS/LNX-HCOM_HEX_FACT.log
    
    _LOG "loading reporting requirements table $STAGE_DB.$REPORT_TABLE" $HEX_LOGS/LNX-HCOM_HEX_FACT.log
    hive -hiveconf min_src_bookmark="${MIN_SRC_BOOKMARK}" -hiveconf mapred.job.queue.name="${JOB_QUEUE}" -hiveconf lz.db="LZ" -hiveconf hex.db="${STAGE_DB}" -hiveconf hex.report.table="${REPORT_TABLE}" -f $SCRIPT_PATH_REP/insert_HEX_REPORTING_REQUIREMENTS.hql >> $HEX_LOGS/$LOG_FILE_NAME 2>&1
    ERROR_CODE=$?
    if [[ $ERROR_CODE -ne 0 ]]; then
      _LOG "HEX_FACT_STAGE: Reporting table load FAILED [ERROR_CODE=$ERROR_CODE]. See [$HEX_LOGS/$LOG_FILE_NAME] for more information." $HEX_LOGS/LNX-HCOM_HEX_FACT.log
      _END_PROCESS $RUN_ID $ERROR_CODE
      _LOG_PROCESS_DETAIL $RUN_ID "STATUS" "ERROR: $ERROR_CODE"
      _FREE_LOCK $HWW_LOCK_NAME
      exit 1
    fi
    _LOG "Done loading reporting requirements table $STAGE_DB.$REPORT_TABLE" $HEX_LOGS/LNX-HCOM_HEX_FACT.log
    _WRITE_PROCESS_CONTEXT "$PROCESS_ID" "STEP_TO_PROCESS_FROM" "$STEP_LOAD_STAGING_DATA"
  fi
  
  if [[ "$STEP_TO_PROCESS_FROM"  -le  "$STEP_LOAD_STAGING_DATA" ]]; then
    # daily incremental load
    _LOG "Incremental Booking Fact Staging data load (SRC_BOOKMARK_OMNI=[$SRC_BOOKMARK_OMNI], SRC_BOOKMARK_OMNI_HR=[$SRC_BOOKMARK_OMNI_HOUR], SRC_BOOKMARK_BKG=[$SRC_BOOKMARK_BKG], MIN_SRC_BOOKMARK=[$MIN_SRC_BOOKMARK])" $HEX_LOGS/LNX-HCOM_HEX_FACT.log
    
    LOG_FILE_NAME="hdp_hex_fact_stage_active_hits_${SRC_BOOKMARK_OMNI}-${SRC_BOOKMARK_BKG}.log"
    #MONTH=`date --date="${START_DT}" '+%Y-%m'`
  
    MIN_REPORT_DATE=`hive -hiveconf mapred.job.queue.name="${JOB_QUEUE}" -e "select min(report_start_date) from ${STAGE_DB}.${REPORT_TABLE};"`
    MIN_REPORT_DATE_YM=`date --date="${MIN_REPORT_DATE}" '+%Y-%m'`
  
  
    MAX_REPORT_HIT_DATE=`hive -hiveconf mapred.job.queue.name="${JOB_QUEUE}" -e "select max(report_end_date) from ${STAGE_DB}.${REPORT_TABLE};"`
    if [ "${FAH_BOOKMARK_DATE}" \< "${MAX_REPORT_HIT_DATE}" ]
    then 
      MAX_OMNI_HIT_DATE=${FAH_BOOKMARK_DATE}
    else
      MAX_OMNI_HIT_DATE=${MAX_REPORT_HIT_DATE}
    fi
    MAX_OMNI_HIT_DATE_YM=`date --date="$MAX_OMNI_HIT_DATE" '+%Y-%m'`
  
  
    _LOG "MIN_REPORT_DATE=$MIN_REPORT_DATE, MIN_REPORT_DATE_YM=$MIN_REPORT_DATE_YM, MAX_OMNI_HIT_DATE=$MAX_OMNI_HIT_DATE, MAX_OMNI_HIT_DATE_YM=$MAX_OMNI_HIT_DATE_YM" $HEX_LOGS/LNX-HCOM_HEX_FACT.log
  
    _LOG "loading first assignment hits for active reporting requirements into $ACTIVE_FAH_TABLE ..." $HEX_LOGS/LNX-HCOM_HEX_FACT.log
    hive -hiveconf max_omniture_record_yr_month="${MAX_OMNI_HIT_DATE_YM}" -hiveconf max_omniture_record_date="${MAX_OMNI_HIT_DATE}" -hiveconf min_report_date="${MIN_REPORT_DATE}" -hiveconf min_report_date_yrmonth="${MIN_REPORT_DATE_YM}" -hiveconf hex.rep.table="${REPORT_TABLE}" -hiveconf job.queue="${JOB_QUEUE}" -hiveconf hex.db="${STAGE_DB}" -hiveconf hex.sup.map.table="${SUP_MAP_TABLE}" -hiveconf hex.table="${ACTIVE_FAH_TABLE}" -f $SCRIPT_PATH/insertTable_ETL_HCOM_HEX_ACTIVE_FIRST_ASSIGNMENT_HITS.hql >> $HEX_LOGS/$LOG_FILE_NAME 2>&1 
    ERROR_CODE=$?
    if [[ $ERROR_CODE -ne 0 ]]; then
      _LOG "HEX_FACT_STAGE: Booking Fact Staging load FAILED [ERROR_CODE=$ERROR_CODE]. See [$HEX_LOGS/$LOG_FILE_NAME] for more information." $HEX_LOGS/LNX-HCOM_HEX_FACT.log
      _END_PROCESS $RUN_ID $ERROR_CODE
      _LOG_PROCESS_DETAIL $RUN_ID "STATUS" "ERROR: $ERROR_CODE"
      _FREE_LOCK $HWW_LOCK_NAME
      exit 1
    fi
    _LOG "Done loading first assignment hits for active reporting requirements into $ACTIVE_FAH_TABLE" $HEX_LOGS/LNX-HCOM_HEX_FACT.log

  
    _LOG "loading incremental first_assignment_hits into $STAGE_TABLE ..." $HEX_LOGS/LNX-HCOM_HEX_FACT.log
    hive -hiveconf src_bookmark_omni="${SRC_BOOKMARK_OMNI}" -hiveconf src_bookmark_omni_hr="${SRC_BOOKMARK_OMNI_HOUR}" -hiveconf hex.active.hits.table="${ACTIVE_FAH_TABLE}" -hiveconf job.queue="${JOB_QUEUE}" -hiveconf hex.db="${STAGE_DB}" -hiveconf hex.table="${STAGE_TABLE}" -f $SCRIPT_PATH/insertTable_ETL_HCOM_HEX_FACT_STAGE_OMNITURE.hql >> $HEX_LOGS/$LOG_FILE_NAME 2>&1 
    ERROR_CODE=$?
    if [[ $ERROR_CODE -ne 0 ]]; then
      _LOG "HEX_FACT_STAGE: Booking Fact Staging load FAILED [ERROR_CODE=$ERROR_CODE]. See [$HEX_LOGS/$LOG_FILE_NAME] for more information." $HEX_LOGS/LNX-HCOM_HEX_FACT.log
      _END_PROCESS $RUN_ID $ERROR_CODE
      _LOG_PROCESS_DETAIL $RUN_ID "STATUS" "ERROR: $ERROR_CODE"
      _FREE_LOCK $HWW_LOCK_NAME
      exit 1
    fi
    _LOG "Done loading incremental first_assignment_hits into $STAGE_TABLE" $HEX_LOGS/LNX-HCOM_HEX_FACT.log

    MAX_REPORT_TRANS_DATE=`hive -hiveconf mapred.job.queue.name="${JOB_QUEUE}" -e "select max(trans_date) from ${STAGE_DB}.${REPORT_TABLE};"`
    if [ "${FAH_BOOKMARK_DATE}" \< "${MAX_REPORT_TRANS_DATE}" ]
    then 
      MAX_OMNI_TRANS_DATE=${FAH_BOOKMARK_DATE}
    else
      MAX_OMNI_TRANS_DATE=${MAX_REPORT_TRANS_DATE}
    fi
    MAX_OMNI_TRANS_DATE_YM=`date --date="$MAX_OMNI_TRANS_DATE" '+%Y-%m'`
  
  
    if [ "${BKG_BOOKMARK_DATE}" \< "${MAX_REPORT_TRANS_DATE}" ]
    then 
      MAX_BKG_DATE=${BKG_BOOKMARK_DATE}
    else
      MAX_BKG_DATE=${MAX_REPORT_TRANS_DATE}
    fi
  
    if [ "${MAX_BKG_DATE}" \< "${MAX_OMNI_TRANS_DATE}" ]
    then
      MAX_TRANS_DATE=${MAX_OMNI_TRANS_DATE}
    else
      MAX_TRANS_DATE=${MAX_BKG_DATE}
    fi
    MAX_TRANS_YM=`date --date="${MAX_TRANS_DATE}" '+%Y-%m'`
  
    _LOG "loading incremental booking data into $STAGE_TABLE ..." $HEX_LOGS/LNX-HCOM_HEX_FACT.log
    hive -hiveconf max_trans_record_date_yr_month="${MAX_TRANS_DATE}" -hiveconf max_booking_record_date="${MAX_BKG_DATE}" -hiveconf max_omniture_record_date="${MAX_OMNI_TRANS_DATE}" -hiveconf min_report_date="${MIN_REPORT_DATE}" -hiveconf min_report_date_yrmonth="${MIN_REPORT_DATE_YM}" -hiveconf min_src_bookmark="${MIN_SRC_BOOKMARK}" -hiveconf src_bookmark_bkg="${SRC_BOOKMARK_BKG}" -hiveconf src_bookmark_omni_hr="${SRC_BOOKMARK_OMNI_HOUR}" -hiveconf src_bookmark_omni="${SRC_BOOKMARK_OMNI}" -hiveconf hex.active.hits.table="${ACTIVE_FAH_TABLE}" -hiveconf job.queue="${JOB_QUEUE}" -hiveconf hex.db="${STAGE_DB}" -hiveconf hex.table="${STAGE_TABLE}" -f $SCRIPT_PATH/insertTable_ETL_HCOM_HEX_FACT_STAGE_BOOKING.hql >> $HEX_LOGS/$LOG_FILE_NAME 2>&1 
    ERROR_CODE=$?
    if [[ $ERROR_CODE -ne 0 ]]; then
      _LOG "HEX_FACT_STAGE: Booking Fact Staging load FAILED [ERROR_CODE=$ERROR_CODE]. See [$HEX_LOGS/$LOG_FILE_NAME] for more information." $HEX_LOGS/LNX-HCOM_HEX_FACT.log
      _END_PROCESS $RUN_ID $ERROR_CODE
      _LOG_PROCESS_DETAIL $RUN_ID "STATUS" "ERROR: $ERROR_CODE"
      _FREE_LOCK $HWW_LOCK_NAME
      exit 1
    fi
    _LOG "Done loading incremental booking data into $STAGE_TABLE" $HEX_LOGS/LNX-HCOM_HEX_FACT.log
  

    _WRITE_PROCESS_CONTEXT "$PROCESS_ID" "SRC_BOOKMARK_OMNI" "$FAH_BOOKMARK_DATE_FULL"
    ERROR_CODE=$?
    if [[ $ERROR_CODE -ne 0 ]]; then
      _WRITE_PROCESS_CONTEXT "$PROCESS_ID" "STEP_TO_PROCESS_FROM" "$STEP_LOAD_FACT_DATA"
      _LOG "HEMS ERROR! Unable to update bookmark. [ERROR_CODE=$ERROR_CODE]. Manually Update Bookmark before next run or reprocess!" $HEX_LOGS/LNX-HCOM_HEX_FACT.log
      _END_PROCESS $RUN_ID $ERROR_CODE
      _LOG_PROCESS_DETAIL $RUN_ID "STATUS" "ERROR: $ERROR_CODE"
      _FREE_LOCK $HWW_LOCK_NAME
      exit 1
    fi
    _LOG "Updated Omniture source bookmark to [$FAH_BOOKMARK_DATE]" $HEX_LOGS/LNX-HCOM_HEX_FACT.log
  
    _WRITE_PROCESS_CONTEXT "$PROCESS_ID" "SRC_BOOKMARK_BKG" "$BKG_BOOKMARK_DATE"
    ERROR_CODE=$?
    if [[ $ERROR_CODE -ne 0 ]]; then
      _WRITE_PROCESS_CONTEXT "$PROCESS_ID" "STEP_TO_PROCESS_FROM" "$STEP_LOAD_FACT_DATA"
      _LOG "HEMS ERROR! Unable to update bookmark. [ERROR_CODE=$ERROR_CODE]. Manually Update Bookmark before next run or reprocess!" $HEX_LOGS/LNX-HCOM_HEX_FACT.log
      _END_PROCESS $RUN_ID $ERROR_CODE
      _LOG_PROCESS_DETAIL $RUN_ID "STATUS" "ERROR: $ERROR_CODE"
      _FREE_LOCK $HWW_LOCK_NAME
      exit 1
    fi
    _LOG_PROCESS_DETAIL $RUN_ID "AFTER_SRC_BOOKMARK_OMNI" "$FAH_BOOKMARK_DATE_FULL"
    _LOG_PROCESS_DETAIL $RUN_ID "AFTER_SRC_BOOKMARK_BKG" "$BKG_BOOKMARK_DATE"
  
    _LOG "Updated Transactions source bookmark to [$BKG_BOOKMARK_DATE]" $HEX_LOGS/LNX-HCOM_HEX_FACT.log
    _WRITE_PROCESS_CONTEXT "$PROCESS_ID" "STEP_TO_PROCESS_FROM" "$STEP_LOAD_FACT_DATA"
  fi
  
  source /usr/etl/HWW/common-scripts/init.sh
  
  if [[ "$STEP_TO_PROCESS_FROM"  -le  "$STEP_LOAD_FACT_DATA" ]]; then
    DATE=$(date +"%Y%m%d%H%M");
    LOG_FILE_NAME="hdp_hex_fact_"$DATE".log";
    _LOG "Starting Fact MapReduce [Log file: $HEX_LOGS/$LOG_FILE_NAME]" $HEX_LOGS/LNX-HCOM_HEX_FACT.log
    _LOG_PROCESS_DETAIL $RUN_ID "FACT_UNPARTED_STATUS" "STARTED"
    export HADOOP_CLASSPATH=$HADOOP_CLASSPATH:/usr/lib/hive/lib/*:/app/edw/hive/conf


    hadoop jar ${JAR_PATH} com.expedia.edw.hww.hex.etl.aggregation.R4AggregationTool \
    -Dmapred.job.queue.name=${JOB_QUEUE} \
    -Dgraphite.host=$GRAPHITE_HOST \
    -Dgraphite.prefix=$GRAPHITE_PREFIX \
    --reducers=${FACT_REDUCERS} \
    --sourceDbName=${STAGE_DB} \
    --targetDbName=${STAGE_DB} \
    --sourceTableName=${STAGE_TABLE} \
    --targetTableName=${FACT_TABLE_UNPARTED} \
    --reportTableName=${REPORT_TABLE} >> $HEX_LOGS/$LOG_FILE_NAME 2>&1 
    ERROR_CODE=$?
    if [[ $ERROR_CODE -ne 0 ]]; then
      _LOG "HEX_FACT: Booking Fact load FAILED [ERROR_CODE=$ERROR_CODE]. See [$HEX_LOGS/$LOG_FILE_NAME] for more information." $HEX_LOGS/LNX-HCOM_HEX_FACT.log
      _END_PROCESS $RUN_ID $ERROR_CODE
      _LOG_PROCESS_DETAIL $RUN_ID "STATUS" "ERROR: $ERROR_CODE"
      _FREE_LOCK $HWW_LOCK_NAME
      exit 1
    fi
    _LOG_PROCESS_DETAIL $RUN_ID "FACT_UNPARTED_STATUS" "ENDED"
    _LOG "Fact MapReduce Done" $HEX_LOGS/LNX-HCOM_HEX_FACT.log
    _WRITE_PROCESS_CONTEXT "$PROCESS_ID" "STEP_TO_PROCESS_FROM" "$STEP_LOAD_AGG_DATA"
  fi
  
  if [[ "$STEP_TO_PROCESS_FROM"  -le  "$STEP_LOAD_AGG_DATA" ]]; then
    _LOG "Starting Fact Aggregation Load" $HEX_LOGS/LNX-HCOM_HEX_FACT.log
    _LOG_PROCESS_DETAIL $RUN_ID "FACT_AGGREGATION" "STARTED"
  
    _LOG "Fetching High Frequence Keys for Column all_mktg_seo_30_day" $HEX_LOGS/LNX-HCOM_HEX_FACT.log
    MKTG_SEO_STR=`hive -hiveconf mapred.job.queue.name="${JOB_QUEUE}" -e "select all_mktg_seo_30_day from ${STAGE_DB}.${FACT_TABLE_UNPARTED} group by all_mktg_seo_30_day having count(*)>${KEYS_COUNT_LIMIT};"`
    ERROR_CODE=$?
    if [[ $ERROR_CODE -ne 0 ]]; then
      _LOG "HEX_FACT: Aggregation load FAILED. Error while fetching all_mktg_seo_30_day keys. [ERROR_CODE=$ERROR_CODE]." $HEX_LOGS/LNX-HCOM_HEX_FACT.log
      _END_PROCESS $RUN_ID $ERROR_CODE
      _LOG_PROCESS_DETAIL $RUN_ID "STATUS" "ERROR: $ERROR_CODE"
      _FREE_LOCK $HWW_LOCK_NAME
      exit 1
    fi
    MKTG_SEO_ARR=( $MKTG_SEO_STR );
    delimiter="','";
    MKTG_SEO_STR_FINAL=$(printf "${delimiter}%s" "${MKTG_SEO_ARR[@]}");
    MKTG_SEO_STR_FINAL=${MKTG_SEO_STR_FINAL:${#delimiter}};
    MKTG_SEO_STR_FINAL="array('"${MKTG_SEO_STR_FINAL}"')";

    _LOG "Fetching High Frequence Keys for Column all_mktg_seo_30_day_direct" $HEX_LOGS/LNX-HCOM_HEX_FACT.log
    MKTG_SEO_DIRECT_STR=`hive -hiveconf mapred.job.queue.name="${JOB_QUEUE}" -e "select all_mktg_seo_30_day_direct from ${STAGE_DB}.${FACT_TABLE_UNPARTED} group by all_mktg_seo_30_day_direct having count(*)>${KEYS_COUNT_LIMIT};"`
    ERROR_CODE=$?
    if [[ $ERROR_CODE -ne 0 ]]; then
      _LOG "HEX_FACT: Aggregation load FAILED. Error while fetching all_mktg_seo_30_day_direct keys. [ERROR_CODE=$ERROR_CODE]." $HEX_LOGS/LNX-HCOM_HEX_FACT.log
      _END_PROCESS $RUN_ID $ERROR_CODE
      _LOG_PROCESS_DETAIL $RUN_ID "STATUS" "ERROR: $ERROR_CODE"
      _FREE_LOCK $HWW_LOCK_NAME
      exit 1
    fi
    MKTG_SEO_DIRECT_ARR=( $MKTG_SEO_DIRECT_STR );
    MKTG_SEO_DIRECT_STR_FINAL=$(printf "${delimiter}%s" "${MKTG_SEO_DIRECT_ARR[@]}");
    MKTG_SEO_DIRECT_STR_FINAL=${MKTG_SEO_DIRECT_STR_FINAL:${#delimiter}};
    MKTG_SEO_DIRECT_STR_FINAL="array('"${MKTG_SEO_DIRECT_STR_FINAL}"')";

    _LOG "Fetching High Frequency Keys for Column property_destination_id" $HEX_LOGS/LNX-HCOM_HEX_FACT.log
    PROP_DEST_STR=`hive -hiveconf mapred.job.queue.name="${JOB_QUEUE}" -e "select property_destination_id from ${STAGE_DB}.${FACT_TABLE_UNPARTED} group by property_destination_id having count(*)>${KEYS_COUNT_LIMIT};"`
    ERROR_CODE=$?
    if [[ $ERROR_CODE -ne 0 ]]; then    
      _LOG "HEX_FACT: Aggregation load FAILED. Error while fetching property_destination_id keys. [ERROR_CODE=$ERROR_CODE]." $HEX_LOGS/LNX-HCOM_HEX_FACT.log
      _END_PROCESS $RUN_ID $ERROR_CODE
      _LOG_PROCESS_DETAIL $RUN_ID "STATUS" "ERROR: $ERROR_CODE"
      _FREE_LOCK $HWW_LOCK_NAME
      exit 1
    fi
    PROP_DEST_ARR=( $PROP_DEST_STR );
    delimiter="','";
    PROP_DEST_STR_FINAL=$(printf "${delimiter}%s" "${PROP_DEST_ARR[@]}");
    PROP_DEST_STR_FINAL=${PROP_DEST_STR_FINAL:${#delimiter}};
    PROP_DEST_STR_FINAL="array('"${PROP_DEST_STR_FINAL}"')";
  
    _LOG "Fetching High Frequence Keys for Column lodg_property_key" $HEX_LOGS/LNX-HCOM_HEX_FACT.log
    LODG_PROP_STR=`hive -hiveconf mapred.job.queue.name="${JOB_QUEUE}" -e "select lodg_property_key from ${STAGE_DB}.${FACT_TABLE_UNPARTED} group by lodg_property_key having count(*)>${KEYS_COUNT_LIMIT};"`
    ERROR_CODE=$?
    if [[ $ERROR_CODE -ne 0 ]]; then
      _LOG "HEX_FACT: Aggregation load FAILED. Error while fetching lodg_property_key keys. [ERROR_CODE=$ERROR_CODE]." $HEX_LOGS/LNX-HCOM_HEX_FACT.log
      _END_PROCESS $RUN_ID $ERROR_CODE
      _LOG_PROCESS_DETAIL $RUN_ID "STATUS" "ERROR: $ERROR_CODE"
      _FREE_LOCK $HWW_LOCK_NAME
      exit 1
    fi
    LODG_PROP_ARR=( $LODG_PROP_STR );
    delimiter="','";
    LODG_PROP_STR_FINAL=$(printf "${delimiter}%s" "${LODG_PROP_ARR[@]}");
    LODG_PROP_STR_FINAL=${LODG_PROP_STR_FINAL:${#delimiter}};
    LODG_PROP_STR_FINAL="array('"${LODG_PROP_STR_FINAL}"')";

    echo "$MKTG_SEO_STR_FINAL" > $HEX_LOGS/mktg_seo.lst
    echo "$MKTG_SEO_DIRECT_STR_FINAL" > $HEX_LOGS/mktg_seo_direct.lst
    echo "$PROP_DEST_STR_FINAL" > $HEX_LOGS/prop_dest.lst
    echo "$LODG_PROP_STR_FINAL" > $HEX_LOGS/lodg_prop.lst
    MKTG_SEO_STR_FINAL=""
    MKTG_SEO_DIRECT_STR_FINAL=""
    PROP_DEST_STR_FINAL=""
    LODG_PROP_STR_FINAL=""


    perl -pe 'BEGIN{open F,"/usr/etl/HWW/log/mktg_seo.lst";@f=<F>}s#\${hiveconf:hex.agg.mktg.randomize.array}#@f#' $SCRIPT_PATH_AGG/insert_ETL_HCOM_HEX_AGG_UNPARTED.hql > $HEX_LOGS/temp.hql
    perl -pe 'BEGIN{open F,"/usr/etl/HWW/log/mktg_seo_direct.lst";@f=<F>}s#\${hiveconf:hex.agg.mktg.direct.randomize.array}#@f#' $HEX_LOGS/temp.hql > $HEX_LOGS/temp2.hql
    perl -pe 'BEGIN{open F,"/usr/etl/HWW/log/prop_dest.lst";@f=<F>}s#\${hiveconf:hex.agg.pd.randomize.array}#@f#' $HEX_LOGS/temp2.hql > $HEX_LOGS/temp.hql
    perl -pe 'BEGIN{open F,"/usr/etl/HWW/log/lodg_prop.lst";@f=<F>}s#\${hiveconf:hex.agg.lp.randomize.array}#@f#' $HEX_LOGS/temp.hql > $HEX_LOGS/temp2.hql
      
    _LOG "Fetching Reporting Requirements Count" $HEX_LOGS/LNX-HCOM_HEX_FACT.log
    REQ_COUNT=`hive -hiveconf mapred.job.queue.name=${JOB_QUEUE} -e "select count(1) from ${STAGE_DB}.${REPORT_TABLE}"`
    ERROR_CODE=$?
    if [[ $ERROR_CODE -ne 0 ]]; then
      _LOG "HEX_FACT: Aggregation load FAILED while counting reporting rows. [ERROR_CODE=$ERROR_CODE]." $HEX_LOGS/LNX-HCOM_HEX_FACT.log
      _END_PROCESS $RUN_ID $ERROR_CODE
      _LOG_PROCESS_DETAIL $RUN_ID "STATUS" "ERROR: $ERROR_CODE"
      _FREE_LOCK $HWW_LOCK_NAME
      exit 1
    fi
      
    _LOG "Fetching Reporting Requirements for Batching into $HEX_LOGS/rep_reqs" $HEX_LOGS/LNX-HCOM_HEX_FACT.log
    hive -hiveconf mapred.job.queue.name=${JOB_QUEUE} -e "insert overwrite local directory '$HEX_LOGS/rep_reqs' select concat(experiment_code, ',', version_number, ',', variant_code) from ${STAGE_DB}.${REPORT_TABLE};"
    ERROR_CODE=$?
    if [[ $ERROR_CODE -ne 0 ]]; then
      _LOG "HEX_FACT: Aggregation load FAILED while fetching reporting rows. [ERROR_CODE=$ERROR_CODE]." $HEX_LOGS/LNX-HCOM_HEX_FACT.log
      _END_PROCESS $RUN_ID $ERROR_CODE
      _LOG_PROCESS_DETAIL $RUN_ID "STATUS" "ERROR: $ERROR_CODE"
      _FREE_LOCK $HWW_LOCK_NAME
      exit 1
    fi

    BATCH_COUNT=0
    BATCH_COND=""
  
    _LOG "Total Reporting Requirements: $REQ_COUNT, Batch Size: $REP_BATCH_SIZE" $HEX_LOGS/LNX-HCOM_HEX_FACT.log
  
    while read x
    do
      i=0
      OIFS=$IFS
      IFS=','
      for y in $x
      do
        if [ 0 -eq $i ]
        then
          EXP=$y
        elif [ 1 -eq $i ]
        then
          VER=$y
        else
          VAR=$y
        fi
        i=$(( i + 1 ))
      done
      IFS=$OIFS
      CURR_FILTER="(experiment_code='$EXP' and version_number=$VER and variant_code='$VAR')"
      if [ -n "$BATCH_COND" ];
      then
        BATCH_COND="$BATCH_COND or "
      fi
      BATCH_COND="${BATCH_COND}${CURR_FILTER}"
      BATCH_COUNT=$(( BATCH_COUNT + 1 ))
    
      if [ $BATCH_COUNT -eq $REP_BATCH_SIZE ] || [ $BATCH_COUNT -eq $REQ_COUNT ] 
      then
        echo "$BATCH_COND" > $HEX_LOGS/batch_cond.lst
        BATCH_COND=""
        _LOG "Current Batch Size: $BATCH_COUNT. Remaining: $REQ_COUNT" $HEX_LOGS/LNX-HCOM_HEX_FACT.log
        perl -pe 'BEGIN{open F,"/usr/etl/HWW/log/batch_cond.lst";@f=<F>}s#\${hiveconf:rep.where}#@f#' $HEX_LOGS/temp2.hql > $HEX_LOGS/substitutedAggQuery.hql
      
        perl -p -i -e "s/\\\${hiveconf:job.queue}/$JOB_QUEUE/g" $HEX_LOGS/substitutedAggQuery.hql
        perl -p -i -e "s/\\\${hiveconf:agg.num.reduce.tasks}/$AGG_NUM_REDUCERS/g" $HEX_LOGS/substitutedAggQuery.hql
        perl -p -i -e "s/\\\${hiveconf:hex.fact.table}/$FACT_TABLE_UNPARTED/g" $HEX_LOGS/substitutedAggQuery.hql
        perl -p -i -e "s/\\\${hiveconf:hex.db}/$AGG_DB/g" $HEX_LOGS/substitutedAggQuery.hql
        perl -p -i -e "s/\\\${hiveconf:stage.db}/$STAGE_DB/g" $HEX_LOGS/substitutedAggQuery.hql
        perl -p -i -e "s/\\\${hiveconf:hex.agg.unparted.table}/$FACT_AGG_UNPARTED_TABLE/g" $HEX_LOGS/substitutedAggQuery.hql
        perl -p -i -e "s/\\\${hiveconf:hex.agg.seed}/1000/g" $HEX_LOGS/substitutedAggQuery.hql
        perl -p -i -e "s/\\\${hiveconf:hex.report.table}/$REPORT_TABLE/g" $HEX_LOGS/substitutedAggQuery.hql
        REQ_COUNT=$(( REQ_COUNT - BATCH_COUNT ))
        BATCH_COUNT=0
    
        DATE=$(date +"%Y%m%d%H%M");
        LOG_FILE_NAME="agg_"$DATE".log";
        _LOG "Starting Fact Aggregation Insert [log file: $HEX_LOGS/$LOG_FILE_NAME]" $HEX_LOGS/LNX-HCOM_HEX_FACT.log
        _LOG_PROCESS_DETAIL $RUN_ID "FACT_AGGREGATION_INSERT" "STARTED"
        hive -f $HEX_LOGS/substitutedAggQuery.hql >> $HEX_LOGS/$LOG_FILE_NAME 2>&1
        ERROR_CODE=$?
        if [[ $ERROR_CODE -ne 0 ]]; then
          _LOG "HEX_FACT: Aggregation load FAILED. [ERROR_CODE=$ERROR_CODE]. See [$HEX_LOGS/$LOG_FILE_NAME] for more information." $HEX_LOGS/LNX-HCOM_HEX_FACT.log
          _END_PROCESS $RUN_ID $ERROR_CODE
          _LOG_PROCESS_DETAIL $RUN_ID "STATUS" "ERROR: $ERROR_CODE"
          _FREE_LOCK $HWW_LOCK_NAME
          exit 1
        fi
      fi
    done < $HEX_LOGS/rep_reqs/000000_0

    _LOG_PROCESS_DETAIL $RUN_ID "FACT_AGGREGATION_INSERT" "ENDED"
    _LOG "Fact Aggregation Insert Done" $HEX_LOGS/LNX-HCOM_HEX_FACT.log
    _LOG_PROCESS_DETAIL $RUN_ID "FACT_AGGREGATION" "ENDED"
    _LOG "Fact Aggregation Load Done" $HEX_LOGS/LNX-HCOM_HEX_FACT.log
    _WRITE_PROCESS_CONTEXT "$PROCESS_ID" "STEP_TO_PROCESS_FROM" "$STEP_LOAD_SEG_DATA"
  fi
  
  if [[ "$STEP_TO_PROCESS_FROM"  -le  "$STEP_LOAD_SEG_DATA" ]]; then
    DATE=$(date +"%Y%m%d%H%M");
    LOG_FILE_NAME="seg_"$DATE".log";
    _LOG "Starting Segmentation MapReduce [Log file: $HEX_LOGS/$LOG_FILE_NAME]" $HEX_LOGS/LNX-HCOM_HEX_FACT.log
    _LOG_PROCESS_DETAIL $RUN_ID "SEG_UNPARTED_STATUS" "STARTED"
    export HADOOP_CLASSPATH=$HADOOP_CLASSPATH:/usr/lib/hive/lib/*:/app/edw/hive/conf
    
    hadoop jar ${JAR_PATH} com.expedia.edw.hww.hex.etl.segmentation.SegmentationTool \
    -Dmapred.job.queue.name=${JOB_QUEUE} \
    -Dgraphite.host=$GRAPHITE_HOST \
    -Dgraphite.prefix=$GRAPHITE_PREFIX \
    --reducers=${SEG_NUM_REDUCERS} \
    --sourceDbName=${AGG_DB} \
    --targetDbName=${AGG_DB} \
    --sourceTableName=${FACT_AGG_UNPARTED_TABLE} \
    --targetTableName=${SEG_UNPARTED_TABLE} \
    --segmentationInputFilePath=${SEG_INPUT_FILE_PATH} >> $HEX_LOGS/$LOG_FILE_NAME 2>&1 
  
    ERROR_CODE=$?
    if [[ $ERROR_CODE -ne 0 ]]; then
      _LOG "HEX_SEG: Segmentation load FAILED [ERROR_CODE=$ERROR_CODE]. See [$HEX_LOGS/$LOG_FILE_NAME] for more information." $HEX_LOGS/LNX-HCOM_HEX_FACT.log
      _END_PROCESS $RUN_ID $ERROR_CODE
      _LOG_PROCESS_DETAIL $RUN_ID "STATUS" "ERROR: $ERROR_CODE"
      _FREE_LOCK $HWW_LOCK_NAME
      exit 1
    fi
    _LOG_PROCESS_DETAIL $RUN_ID "SEG_UNPARTED_STATUS" "ENDED"
    _LOG "Segmentation MapReduce Done" $HEX_LOGS/LNX-HCOM_HEX_FACT.log
  
    _LOG "Starting EXP_LIST Load" $HEX_LOGS/LNX-HCOM_HEX_FACT.log
    _LOG_PROCESS_DETAIL $RUN_ID "EXP_STATUS" "STARTED"
  
    hive -hiveconf job.queue="${JOB_QUEUE}" -hiveconf hex.db="${AGG_DB}" -hiveconf hex.table="${SEG_EXP_LIST_TABLE}" -f $SCRIPT_PATH_REP/insert_SEG_EXP_LIST_TABLE.hql >> $HEX_LOGS/$LOG_FILE_NAME 2>&1
    ERROR_CODE=$?
    if [[ $ERROR_CODE -ne 0 ]]; then   
      _LOG "HEX_EXP: EXP_LIST load FAILED [ERROR_CODE=$ERROR_CODE]. See [$HEX_LOGS/$LOG_FILE_NAME] for more information." $HEX_LOGS/LNX-HCOM_HEX_FACT.log
      _END_PROCESS $RUN_ID $ERROR_CODE
      _LOG_PROCESS_DETAIL $RUN_ID "STATUS" "ERROR: $ERROR_CODE"
      _FREE_LOCK $HWW_LOCK_NAME
      exit 1
    fi
    _WRITE_PROCESS_CONTEXT "$PROCESS_ID" "STEP_TO_PROCESS_FROM" "$STEP_LOAD_DB2_DATA"
  fi
  
  if [[ ( "$STEP_TO_PROCESS_FROM"  -le  "$STEP_LOAD_PARTITIONED_DATA"  &&  "$STEP_TO_PROCESS_FROM"  -ne  "$STEP_LOAD_DB2_SP"  &&  "$STEP_TO_PROCESS_FROM"  -ne  "$STEP_LOAD_DB2_DATA" ) || ( "$PARTED_FACT_LOAD" == "false" )]];then
    DATE=$(date +"%Y%m%d%H%M");
    LOG_FILE_NAME="fact_partition_load_"$DATE".log";
    _LOG "Starting Fact Partition Load [Log file: $HEX_LOGS/$LOG_FILE_NAME]" $HEX_LOGS/LNX-HCOM_HEX_FACT.log
    _LOG_PROCESS_DETAIL $RUN_ID "FACT_STATUS" "STARTED"
    
    sh $SCRIPT_PATH_PARTED/HEX_FACT_PARTED_LOAD.sh $JOB_QUEUE $FACT_LOAD_SPLIT_SIZE $STAGE_DB $FACT_TABLE $HEX_LOGS $LOG_FILE_NAME $PROCESS_ID &
  fi
  
  if [[ ( "$STEP_TO_PROCESS_FROM"  -le  "$STEP_LOAD_PARTITIONED_DATA"  &&  "$STEP_TO_PROCESS_FROM"  -ne  "$STEP_LOAD_DB2_SP"   &&  "$STEP_TO_PROCESS_FROM"  -ne  "$STEP_LOAD_DB2_DATA" ) || ( "$PARTED_AGG_LOAD" == "false" )]];then
    DATE=$(date +"%Y%m%d%H%M");
    LOG_FILE_NAME="agg_partition_load_"$DATE".log";
    _LOG "Starting Agg Partition Load [Log file: $HEX_LOGS/$LOG_FILE_NAME]" $HEX_LOGS/LNX-HCOM_HEX_FACT.log
    _LOG_PROCESS_DETAIL $RUN_ID "FACT_STATUS" "STARTED"
    
    sh $SCRIPT_PATH_PARTED/HEX_AGG_PARTED_LOAD.sh $JOB_QUEUE $FACT_LOAD_SPLIT_SIZE $AGG_DB $AGG_TABLE $FACT_AGG_UNPARTED_TABLE $AGG_NUM_REDUCERS $HEX_LOGS $LOG_FILE_NAME $PROCESS_ID &
  fi
  
  if [[ ( "$STEP_TO_PROCESS_FROM"  -le  "$STEP_LOAD_PARTITIONED_DATA"  &&  "$STEP_TO_PROCESS_FROM"  -ne  "$STEP_LOAD_DB2_SP"   &&  "$STEP_TO_PROCESS_FROM"  -ne  "$STEP_LOAD_DB2_DATA" ) || ( "$PARTED_SEG_LOAD" == "false" )]];then
    DATE=$(date +"%Y%m%d%H%M");
    LOG_FILE_NAME="seg_partition_load_"$DATE".log";
    _LOG "Starting Segmentation Partition Load [Log file: $HEX_LOGS/$LOG_FILE_NAME]" $HEX_LOGS/LNX-HCOM_HEX_FACT.log
    _LOG_PROCESS_DETAIL $RUN_ID "FACT_STATUS" "STARTED"
    sh $SCRIPT_PATH_PARTED/HEX_SEG_PARTED_LOAD.sh  $JOB_QUEUE $FACT_LOAD_SPLIT_SIZE $AGG_DB $SEG_TABLE $SEG_UNPARTED_TABLE $SEG_NUM_REDUCERS $HEX_LOGS $LOG_FILE_NAME $PROCESS_ID &
  fi
  
  if [[ "$STEP_TO_PROCESS_FROM"  -le  "$STEP_LOAD_DB2_DATA" ]]; then
    ########################
    # DB2 Load
    ########################
    if [ "$LOAD_DB2" == "Y" ]
    then
    source /home/hwwetl/.bashrc
    source /home/db2clnt1/sqllib/db2profile
    _LOG "PLAT_HOME: [$PLAT_HOME]" $HEX_LOGS/LNX-HCOM_HEX_FACT.log
    export PLAT_HOME=/usr/local/edw/platform
  
    _LOG "ETLCOMMONSCR: [$ETLCOMMONSCR]" $HEX_LOGS/LNX-HCOM_HEX_FACT.log
    _LOG "DB2INSTANCE: [$DB2INSTANCE]" $HEX_LOGS/LNX-HCOM_HEX_FACT.log
    _LOG "HEMS RUN_ID: [$RUN_ID]" $HEX_LOGS/LNX-HCOM_HEX_FACT.log
    #############
    # REP_REQ
    #############
    #Connect to DB2 and create the table
    _LOG "Create the table [$REP_REQ_TGT_DB2_TABLE] in DB2" $HEX_LOGS/LNX-HCOM_HEX_FACT.log
    _LOG_PROCESS_DETAIL $RUN_ID "REP_DB2_STATUS" "STARTED"
    _DBCONNECT $DB2LOGIN
    /home/db2clnt1/sqllib/bin/db2 -tvf $SCRIPT_PATH_DB2/$REP_REQ_TGT_DB2_TABLE.sql
    ERROR_CODE=$?
    if [ $ERROR_CODE -ge 4 ] ; then
      _WRITE_PROCESS_CONTEXT "$PROCESS_ID" "STEP_TO_PROCESS_FROM" "$STEP_LOAD_DB2_DATA"   
      _LOG "Error: SQL merge step failure, rolling back." $HEX_LOGS/LNX-HCOM_HEX_FACT.log
      _END_PROCESS $RUN_ID $ERROR_CODE
      _LOG_PROCESS_DETAIL $RUN_ID "STATUS" "ERROR: $ERROR_CODE"
      _FREE_LOCK $HWW_LOCK_NAME
      exit 1
    fi

    #Disconnect from DB2 and log the count in HEMS.
    _DBDISCONNECT
  
    #Start the job and logging
    _LOG "============ Starting DB2 load for $REP_REQ_TGT_DB2_TABLE ===============" $HEX_LOGS/LNX-HCOM_HEX_FACT.log
    _LOG "PWD: [$PWD]" $HEX_LOGS/LNX-HCOM_HEX_FACT.log
    _LOG "DB2LOGIN: [$DB2LOGIN]" $HEX_LOGS/LNX-HCOM_HEX_FACT.log
    _LOG "HDPENV: [$HDPENV]" $HEX_LOGS/LNX-HCOM_HEX_FACT.log
    _LOG "STRMJAR: [$STRMJAR]" $HEX_LOGS/LNX-HCOM_HEX_FACT.log
    _LOG "HDPNAMENODE: [$HDPNAMENODE]" $HEX_LOGS/LNX-HCOM_HEX_FACT.log
    _LOG "LOADERPATH: [$LOADERPATH]" $HEX_LOGS/LNX-HCOM_HEX_FACT.log
    _LOG "LOADERSCRIPT: [$LOADERSCRIPT]" $HEX_LOGS/LNX-HCOM_HEX_FACT.log
    _LOG "TGTTBL: [$REP_REQ_TGT_DB2_TABLE]" $HEX_LOGS/LNX-HCOM_HEX_FACT.log
    _LOG "HDFSETLPATH: [$REP_REQ_SRC_HDFS_PATH]" $HEX_LOGS/LNX-HCOM_HEX_FACT.log
  
    #Check the count of records from HDFS
    if [ $REP_REQ_INPUT_TYPE = "LIST" ]; then
      HDPFILEROWCOUNT=$(hadoop fs -cat `cat $REP_REQ_SRC_HDFS_PATH` | wc -l)
    else
      HDPFILEROWCOUNT=$(hadoop fs -cat $REP_REQ_SRC_HDFS_PATH | wc -l)
    fi;

    if [ $HDPFILEROWCOUNT -eq 0 ]; then
      _LOG "Warning: ETL result is empty, no work to do; exiting." $HEX_LOGS/LNX-HCOM_HEX_FACT.log
      _LOG_PROCESS_DETAIL $RUN_ID "DB2_REP_STATUS" "NO DATA"
    else
      _LOG "Data found in source rows; continuing process." $HEX_LOGS/LNX-HCOM_HEX_FACT.log
      _LOG_PROCESS_DETAIL $RUN_ID "DB2_REP_HDP_COUNT" "$HDPFILEROWCOUNT"
      _LOG " Total Records in the Source File :  $HDPFILEROWCOUNT" $HEX_LOGS/LNX-HCOM_HEX_FACT.log

      #Invoke Pipeloader
      _LOG "DB2 integration : bash $LOADERSCRIPT $DB2LOGIN $REP_REQ_SRC_HDFS_PATH $REP_REQ_TGT_DB2_TABLE $HDPNAMENODE $REP_REQ_INPUT_TYPE" $HEX_LOGS/LNX-HCOM_HEX_FACT.log
      bash $LOADERSCRIPT $DB2LOGIN $REP_REQ_SRC_HDFS_PATH $REP_REQ_TGT_DB2_TABLE $HDPNAMENODE $REP_REQ_INPUT_TYPE
      ERROR_CODE=$?
      if [ $ERROR_CODE -ne 0 ] ; then
        _WRITE_PROCESS_CONTEXT "$PROCESS_ID" "STEP_TO_PROCESS_FROM" "$STEP_LOAD_DB2_DATA"
        _LOG "Error: SQL merge step failure, rolling back." $HEX_LOGS/LNX-HCOM_HEX_FACT.log
        _LOG_PROCESS_DETAIL $RUN_ID "DB2_REP_STATUS" "FAILED"
        _END_PROCESS $RUN_ID $ERROR_CODE
        _LOG_PROCESS_DETAIL $RUN_ID "STATUS" "ERROR: $ERROR_CODE"
        _FREE_LOCK $HWW_LOCK_NAME
        exit 1
      fi
      
      #Connect to DB2 and check Count of records Loaded
      _LOG "Update the count from DB2 to HEMS" $HEX_LOGS/LNX-HCOM_HEX_FACT.log
      _DBCONNECT $DB2LOGIN
      DCOUNT=`/home/db2clnt1/sqllib/bin/db2 -x "select count(*) from $REP_REQ_TGT_DB2_TABLE"`
      ERROR_CODE=$?
      if [ $ERROR_CODE -ge 4 ] ; then
        _WRITE_PROCESS_CONTEXT "$PROCESS_ID" "STEP_TO_PROCESS_FROM" "$STEP_LOAD_DB2_DATA"
        _LOG "Error: SQL merge step failure, rolling back." $HEX_LOGS/LNX-HCOM_HEX_FACT.log
        _LOG_PROCESS_DETAIL $RUN_ID "DB2_REP_STATUS" "FAILED"
        _END_PROCESS $RUN_ID $ERROR_CODE
        _LOG_PROCESS_DETAIL $RUN_ID "STATUS" "ERROR: $ERROR_CODE"
        _FREE_LOCK $HWW_LOCK_NAME
        exit 1
      fi

      #Disconnect from DB2 and log the count in HEMS.
      _DBDISCONNECT

      _LOG_PROCESS_DETAIL $RUN_ID "DB2_REP_DB2_COUNT" "$DCOUNT"
      _LOG_PROCESS_DETAIL $RUN_ID "DB2_REP_STATUS" "COMPLETED"
      _LOG "============ Completed DB2 load for $REP_REQ_TGT_DB2_TABLE ===============" $HEX_LOGS/LNX-HCOM_HEX_FACT.log
    fi;

    #############
    # EXP
    #############
    #Connect to DB2 and create the table
    _LOG "Create the table [$EXP_TGT_DB2_TABLE] in DB2" $HEX_LOGS/LNX-HCOM_HEX_FACT.log
    _LOG_PROCESS_DETAIL $RUN_ID "EXP_DB2_STATUS" "STARTED"
    _DBCONNECT $DB2LOGIN
    /home/db2clnt1/sqllib/bin/db2 -tvf $SCRIPT_PATH_DB2/$EXP_TGT_DB2_TABLE.sql
    ERROR_CODE=$?
    if [ $ERROR_CODE -ge 4 ] ; then
      _WRITE_PROCESS_CONTEXT "$PROCESS_ID" "STEP_TO_PROCESS_FROM" "$STEP_LOAD_DB2_DATA"
      _LOG "Error: SQL merge step failure, rolling back." $HEX_LOGS/LNX-HCOM_HEX_FACT.log
      _END_PROCESS $RUN_ID $ERROR_CODE
      _LOG_PROCESS_DETAIL $RUN_ID "STATUS" "ERROR: $ERROR_CODE"
      _FREE_LOCK $HWW_LOCK_NAME
      exit 1
    fi

    #Disconnect from DB2 and log the count in HEMS.
    _DBDISCONNECT
  
    #Start the job and logging
    _LOG "============ Starting DB2 load for $EXP_TGT_DB2_TABLE ===============" $HEX_LOGS/LNX-HCOM_HEX_FACT.log
    _LOG "PWD: [$PWD]" $HEX_LOGS/LNX-HCOM_HEX_FACT.log
    _LOG "DB2LOGIN: [$DB2LOGIN]" $HEX_LOGS/LNX-HCOM_HEX_FACT.log
    _LOG "HDPENV: [$HDPENV]" $HEX_LOGS/LNX-HCOM_HEX_FACT.log
    _LOG "STRMJAR: [$STRMJAR]" $HEX_LOGS/LNX-HCOM_HEX_FACT.log
    _LOG "HDPNAMENODE: [$HDPNAMENODE]" $HEX_LOGS/LNX-HCOM_HEX_FACT.log
    _LOG "LOADERPATH: [$LOADERPATH]" $HEX_LOGS/LNX-HCOM_HEX_FACT.log
    _LOG "LOADERSCRIPT: [$LOADERSCRIPT]" $HEX_LOGS/LNX-HCOM_HEX_FACT.log
    _LOG "TGTTBL: [$EXP_TGT_DB2_TABLE]" $HEX_LOGS/LNX-HCOM_HEX_FACT.log
    _LOG "HDFSETLPATH: [$EXP_SRC_HDFS_PATH]" $HEX_LOGS/LNX-HCOM_HEX_FACT.log
  
    #Check the count of records from HDFS
    if [ $EXP_INPUT_TYPE = "LIST" ]; then
      HDPFILEROWCOUNT=$(hadoop fs -cat `cat $EXP_SRC_HDFS_PATH` | wc -l)
    else
      HDPFILEROWCOUNT=$(hadoop fs -cat $EXP_SRC_HDFS_PATH | wc -l)
    fi;

    if [ $HDPFILEROWCOUNT -eq 0 ]; then
      _LOG "Warning: ETL result is empty, no work to do; exiting." $HEX_LOGS/LNX-HCOM_HEX_FACT.log
      _LOG_PROCESS_DETAIL $RUN_ID "DB2_EXP_STATUS" "NO DATA"
    else
      _LOG "Data found in source rows; continuing process." $HEX_LOGS/LNX-HCOM_HEX_FACT.log
      _LOG_PROCESS_DETAIL $RUN_ID "DB2_EXP_HDP_COUNT" "$HDPFILEROWCOUNT"
      _LOG " Total Records in the Source File :  $HDPFILEROWCOUNT" $HEX_LOGS/LNX-HCOM_HEX_FACT.log

      #Invoke Pipeloader
      _LOG "DB2 integration : bash $LOADERSCRIPT $DB2LOGIN $EXP_SRC_HDFS_PATH $EXP_TGT_DB2_TABLE $HDPNAMENODE $EXP_INPUT_TYPE" $HEX_LOGS/LNX-HCOM_HEX_FACT.log
      bash $LOADERSCRIPT $DB2LOGIN $EXP_SRC_HDFS_PATH $EXP_TGT_DB2_TABLE $HDPNAMENODE $EXP_INPUT_TYPE
      ERROR_CODE=$?
      if [ $ERROR_CODE -ne 0 ] ; then
        _WRITE_PROCESS_CONTEXT "$PROCESS_ID" "STEP_TO_PROCESS_FROM" "$STEP_LOAD_DB2_DATA"
        _LOG "Error: SQL merge step failure, rolling back." $HEX_LOGS/LNX-HCOM_HEX_FACT.log
        _LOG_PROCESS_DETAIL $RUN_ID "DB2_EXP_STATUS" "FAILED"
        _END_PROCESS $RUN_ID $ERROR_CODE
        _LOG_PROCESS_DETAIL $RUN_ID "STATUS" "ERROR: $ERROR_CODE"
        _FREE_LOCK $HWW_LOCK_NAME
        exit 1
      fi
      
      #Connect to DB2 and check Count of records Loaded
      _LOG "Update the count from DB2 to HEMS" $HEX_LOGS/LNX-HCOM_HEX_FACT.log
      _DBCONNECT $DB2LOGIN
      DCOUNT=`/home/db2clnt1/sqllib/bin/db2 -x "select count(*) from $EXP_TGT_DB2_TABLE"`
      ERROR_CODE=$?
      if [ $ERROR_CODE -ge 4 ] ; then
        _WRITE_PROCESS_CONTEXT "$PROCESS_ID" "STEP_TO_PROCESS_FROM" "$STEP_LOAD_DB2_DATA"
        _LOG "Error: SQL merge step failure, rolling back." $HEX_LOGS/LNX-HCOM_HEX_FACT.log
        _LOG_PROCESS_DETAIL $RUN_ID "DB2_EXP_STATUS" "FAILED"
        _END_PROCESS $RUN_ID $ERROR_CODE
        _LOG_PROCESS_DETAIL $RUN_ID "STATUS" "ERROR: $ERROR_CODE"
        _FREE_LOCK $HWW_LOCK_NAME
        exit 1
      fi

      #Disconnect from DB2 and log the count in HEMS.
      _DBDISCONNECT

      _LOG_PROCESS_DETAIL $RUN_ID "DB2_EXP_DB2_COUNT" "$DCOUNT"
      _LOG_PROCESS_DETAIL $RUN_ID "DB2_EXP_STATUS" "COMPLETED"
      _LOG "============ Completed DB2 load for $EXP_TGT_DB2_TABLE ===============" $HEX_LOGS/LNX-HCOM_HEX_FACT.log
    fi;
    
    #############
    # SEG
    #############
    #Connect to DB2 and create the table
    _LOG "Create the table [$SEG_TGT_DB2_TABLE] in DB2" $HEX_LOGS/LNX-HCOM_HEX_FACT.log
    _LOG_PROCESS_DETAIL $RUN_ID "SEG_DB2_STATUS" "STARTED"
    _DBCONNECT $DB2LOGIN
    /home/db2clnt1/sqllib/bin/db2 -tvf $SCRIPT_PATH_DB2/$SEG_TGT_DB2_TABLE.sql
    ERROR_CODE=$?
    if [ $ERROR_CODE -ge 4 ] ; then
      _WRITE_PROCESS_CONTEXT "$PROCESS_ID" "STEP_TO_PROCESS_FROM" "$STEP_LOAD_DB2_DATA"
      _LOG "Error: SQL merge step failure, rolling back." $HEX_LOGS/LNX-HCOM_HEX_FACT.log
      _END_PROCESS $RUN_ID $ERROR_CODE
      _LOG_PROCESS_DETAIL $RUN_ID "STATUS" "ERROR: $ERROR_CODE"
      _FREE_LOCK $HWW_LOCK_NAME
      exit 1
    fi

    #Disconnect from DB2 and log the count in HEMS.
    _DBDISCONNECT
  
    #Start the job and logging
    _LOG "============ Starting DB2 load for $SEG_TGT_DB2_TABLE ===============" $HEX_LOGS/LNX-HCOM_HEX_FACT.log
    _LOG "PWD: [$PWD]" $HEX_LOGS/LNX-HCOM_HEX_FACT.log
    _LOG "DB2LOGIN: [$DB2LOGIN]" $HEX_LOGS/LNX-HCOM_HEX_FACT.log
    _LOG "HDPENV: [$HDPENV]" $HEX_LOGS/LNX-HCOM_HEX_FACT.log
    _LOG "STRMJAR: [$STRMJAR]" $HEX_LOGS/LNX-HCOM_HEX_FACT.log
    _LOG "HDPNAMENODE: [$HDPNAMENODE]" $HEX_LOGS/LNX-HCOM_HEX_FACT.log
    _LOG "LOADERPATH: [$LOADERPATH]" $HEX_LOGS/LNX-HCOM_HEX_FACT.log
    _LOG "LOADERSCRIPT: [$LOADERSCRIPT]" $HEX_LOGS/LNX-HCOM_HEX_FACT.log
    _LOG "TGTTBL: [$SEG_TGT_DB2_TABLE]" $HEX_LOGS/LNX-HCOM_HEX_FACT.log
    _LOG "HDFSETLPATH: [$SEG_SRC_HDFS_PATH]" $HEX_LOGS/LNX-HCOM_HEX_FACT.log
  
    #Check the count of records from HDFS
    if [ $REP_REQ_INPUT_TYPE = "LIST" ]; then
      HDPFILEROWCOUNT=$(hadoop fs -cat `cat $SEG_SRC_HDFS_PATH` | wc -l)
    else
      HDPFILEROWCOUNT=`hive -hiveconf mapred.job.queue.name=${JOB_QUEUE} -e "select count(1) from ${AGG_DB}.${SEG_UNPARTED_TABLE}"`
    fi;

    if [ $HDPFILEROWCOUNT -eq 0 ]; then
      _LOG "Warning: ETL result is empty, no work to do; exiting." $HEX_LOGS/LNX-HCOM_HEX_FACT.log
      _LOG_PROCESS_DETAIL $RUN_ID "DB2_SEG_STATUS" "NO DATA"
    else
      _LOG "Data found in source rows; continuing process." $HEX_LOGS/LNX-HCOM_HEX_FACT.log
      _LOG_PROCESS_DETAIL $RUN_ID "DB2_SEG_HDP_COUNT" "$HDPFILEROWCOUNT"
      _LOG "Total Records in the Source File :  $HDPFILEROWCOUNT" $HEX_LOGS/LNX-HCOM_HEX_FACT.log

      #Invoke Pipeloader
      _LOG "DB2 integration : bash $LOADERSCRIPT $DB2LOGIN $SEG_SRC_HDFS_PATH $SEG_TGT_DB2_TABLE $HDPNAMENODE $SEG_INPUT_TYPE" $HEX_LOGS/LNX-HCOM_HEX_FACT.log
      bash $LOADERSCRIPT $DB2LOGIN $SEG_SRC_HDFS_PATH $SEG_TGT_DB2_TABLE $HDPNAMENODE $SEG_INPUT_TYPE
      ERROR_CODE=$?
      if [ $ERROR_CODE -ne 0 ] ; then
        _WRITE_PROCESS_CONTEXT "$PROCESS_ID" "STEP_TO_PROCESS_FROM" "$STEP_LOAD_DB2_DATA"
        _LOG "Error: SQL merge step failure, rolling back." $HEX_LOGS/LNX-HCOM_HEX_FACT.log
        _LOG_PROCESS_DETAIL $RUN_ID "DB2_SEG_STATUS" "FAILED"
        _END_PROCESS $RUN_ID $ERROR_CODE
        _LOG_PROCESS_DETAIL $RUN_ID "STATUS" "ERROR: $ERROR_CODE"
        _FREE_LOCK $HWW_LOCK_NAME
        exit 1
      fi
      
      #Connect to DB2 and check Count of records Loaded
      _LOG "Update the count from DB2 to HEMS" $HEX_LOGS/LNX-HCOM_HEX_FACT.log
      _DBCONNECT $DB2LOGIN
      DCOUNT=`/home/db2clnt1/sqllib/bin/db2 -x "select count(*) from $SEG_TGT_DB2_TABLE"`
      ERROR_CODE=$?
      if [ $ERROR_CODE -ge 4 ] ; then
        _WRITE_PROCESS_CONTEXT "$PROCESS_ID" "STEP_TO_PROCESS_FROM" "$STEP_LOAD_DB2_DATA"
        _LOG "Error: SQL merge step failure, rolling back." $HEX_LOGS/LNX-HCOM_HEX_FACT.log
        _LOG_PROCESS_DETAIL $RUN_ID "DB2_SEG_STATUS" "FAILED"
        _END_PROCESS $RUN_ID $ERROR_CODE
        _LOG_PROCESS_DETAIL $RUN_ID "STATUS" "ERROR: $ERROR_CODE"
        _FREE_LOCK $HWW_LOCK_NAME
        exit 1
      fi

      #Disconnect from DB2 and log the count in HEMS.
      _DBDISCONNECT

      _LOG_PROCESS_DETAIL $RUN_ID "DB2_SEG_DB2_COUNT" "$DCOUNT"
      _LOG_PROCESS_DETAIL $RUN_ID "DB2_SEG_STATUS" "COMPLETED"
	  _WRITE_PROCESS_CONTEXT "$PROCESS_ID" "STEP_TO_PROCESS_FROM" "$STEP_LOAD_DB2_SP"
      _LOG "============ Completed DB2 load for $SEG_TGT_DB2_TABLE ===============" $HEX_LOGS/LNX-HCOM_HEX_FACT.log
 fi;
fi
fi

	_LOG "============ Start DB2 post processing  ===============" $HEX_LOGS/LNX-HCOM_HEX_FACT.log

if [[ "$STEP_TO_PROCESS_FROM"  -le  "$STEP_LOAD_DB2_SP" ]]; then 
      #####################
      # DB2 post processing
      #####################
      _LOG "Create partitions for DM.RPT_HEXDM_AGG_SEGMENT_COMP" $HEX_LOGS/LNX-HCOM_HEX_FACT.log
      _LOG_PROCESS_DETAIL $RUN_ID "DB2_SP_STATUS" "STARTED"
      #Connect to DB2 and invoke the stored procedure to create partitions for DM.RPT_HEXDM_AGG_SEGMENT_COMP
      _DBCONNECT $DB2LOGIN
      /home/db2clnt1/sqllib/bin/db2 -x "call ETL.SP_HEX_COMPLETED_CREATE_PARTITION()"
    
      ERROR_CODE=$?
      if [ $ERROR_CODE -ge 2 ] ; then
        _WRITE_PROCESS_CONTEXT "$PROCESS_ID" "STEP_TO_PROCESS_FROM" "$STEP_LOAD_DB2_SP"
        _LOG "Error:Check etl.etl_sproc_error for more information" $HEX_LOGS/LNX-HCOM_HEX_FACT.log
        _LOG_PROCESS_DETAIL $RUN_ID "DB2_SP_STATUS" "FAILED"
        _END_PROCESS $RUN_ID $ERROR_CODE
        _LOG_PROCESS_DETAIL $RUN_ID "STATUS" "ERROR: $ERROR_CODE"
        _FREE_LOCK $HWW_LOCK_NAME
        exit 1
      fi
      
      _DBDISCONNECT

      _LOG "Load data into Live and Completed tables" $HEX_LOGS/LNX-HCOM_HEX_FACT.log
      
      rm -f $HEX_LOGS/hex_db2.out
      RETRY_COUNT=1

      while true;
      do
        #Invoke the procedure to insert data into Live and Completed Tables
        _DBCONNECT $DB2LOGIN
        _LOG "Run $RETRY_COUNT" $HEX_LOGS/LNX-HCOM_HEX_FACT.log

        /home/db2clnt1/sqllib/bin/db2 -x "call ETL.SP_RPT_HEXDM_AGG_SEGMENT_LOAD('$FAH_BOOKMARK_DATE_FULL', '$BKG_BOOKMARK_DATE')" > $HEX_LOGS/hex_db2.out
        ERROR_CODE=$?
  
        if [ $ERROR_CODE -ge 2 ] ; then
             _LOG "Error:Check etl.etl_sproc_error for more information" $HEX_LOGS/LNX-HCOM_HEX_FACT.log
             SQL_COND=`egrep -o 'SQL[0-9]+[A-Z]*' $HEX_LOGS/hex_db2.out`
             _LOG "Failure due to $SQL_COND" $HEX_LOGS/LNX-HCOM_HEX_FACT.log

                if [ $SQL_COND == 'SQL4712N' ] || [ $SQL_COND == 'SQL0911N' ] || [ $SQL_COND == 'SQL2310N' ] || [ $SQL_COND == 'SQL1229N' ] || [ $SQL_COND == 'SQL20540N' ] && [ $RETRY_COUNT -le 3 ] ; then
                        _LOG "Retry after 30 minutes" $HEX_LOGS/LNX-HCOM_HEX_FACT.log
                        _LOG_PROCESS_DETAIL $RUN_ID "DB2_SP_STATUS" "FAILED"
                        _LOG_PROCESS_DETAIL $RUN_ID "STATUS" "ERROR: $ERROR_CODE"
                        _DBDISCONNECT
                        RETRY_COUNT=$((RETRY_COUNT+1))
                        sleep 1800
                else
                        _LOG "Sproc Failed" $HEX_LOGS/LNX-HCOM_HEX_FACT.log
                        _DBDISCONNECT
                        _LOG_PROCESS_DETAIL $RUN_ID "DB2_SP_STATUS" "FAILED"
                        _END_PROCESS $RUN_ID $ERROR_CODE
                        _LOG_PROCESS_DETAIL $RUN_ID "STATUS" "ERROR: $ERROR_CODE"
                        _FREE_LOCK $HWW_LOCK_NAME
                        exit 1
                fi
        else
             _LOG "Sproc Successful" $HEX_LOGS/LNX-HCOM_HEX_FACT.log
             _DBDISCONNECT
             break;
        fi
      done

      _LOG_PROCESS_DETAIL $RUN_ID "DB2_SP_STATUS" "COMPLETED"
      _LOG "============ Completed DB2 post processing for $SEG_TGT_DB2_TABLE ===============" $HEX_LOGS/LNX-HCOM_HEX_FACT.log
    
    fi;
  
 
  PARTITION_LOAD_FAILED="N";
  if [[ ( "$STEP_TO_PROCESS_FROM"  -le  "$STEP_LOAD_PARTITIONED_DATA"  &&  "$STEP_TO_PROCESS_FROM"  -ne  "$STEP_LOAD_DB2_SP" ) || ( "$PARTED_SEG_LOAD" == "false" )]];then
    fg
  fi
  
  if [[ ( "$STEP_TO_PROCESS_FROM"  -le  "$STEP_LOAD_PARTITIONED_DATA"  &&  "$STEP_TO_PROCESS_FROM"  -ne  "$STEP_LOAD_DB2_SP" ) || ( "$PARTED_AGG_LOAD" == "false" )]];then
    fg
  fi
  
  if [[ ( "$STEP_TO_PROCESS_FROM"  -le  "$STEP_LOAD_PARTITIONED_DATA"  &&  "$STEP_TO_PROCESS_FROM"  -ne  "$STEP_LOAD_DB2_SP" ) || ( "$PARTED_FACT_LOAD" == "false" )]];then
    fg
  fi
  
  PARTED_SEG_LOAD=`_READ_PROCESS_CONTEXT $PROCESS_ID "PARTED_SEG_LOAD"`
  PARTED_AGG_LOAD=`_READ_PROCESS_CONTEXT $PROCESS_ID "PARTED_AGG_LOAD"`
  PARTED_FACT_LOAD=`_READ_PROCESS_CONTEXT $PROCESS_ID "PARTED_FACT_LOAD"`
  if [[ ( "$PARTED_FACT_LOAD" == "false" || "$PARTED_AGG_LOAD" == "false" || "$PARTED_SEG_LOAD" == "false" ) ]];then
      PARTITION_LOAD_FAILED="Y";
      _WRITE_PROCESS_CONTEXT "$PROCESS_ID" "STEP_TO_PROCESS_FROM" "$STEP_LOAD_PARTITIONED_DATA"
      _LOG "Error: At least one of PARTED_FACT_LOAD=$PARTED_FACT_LOAD, PARTED_AGG_LOAD=$PARTED_AGG_LOAD, PARTED_SEG_LOAD=$PARTED_SEG_LOAD failed (is false)." $HEX_LOGS/LNX-HCOM_HEX_FACT.log
      _LOG_PROCESS_DETAIL $RUN_ID "FACT_STATUS" "FAILED"
      _END_PROCESS $RUN_ID $ERROR_CODE
      _LOG_PROCESS_DETAIL $RUN_ID "STATUS" "ERROR: $ERROR_CODE"
      _FREE_LOCK $HWW_LOCK_NAME
      exit 1      
  fi
  _LOG_PROCESS_DETAIL $RUN_ID "FACT_STATUS" "ENDED"

fi
if [ "$PARTITION_LOAD_FAILED" == "N" ]; then 
  _WRITE_PROCESS_CONTEXT "$PROCESS_ID" "STEP_TO_PROCESS_FROM" "$STEP_LOAD_REPORTING_REQUIREMENTS"
fi

SRC_BOOKMARK_OMNI_FULL=`_READ_PROCESS_CONTEXT $PROCESS_ID "SRC_BOOKMARK_OMNI"`
SRC_BOOKMARK_BKG=`_READ_PROCESS_CONTEXT $PROCESS_ID "SRC_BOOKMARK_BKG"`
  
echo -e "====================================================================================================================================================================\nHEX Segmented data for Omniture & Booking loaded to DB2 up to BOOKMARKs=[Omniture: $FAH_BOOKMARK_DATE_FULL, Booking: $BKG_BOOKMARK_DATE].\n\nScript Name : $0\n====================================================================================================================================================================\n" | mailx -s "HEX data mart has been refreshed till [Omniture: $FAH_BOOKMARK_DATE_FULL, Booking: $BKG_BOOKMARK_DATE]." $EMAIL_SUCCESS_RECIPIENTS
  
_LOG_PROCESS_DETAIL $RUN_ID "STATUS" "SUCCESS"
_END_PROCESS $RUN_ID $ERROR_CODE
_FREE_LOCK $HWW_LOCK_NAME

_LOG "Job completed successfully" $HEX_LOGS/LNX-HCOM_HEX_FACT.log

