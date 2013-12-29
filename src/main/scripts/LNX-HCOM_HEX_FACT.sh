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
KEYS_COUNT_LIMIT=`_READ_PROCESS_CONTEXT $PROCESS_ID "KEYS_COUNT_LIMIT"`
AGG_NUM_REDUCERS=`_READ_PROCESS_CONTEXT $PROCESS_ID "AGG_NUM_REDUCERS"`
REP_BATCH_SIZE=`_READ_PROCESS_CONTEXT $PROCESS_ID "REP_BATCH_SIZE"`

FAH_PROCESS_ID=`_READ_PROCESS_CONTEXT $PROCESS_ID "FAH_PROCESS_ID"`
FAH_BOOKMARK_DATE_FULL=`_READ_PROCESS_CONTEXT $FAH_PROCESS_ID "BOOKMARK"`

EMAIL_TO=`_READ_PROCESS_CONTEXT $PROCESS_ID "EMAIL_TO"`
EMAIL_CC=`_READ_PROCESS_CONTEXT $PROCESS_ID "EMAIL_CC"`

EMAIL_RECIPIENTS=$EMAIL_TO
if [ $EMAIL_CC ]
then
  EMAIL_RECIPIENTS="-c $EMAIL_CC $EMAIL_RECIPIENTS"
fi

FAH_BOOKMARK_DATE=`date --date="$FAH_BOOKMARK_DATE_FULL" '+%Y-%m-%d'`

BKG_PROCESS_ID=`_READ_PROCESS_CONTEXT $PROCESS_ID "BKG_PROCESS_ID"`
BKG_BOOKMARK_DATE=`_READ_PROCESS_CONTEXT $BKG_PROCESS_ID "BOOKMARK"`

SRC_BOOKMARK_OMNI=`date --date="${SRC_BOOKMARK_OMNI_FULL}" '+%Y-%m-%d'`
SRC_BOOKMARK_OMNI_HOUR=`date --date="${SRC_BOOKMARK_OMNI_FULL}" '+%H'`

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

_LOG "PROCESSING_TYPE=$PROCESSING_TYPE"
_LOG "MIN_SRC_BOOKMARK=$MIN_SRC_BOOKMARK"
_LOG "MAX_SRC_BOOKMARK=$MAX_SRC_BOOKMARK"


_LOG_PROCESS_DETAIL $RUN_ID "BEFORE_SRC_BOOKMARK_OMNI" "$SRC_BOOKMARK_OMNI_FULL"
_LOG_PROCESS_DETAIL $RUN_ID "BEFORE_SRC_BOOKMARK_BKG" "$SRC_BOOKMARK_BKG"
_LOG_PROCESS_DETAIL $RUN_ID "PROCESSING_TYPE" "$PROCESSING_TYPE"
_LOG_PROCESS_DETAIL $RUN_ID "FAH_BOOKMARK_DATE" "$FAH_BOOKMARK_DATE_FULL"
_LOG_PROCESS_DETAIL $RUN_ID "BKG_BOOKMARK_DATE" "$BKG_BOOKMARK_DATE"

LOG_FILE_NAME="hdp_hex_fact_populate_reporting_table_${SRC_BOOKMARK_OMNI}-${SRC_BOOKMARK_BKG}.log"
_LOG "loading raw reporting requirements table LZ.${REPORT_TABLE}"
hive -hiveconf hex.report.file="${REPORT_FILE}" -hiveconf mapred.job.queue.name="${JOB_QUEUE}" -hiveconf lz.db="LZ" -hiveconf hex.db="${STAGE_DB}" -hiveconf hex.report.table="${REPORT_TABLE}" -f $SCRIPT_PATH_REP/createTable_HEX_REPORTING_REQUIREMENTS.hql >> $HEX_LOGS/$LOG_FILE_NAME 2>&1
ERROR_CODE=$?
if [[ $ERROR_CODE -ne 0 ]]; then
  _LOG "HEX_FACT_STAGE: Reporting table load FAILED [ERROR_CODE=$ERROR_CODE]. See [$HEX_LOGS/$LOG_FILE_NAME] for more information."
  _END_PROCESS $RUN_ID $ERROR_CODE
  _LOG_PROCESS_DETAIL $RUN_ID "STATUS" "ERROR: $ERROR_CODE"
  _FREE_LOCK $HWW_LOCK_NAME
  exit 1
fi
_LOG "Done loading raw reporting requirements table LZ.${REPORT_TABLE}"

_LOG "loading reporting requirements table $STAGE_DB.$REPORT_TABLE"
hive -hiveconf min_src_bookmark="${MIN_SRC_BOOKMARK}" -hiveconf mapred.job.queue.name="${JOB_QUEUE}" -hiveconf lz.db="LZ" -hiveconf hex.db="${STAGE_DB}" -hiveconf hex.report.table="${REPORT_TABLE}" -f $SCRIPT_PATH_REP/insert_HEX_REPORTING_REQUIREMENTS.hql >> $HEX_LOGS/$LOG_FILE_NAME 2>&1
ERROR_CODE=$?
if [[ $ERROR_CODE -ne 0 ]]; then
  _LOG "HEX_FACT_STAGE: Reporting table load FAILED [ERROR_CODE=$ERROR_CODE]. See [$HEX_LOGS/$LOG_FILE_NAME] for more information."
  _END_PROCESS $RUN_ID $ERROR_CODE
  _LOG_PROCESS_DETAIL $RUN_ID "STATUS" "ERROR: $ERROR_CODE"
  _FREE_LOCK $HWW_LOCK_NAME
  exit 1
fi
_LOG "Done loading reporting requirements table $STAGE_DB.$REPORT_TABLE"


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
    hive -hiveconf mapred.job.queue.name="${JOB_QUEUE}" -e "use ${STAGE_DB}; alter table ${STAGE_TABLE} drop if exists partition (year_month='${CURR_YEAR}-${CURR_MONTH}', source='omniture');" >> $HEX_LOGS/$LOG_FILE_NAME 2>&1
    ERROR_CODE=$?
    if [[ $ERROR_CODE -ne 0 ]]; then
      _LOG "ERROR while dropping partition [ERROR_CODE=$ERROR_CODE]. See [$HEX_LOGS/$LOG_FILE_NAME] for more information."
      _END_PROCESS $RUN_ID $ERROR_CODE
      _LOG_PROCESS_DETAIL $RUN_ID "STATUS" "ERROR: $ERROR_CODE"
      _FREE_LOCK $HWW_LOCK_NAME
      exit 1
    fi
    
    hdfs dfs -rm -f "/data/HWW/${STAGE_DB}/${STAGE_TABLE}/year_month=${CURR_YEAR}-${CURR_MONTH}/source=omniture/*"
    ERROR_CODE=$?
    if [[ $ERROR_CODE -ne 0 ]]; then
      _LOG "ERROR while dropping partition [ERROR_CODE=$ERROR_CODE]. See [$HEX_LOGS/$LOG_FILE_NAME] for more information."
      _END_PROCESS $RUN_ID $ERROR_CODE
      _LOG_PROCESS_DETAIL $RUN_ID "STATUS" "ERROR: $ERROR_CODE"
      _FREE_LOCK $HWW_LOCK_NAME
      exit 1
    fi
    
    _LOG "Dropping partition [year_month='$CURR_YEAR-$CURR_MONTH', source='booking'] from target: $STAGE_DB.$STAGE_TABLE"
    hive -hiveconf mapred.job.queue.name="${JOB_QUEUE}" -e "use ${STAGE_DB}; alter table ${STAGE_TABLE} drop if exists partition (year_month='${CURR_YEAR}-${CURR_MONTH}', source='booking');" >> $HEX_LOGS/$LOG_FILE_NAME 2>&1
    ERROR_CODE=$?
    if [[ $ERROR_CODE -ne 0 ]]; then
      _LOG "ERROR while dropping partition [ERROR_CODE=$ERROR_CODE]. See [$HEX_LOGS/$LOG_FILE_NAME] for more information."
      _END_PROCESS $RUN_ID $ERROR_CODE
      _LOG_PROCESS_DETAIL $RUN_ID "STATUS" "ERROR: $ERROR_CODE"
      _FREE_LOCK $HWW_LOCK_NAME
      exit 1
    fi

    hdfs dfs -rm -f "/data/HWW/${STAGE_DB}/${STAGE_TABLE}/year_month=${CURR_YEAR}-${CURR_MONTH}/source=booking/*"
    ERROR_CODE=$?
    if [[ $ERROR_CODE -ne 0 ]]; then
      _LOG "ERROR while dropping partition [ERROR_CODE=$ERROR_CODE]. See [$HEX_LOGS/$LOG_FILE_NAME] for more information."
      _END_PROCESS $RUN_ID $ERROR_CODE
      _LOG_PROCESS_DETAIL $RUN_ID "STATUS" "ERROR: $ERROR_CODE"
      _FREE_LOCK $HWW_LOCK_NAME
      exit 1
    fi
    
    NEW_YEAR=`date --date="${CURR_YEAR}-${CURR_MONTH}-01 00 +1 months" '+%Y'`
    CURR_MONTH=`date --date="${CURR_YEAR}-${CURR_MONTH}-01 00 +1 months" '+%m'`
    CURR_YEAR=$NEW_YEAR
  done
  
  NEW_BOOKMARK=`date --date="${START_YEAR}-${START_MONTH}-01 00 -1 days" '+%Y-%m-%d'`
  _WRITE_PROCESS_CONTEXT "$PROCESS_ID" "SRC_BOOKMARK_OMNI" "$NEW_BOOKMARK"
  ERROR_CODE=$?
  if [[ $ERROR_CODE -ne 0 ]]; then
    _LOG "HEMS ERROR! Unable to update bookmark. [ERROR_CODE=$ERROR_CODE]. Manually Update Bookmark before next run or reprocess!"
    _END_PROCESS $RUN_ID $ERROR_CODE
    _LOG_PROCESS_DETAIL $RUN_ID "STATUS" "ERROR: $ERROR_CODE"
    _FREE_LOCK $HWW_LOCK_NAME
    exit 1
  fi
  _LOG "Updated Omniture source bookmark to to [$NEW_BOOKMARK]"
  
   _WRITE_PROCESS_CONTEXT "$PROCESS_ID" "SRC_BOOKMARK_BKG" "$NEW_BOOKMARK"
   ERROR_CODE=$?
   if [[ $ERROR_CODE -ne 0 ]]; then
    _LOG "HEMS ERROR! Unable to update bookmark. [ERROR_CODE=$ERROR_CODE]. Manually Update Bookmark before next run or reprocess!"
    _END_PROCESS $RUN_ID $ERROR_CODE
    _LOG_PROCESS_DETAIL $RUN_ID "STATUS" "ERROR: $ERROR_CODE"
    _FREE_LOCK $HWW_LOCK_NAME
    exit 1
  fi
  _LOG "Updated Transactions source bookmark to to [$NEW_BOOKMARK]"
  
  _LOG "Done Reprocessing"

  _LOG_PROCESS_DETAIL $RUN_ID "AFTER_SRC_BOOKMARK_OMNI" "$NEW_BOOKMARK"
  _LOG_PROCESS_DETAIL $RUN_ID "AFTER_SRC_BOOKMARK_BKG" "$NEW_BOOKMARK"
  
  _LOG "Setting PROCESSING_TYPE to [D] for next run"
  _WRITE_PROCESS_CONTEXT "$PROCESS_ID" "PROCESSING_TYPE" "D"
else
  # daily incremental load
  _LOG "Incremental Booking Fact Staging data load (SRC_BOOKMARK_OMNI=[$SRC_BOOKMARK_OMNI], SRC_BOOKMARK_OMNI_HR=[$SRC_BOOKMARK_OMNI_HOUR], SRC_BOOKMARK_BKG=[$SRC_BOOKMARK_BKG], MIN_SRC_BOOKMARK=[$MIN_SRC_BOOKMARK])"
  
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
  
  
  _LOG "MIN_REPORT_DATE=$MIN_REPORT_DATE, MIN_REPORT_DATE_YM=$MIN_REPORT_DATE_YM, MAX_OMNI_HIT_DATE=$MAX_OMNI_HIT_DATE, MAX_OMNI_HIT_DATE_YM=$MAX_OMNI_HIT_DATE_YM"
  
  _LOG "loading first assignment hits for active reporting requirements into $ACTIVE_FAH_TABLE ..."
  hive -hiveconf max_omniture_record_yr_month="${MAX_OMNI_HIT_DATE_YM}" -hiveconf max_omniture_record_date="${MAX_OMNI_HIT_DATE}" -hiveconf min_report_date="${MIN_REPORT_DATE}" -hiveconf min_report_date_yrmonth="${MIN_REPORT_DATE_YM}" -hiveconf hex.rep.table="${REPORT_TABLE}" -hiveconf job.queue="${JOB_QUEUE}" -hiveconf hex.db="${STAGE_DB}" -hiveconf hex.table="${ACTIVE_FAH_TABLE}" -f $SCRIPT_PATH/insertTable_ETL_HCOM_HEX_ACTIVE_FIRST_ASSIGNMENT_HITS.hql >> $HEX_LOGS/$LOG_FILE_NAME 2>&1 
  ERROR_CODE=$?
  if [[ $ERROR_CODE -ne 0 ]]; then
    _LOG "HEX_FACT_STAGE: Booking Fact Staging load FAILED [ERROR_CODE=$ERROR_CODE]. See [$HEX_LOGS/$LOG_FILE_NAME] for more information."
    _END_PROCESS $RUN_ID $ERROR_CODE
    _LOG_PROCESS_DETAIL $RUN_ID "STATUS" "ERROR: $ERROR_CODE"
    _FREE_LOCK $HWW_LOCK_NAME
    exit 1
  fi
  _LOG "Done loading first assignment hits for active reporting requirements into $ACTIVE_FAH_TABLE"

  
  _LOG "loading incremental first_assignment_hits into $STAGE_TABLE ..."
  hive -hiveconf src_bookmark_omni="${SRC_BOOKMARK_OMNI}" -hiveconf src_bookmark_omni_hr="${SRC_BOOKMARK_OMNI_HOUR}" -hiveconf hex.active.hits.table="${ACTIVE_FAH_TABLE}" -hiveconf job.queue="${JOB_QUEUE}" -hiveconf hex.db="${STAGE_DB}" -hiveconf hex.table="${STAGE_TABLE}" -f $SCRIPT_PATH/insertTable_ETL_HCOM_HEX_FACT_STAGE_OMNITURE.hql >> $HEX_LOGS/$LOG_FILE_NAME 2>&1 
  ERROR_CODE=$?
  if [[ $ERROR_CODE -ne 0 ]]; then
    _LOG "HEX_FACT_STAGE: Booking Fact Staging load FAILED [ERROR_CODE=$ERROR_CODE]. See [$HEX_LOGS/$LOG_FILE_NAME] for more information."
    _END_PROCESS $RUN_ID $ERROR_CODE
    _LOG_PROCESS_DETAIL $RUN_ID "STATUS" "ERROR: $ERROR_CODE"
    _FREE_LOCK $HWW_LOCK_NAME
    exit 1
  fi
  _LOG "Done loading incremental first_assignment_hits into $STAGE_TABLE"

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
  
  _LOG "loading incremental booking data into $STAGE_TABLE ..."
  hive -hiveconf max_trans_record_date_yr_month="${MAX_TRANS_DATE}" -hiveconf max_booking_record_date="${MAX_BKG_DATE}" -hiveconf max_omniture_record_date="${MAX_OMNI_TRANS_DATE}" -hiveconf min_report_date="${MIN_REPORT_DATE}" -hiveconf min_report_date_yrmonth="${MIN_REPORT_DATE_YM}" -hiveconf min_src_bookmark="${MIN_SRC_BOOKMARK}" -hiveconf src_bookmark_bkg="${SRC_BOOKMARK_BKG}" -hiveconf src_bookmark_omni_hr="${SRC_BOOKMARK_OMNI_HOUR}" -hiveconf src_bookmark_omni="${SRC_BOOKMARK_OMNI}" -hiveconf hex.active.hits.table="${ACTIVE_FAH_TABLE}" -hiveconf job.queue="${JOB_QUEUE}" -hiveconf hex.db="${STAGE_DB}" -hiveconf hex.table="${STAGE_TABLE}" -f $SCRIPT_PATH/insertTable_ETL_HCOM_HEX_FACT_STAGE_BOOKING.hql >> $HEX_LOGS/$LOG_FILE_NAME 2>&1 
  ERROR_CODE=$?
  if [[ $ERROR_CODE -ne 0 ]]; then
    _LOG "HEX_FACT_STAGE: Booking Fact Staging load FAILED [ERROR_CODE=$ERROR_CODE]. See [$HEX_LOGS/$LOG_FILE_NAME] for more information."
    _END_PROCESS $RUN_ID $ERROR_CODE
    _LOG_PROCESS_DETAIL $RUN_ID "STATUS" "ERROR: $ERROR_CODE"
    _FREE_LOCK $HWW_LOCK_NAME
    exit 1
  fi
  _LOG "Done loading incremental booking data into $STAGE_TABLE"
  

  _WRITE_PROCESS_CONTEXT "$PROCESS_ID" "SRC_BOOKMARK_OMNI" "$FAH_BOOKMARK_DATE_FULL"
  ERROR_CODE=$?
  if [[ $ERROR_CODE -ne 0 ]]; then
    _LOG "HEMS ERROR! Unable to update bookmark. [ERROR_CODE=$ERROR_CODE]. Manually Update Bookmark before next run or reprocess!"
    _END_PROCESS $RUN_ID $ERROR_CODE
    _LOG_PROCESS_DETAIL $RUN_ID "STATUS" "ERROR: $ERROR_CODE"
    _FREE_LOCK $HWW_LOCK_NAME
    exit 1
  fi
  _LOG "Updated Omniture source bookmark to to [$FAH_BOOKMARK_DATE]"
  
  _WRITE_PROCESS_CONTEXT "$PROCESS_ID" "SRC_BOOKMARK_BKG" "$BKG_BOOKMARK_DATE"
  ERROR_CODE=$?
  if [[ $ERROR_CODE -ne 0 ]]; then
    _LOG "HEMS ERROR! Unable to update bookmark. [ERROR_CODE=$ERROR_CODE]. Manually Update Bookmark before next run or reprocess!"
    _END_PROCESS $RUN_ID $ERROR_CODE
    _LOG_PROCESS_DETAIL $RUN_ID "STATUS" "ERROR: $ERROR_CODE"
    _FREE_LOCK $HWW_LOCK_NAME
    exit 1
  fi
  _LOG_PROCESS_DETAIL $RUN_ID "AFTER_SRC_BOOKMARK_OMNI" "$FAH_BOOKMARK_DATE_FULL"
  _LOG_PROCESS_DETAIL $RUN_ID "AFTER_SRC_BOOKMARK_BKG" "$BKG_BOOKMARK_DATE"
  
  _LOG "Updated Transactions source bookmark to to [$BKG_BOOKMARK_DATE]"
  
  _LOG "Starting Fact MapReduce [Log file: $HEX_LOGS/$LOG_FILE_NAME]"
  _LOG_PROCESS_DETAIL $RUN_ID "FACT_UNPARTED_STATUS" "STARTED"
  export HADOOP_CLASSPATH=$HADOOP_CLASSPATH:/usr/lib/hive/lib/*:/app/edw/hive/conf
  
  hadoop jar ${JAR_PATH} mr.aggregation.R4AggregationJob \
  -DqueueName=${JOB_QUEUE} \
  -Dreducers=${FACT_REDUCERS} \
  -DsourceDbName=${STAGE_DB} \
  -DtargetDbName=${STAGE_DB} \
  -DsourceTableName=${STAGE_TABLE} \
  -DtargetTableName=${FACT_TABLE_UNPARTED} \
  -DreportTableName=${REPORT_TABLE} >> $HEX_LOGS/$LOG_FILE_NAME 2>&1 
  ERROR_CODE=$?
  if [[ $ERROR_CODE -ne 0 ]]; then
    _LOG "HEX_FACT: Booking Fact load FAILED [ERROR_CODE=$ERROR_CODE]. See [$HEX_LOGS/$LOG_FILE_NAME] for more information."
    _END_PROCESS $RUN_ID $ERROR_CODE
    _LOG_PROCESS_DETAIL $RUN_ID "STATUS" "ERROR: $ERROR_CODE"
    _FREE_LOCK $HWW_LOCK_NAME
    exit 1
  fi
  _LOG_PROCESS_DETAIL $RUN_ID "FACT_UNPARTED_STATUS" "ENDED"
  _LOG "Fact MapReduce Done"
  
  _LOG "Starting Fact Partition Load"
  _LOG_PROCESS_DETAIL $RUN_ID "FACT_STATUS" "STARTED"
  
  hive -hiveconf job.queue="${JOB_QUEUE}" -hiveconf split.size="${FACT_LOAD_SPLIT_SIZE}" -hiveconf hex.db="${STAGE_DB}" -hiveconf hex.table="${FACT_TABLE}" -f $SCRIPT_PATH/insertTable_ETL_HCOM_HEX_FACT.hql >> $HEX_LOGS/$LOG_FILE_NAME 2>&1 
  ERROR_CODE=$?
  if [[ $ERROR_CODE -ne 0 ]]; then
    _LOG "HEX_FACT: Booking Fact load FAILED [ERROR_CODE=$ERROR_CODE]. See [$HEX_LOGS/$LOG_FILE_NAME] for more information."
    _END_PROCESS $RUN_ID $ERROR_CODE
    _LOG_PROCESS_DETAIL $RUN_ID "STATUS" "ERROR: $ERROR_CODE"
    _FREE_LOCK $HWW_LOCK_NAME
    exit 1
  fi
  
  _LOG_PROCESS_DETAIL $RUN_ID "FACT_STATUS" "ENDED"
  _LOG "Fact Partition Load Done"

  _LOG "Starting Fact Aggregation Load"
  _LOG_PROCESS_DETAIL $RUN_ID "FACT_AGGREGATION" "STARTED"
  
  MKTG_SEO_STR=`hive -hiveconf mapred.job.queue.name="${JOB_QUEUE}" -e "select /*+ MAPJOIN(rep) */ all_mktg_seo_30_day from ${STAGE_DB}.${FACT_TABLE} fact join ${STAGE_DB}.${REPORT_TABLE} rep on (fact.variant_code=rep.variant_code and fact.experiment_code=rep.experiment_code and fact.version_number=rep.version_number) group by all_mktg_seo_30_day having count(*)>${KEYS_COUNT_LIMIT};"`
  ERROR_CODE=$?
  if [[ $ERROR_CODE -ne 0 ]]; then
    _LOG "HEX_FACT: Aggregation load FAILED. Error while fetching all_mktg_seo_30_day keys. [ERROR_CODE=$ERROR_CODE]."
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

  MKTG_SEO_DIRECT_STR=`hive -hiveconf mapred.job.queue.name="${JOB_QUEUE}" -e "select /*+ MAPJOIN(rep) */ all_mktg_seo_30_day_direct from ${STAGE_DB}.${FACT_TABLE} fact join ${STAGE_DB}.${REPORT_TABLE} rep on (fact.variant_code=rep.variant_code and fact.experiment_code=rep.experiment_code and fact.version_number=rep.version_number) group by all_mktg_seo_30_day_direct having count(*)>${KEYS_COUNT_LIMIT};"`
  ERROR_CODE=$?
  if [[ $ERROR_CODE -ne 0 ]]; then
    _LOG "HEX_FACT: Aggregation load FAILED. Error while fetching all_mktg_seo_30_day_direct keys. [ERROR_CODE=$ERROR_CODE]."
    _END_PROCESS $RUN_ID $ERROR_CODE
    _LOG_PROCESS_DETAIL $RUN_ID "STATUS" "ERROR: $ERROR_CODE"
    _FREE_LOCK $HWW_LOCK_NAME
    exit 1
  fi
  MKTG_SEO_DIRECT_ARR=( $MKTG_SEO_DIRECT_STR );
  MKTG_SEO_DIRECT_STR_FINAL=$(printf "${delimiter}%s" "${MKTG_SEO_DIRECT_ARR[@]}");
  MKTG_SEO_DIRECT_STR_FINAL=${MKTG_SEO_DIRECT_STR_FINAL:${#delimiter}};
  MKTG_SEO_DIRECT_STR_FINAL="array('"${MKTG_SEO_DIRECT_STR_FINAL}"')";

  PROP_DEST_STR=`hive -hiveconf mapred.job.queue.name="${JOB_QUEUE}" -e "select /*+ MAPJOIN(rep) */ property_destination_id from ${STAGE_DB}.${FACT_TABLE} fact join ${STAGE_DB}.${REPORT_TABLE} rep on (fact.variant_code=rep.variant_code and fact.experiment_code=rep.experiment_code and fact.version_number=rep.version_number) group by property_destination_id having count(*)>${KEYS_COUNT_LIMIT};"`
  ERROR_CODE=$?
  if [[ $ERROR_CODE -ne 0 ]]; then
    _LOG "HEX_FACT: Aggregation load FAILED. Error while fetching property_destination_id keys. [ERROR_CODE=$ERROR_CODE]."
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

  SUPPLIER_PROP_STR=`hive -hiveconf mapred.job.queue.name="${JOB_QUEUE}" -e "select /*+ MAPJOIN(rep) */ supplier_property_id from ${STAGE_DB}.${FACT_TABLE} fact join ${STAGE_DB}.${REPORT_TABLE} rep on (fact.variant_code=rep.variant_code and fact.experiment_code=rep.experiment_code and fact.version_number=rep.version_number) group by supplier_property_id having count(*)>${KEYS_COUNT_LIMIT};"`
  ERROR_CODE=$?
  if [[ $ERROR_CODE -ne 0 ]]; then
    _LOG "HEX_FACT: Aggregation load FAILED. Error while fetching supplier_property_id keys. [ERROR_CODE=$ERROR_CODE]."
    _END_PROCESS $RUN_ID $ERROR_CODE
    _LOG_PROCESS_DETAIL $RUN_ID "STATUS" "ERROR: $ERROR_CODE"
    _FREE_LOCK $HWW_LOCK_NAME
    exit 1
  fi
  SUPPLIER_PROP_ARR=( $SUPPLIER_PROP_STR );
  delimiter="','";
  SUPPLIER_PROP_STR_FINAL=$(printf "${delimiter}%s" "${SUPPLIER_PROP_ARR[@]}");
  SUPPLIER_PROP_STR_FINAL=${SUPPLIER_PROP_STR_FINAL:${#delimiter}};
  SUPPLIER_PROP_STR_FINAL="array('"${SUPPLIER_PROP_STR_FINAL}"')";
  
  echo "$MKTG_SEO_STR_FINAL" > /tmp/mktg_seo.lst
  echo "$MKTG_SEO_DIRECT_STR_FINAL" > /tmp/mktg_seo_direct.lst
  echo "$PROP_DEST_STR_FINAL" > /tmp/prop_dest.lst
  echo "$SUPPLIER_PROP_STR_FINAL" > /tmp/sup_prop.lst
  
  REQ_COUNT=`hive -hiveconf mapred.job.queue.name=edwdev -hiveconf mapred.min.split.size=1073741824 -e "select count(1) from etldata.etl_hex_reporting_requirements"`

  VALS=`hive -hiveconf mapred.job.queue.name=edwdev -e "select concat(experiment_code, ',', version_number, ',', variant_code) from etldata.etl_hex_reporting_requirements;"`

  _LOG "Total Reporting Requirements: $REQ_COUNT, Batch Size: $REP_BATCH_SIZE"
  BATCH_COUNT=0
  BATCH_COND=""
  arr=$(echo $VALS | tr " " "\n")
  for x in $arr
  do
    inarr=$(echo $x | tr "," "\n")
    i=0
    for y in $inarr
    do
      if [ $i -eq 0 ]
      then
        EXP=$y
      elif [ $i -eq 1 ]
      then
        VER=$y
      else
        VAR=$y
      fi
      i=$(( i + 1 ))
    done
    CURR_FILTER="(experiment_code='${EXP}' and version_number=${VER} and variant_code='${VAR}')"
    if [ -n "$BATCH_COND" ];
    then
      BATCH_COND="$BATCH_COND or "
    fi
    BATCH_COND="${BATCH_COND}${CURR_FILTER}"

    if [ $BATCH_COUNT -eq $REP_BATCH_SIZE ] || [ $BATCH_COUNT -eq $REQ_COUNT ] 
    then
      _LOG "Current Batch Size: $BATCH_COUNT. Remaining: $REQ_COUNT"
      perl -pe 'BEGIN{open F,"/tmp/mktg_seo.lst";@f=<F>}s#\${hiveconf:hex.agg.mktg.randomize.array}#@f#' $SCRIPT_PATH_AGG/insert_ETL_HCOM_HEX_AGG.hql > /tmp/temp.hql
      perl -pe 'BEGIN{open F,"/tmp/mktg_seo_direct.lst";@f=<F>}s#\${hiveconf:hex.agg.mktg.direct.randomize.array}#@f#' /tmp/temp.hql > /tmp/substitutedAggQuery.hql
      perl -pe 'BEGIN{open F,"/tmp/prop_dest.lst";@f=<F>}s#\${hiveconf:hex.agg.pd.randomize.array}#@f#' /tmp/substitutedAggQuery.hql > /tmp/temp.hql
      perl -pe 'BEGIN{open F,"/tmp/sup_prop.lst";@f=<F>}s#\${hiveconf:hex.agg.sp.randomize.array}#@f#' /tmp/temp.hql > /tmp/substitutedAggQuery.hql
  
      perl -p -i -e "s/\\\${hiveconf:job.queue}/$JOB_QUEUE/g" /tmp/substitutedAggQuery.hql
      perl -p -i -e "s/\\\${hiveconf:agg.num.reduce.tasks}/$AGG_NUM_REDUCERS/g" /tmp/substitutedAggQuery.hql
      perl -p -i -e "s/\\\${hiveconf:hex.fact.table}/$FACT_TABLE/g" /tmp/substitutedAggQuery.hql
      perl -p -i -e "s/\\\${hiveconf:hex.db}/$AGG_DB/g" /tmp/substitutedAggQuery.hql
      perl -p -i -e "s/\\\${hiveconf:stage.db}/$STAGE_DB/g" /tmp/substitutedAggQuery.hql
      perl -p -i -e "s/\\\${hiveconf:hex.agg.table}/$AGG_TABLE/g" /tmp/substitutedAggQuery.hql
      perl -p -i -e "s/\\\${hiveconf:hex.agg.seed}/1000/g" /tmp/substitutedAggQuery.hql
      perl -p -i -e "s/\\\${hiveconf:hex.report.table}/$REPORT_TABLE/g" /tmp/substitutedAggQuery.hql
      perl -p -i -e "s/\\\${hiveconf:rep.where}/($BATCH_COND)/g" /tmp/substitutedAggQuery.hql
      BATCH_COND=""
      REQ_COUNT=$(( REQ_COUNT - BATCH_COUNT ))
      BATCH_COUNT=0
    
      DATE=$(date +"%Y%m%d%H%M");
      LOG_FILE_NAME="agg_"$DATE".log";
      _LOG "Starting Fact Aggregation Insert [log file: $HEX_LOGS/$LOG_FILE_NAME]"
      _LOG_PROCESS_DETAIL $RUN_ID "FACT_AGGREGATION_INSERT" "STARTED"
      hive -f /tmp/substitutedAggQuery.hql >> $HEX_LOGS/$LOG_FILE_NAME 2>&1
      ERROR_CODE=$?
      if [[ $ERROR_CODE -ne 0 ]]; then
        _LOG "HEX_FACT: Aggregation load FAILED. [ERROR_CODE=$ERROR_CODE]. See [$HEX_LOGS/$LOG_FILE_NAME] for more information."
        _END_PROCESS $RUN_ID $ERROR_CODE
        _LOG_PROCESS_DETAIL $RUN_ID "STATUS" "ERROR: $ERROR_CODE"
        _FREE_LOCK $HWW_LOCK_NAME
        exit 1
      fi
    else
      BATCH_COUNT=$(( BATCH_COUNT + 1 ))
    fi
  done
  _LOG_PROCESS_DETAIL $RUN_ID "FACT_AGGREGATION_INSERT" "ENDED"
  _LOG "Fact Aggregation Insert Done"
  _LOG_PROCESS_DETAIL $RUN_ID "FACT_AGGREGATION" "ENDED"
  _LOG "Fact Aggregation Load Done"
fi

_LOG_PROCESS_DETAIL $RUN_ID "STATUS" "SUCCESS"
_END_PROCESS $RUN_ID $ERROR_CODE
_FREE_LOCK $HWW_LOCK_NAME

_LOG "Job completed successfully"

