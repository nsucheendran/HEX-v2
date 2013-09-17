#!/bin/bash
#
# LNX-HCOM_HEX_FIRST_ASSIGNMENT_HIT.sh
# Wrapper to load first assignment hit data for HEX
#
# Usage:
#  LNX-HCOM_HEX_FIRST_ASSIGNMENT_HIT.sh
#
# Error codes:
#   0 Success
#   1 General error
#
# user          date            comment
# ############# ############### ####################
# achadha       2013-09-11      Wrapper to incrementally load/reprocess first assignment hit data

PLAT_HOME=/usr/local/edw/platform
HWW_HOME=/usr/etl/HWW
SCRIPT_PATH=$HWW_HOME/hdp_hww_hex_etl/hql/R1
HEX_LST_PATH=/app/etl/HWW/Omniture/listfiles
HEX_LOGS=/usr/etl/HWW/log

source $PLAT_HOME/common/sh_helpers.sh
source $PLAT_HOME/common/sh_metadata_storage.sh

HWW_FAH_LOCK_NAME="hdp_hww_hex_first_assignment_hit.lock"
MESSAGE="LNX-HCOM_HEX_FIRST_ASSIGNMENT_HIT.sh failed: Previous script still running"
_ACQUIRE_LOCK $HWW_FAH_LOCK_NAME "$MESSAGE" 30

STAGE_NAME="R1: HEX First Assignment Hit"
_LOG_START_STAGE "$STAGE_NAME"

PROCESS_NAME="ETL_HCOM_HEX_FIRST_ASSIGNMENT_HIT"
_LOG "PROCESS_NAME=[$PROCESS_NAME]"

ERROR_CODE=0

PROCESS_ID=$(_GET_PROCESS_ID "$PROCESS_NAME");
RETURN_CODE="$?"

if [ "$PROCESS_ID" == "" ] || (( $RETURN_CODE != 0 )); then
  _LOG "ERROR: Process [$PROCESS_NAME] does not exist in HEMS"
  ERROR_CODE=1
  _FREE_LOCK $HWW_FAH_LOCK_NAME
  exit 1;
else
  RUN_ID=$(_RUN_PROCESS $PROCESS_ID "$PROCESS_NAME")
  _LOG "PROCESS_ID=[$PROCESS_ID]"
  _LOG "RUN_ID=[$RUN_ID]"
  _LOG_PROCESS_DETAIL $RUN_ID "Started" "$ERROR_CODE"
fi

FAH_TABLE=`_READ_PROCESS_CONTEXT $PROCESS_ID "FAH_TABLE"`
FAH_DB=`_READ_PROCESS_CONTEXT $PROCESS_ID "FAH_DB"`
JOB_QUEUE=`_READ_PROCESS_CONTEXT $PROCESS_ID "JOB_QUEUE"`
LAST_DT=`_READ_PROCESS_CONTEXT $PROCESS_ID "BOOKMARK"`
PROCESSING_TYPE=`_READ_PROCESS_CONTEXT $PROCESS_ID "PROCESSING_TYPE"`

_LOG "PROCESSING_TYPE=$PROCESSING_TYPE"
_LOG "LAST_DT=$LAST_DT"

if [ $PROCESSING_TYPE = "R" ];
then
  START_YEAR=`_READ_PROCESS_CONTEXT $PROCESS_ID "REPROCESS_START_YEAR"`
  START_MONTH=`_READ_PROCESS_CONTEXT $PROCESS_ID "REPROCESS_START_MONTH"`

  END_YEAR=`date --date="${LAST_DT}" '+%Y'`
  END_MONTH=`date --date="${LAST_DT}" '+%m'`

  _LOG "Starting Reprocessing for period: $START_YEAR-$START_MONTH to $END_YEAR-$END_MONTH (BOOKMARK=[$LAST_DT])"

  # drop monthly partitions that need to be reprocessed. reprocessing can only be specified at a year-month grain.
  CURR_YEAR=$START_YEAR
  CURR_MONTH=$START_MONTH
  while [ "${CURR_YEAR}${CURR_MONTH}" \< "${END_YEAR}${END_MONTH}" -o "${CURR_YEAR}${CURR_MONTH}" = "${END_YEAR}${END_MONTH}" ]
  do
    LOG_FILE_NAME="hdp_first_assignment_hit_reprocess_${CURR_YEAR}-${CURR_MONTH}.log"
    
    _LOG "Dropping partition [$CURR_YEAR-$CURR_MONTH] from target: $FAH_DB.$FAH_TABLE"
    hive -hiveconf part.year="${CURR_YEAR}" -hiveconf part.month="${CURR_MONTH}" -hiveconf job.queue="${JOB_QUEUE}" -hiveconf hex.fah.db="${FAH_DB}" -hiveconf hex.fah.table="${FAH_TABLE}" -f $SCRIPT_PATH/delete_ETL_HEX_ASSIGNMENT_HIT.hql >> $HEX_LOGS/$LOG_FILE_NAME 2>&1
    ERROR_CODE=$?
    if [[ $ERROR_CODE -ne 0 ]]; then
      _LOG "ERROR while dropping partition [ERROR_CODE=$ERROR_CODE]"
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
    START_DT=`date --date="${CURR_YEAR}-${CURR_MONTH}-01 00" '+%Y-%m-%d:%H'`
    if [ "${CURR_YEAR}${CURR_MONTH}" \< "${END_YEAR}${END_MONTH}" ]
    then
      END_DT=`date --date="${CURR_YEAR}-${CURR_MONTH}-01 00 +1 months -1 hours" '+%Y-%m-%d:%H'`
    else
      END_DT=`date --date="${LAST_DT}" '+%Y-%m-%d:%H'` 
    fi
   
    FILTER_YEAR=`date --date="${CURR_YEAR}-${CURR_MONTH}-01 00 -1 years" '+%Y'` 
    FILTER_MONTH=`date --date="${CURR_YEAR}-${CURR_MONTH}-01 00 -1 years" '+%m'` 
    OIFS=$IFS
    IFS=':'
    arr2=($START_DT)
    IFS=$OIFS 
    START_DATE=${arr2[0]}
    START_HOUR=${arr2[1]}
    IFS=':'
    arr2=($END_DT)
    IFS=$OIFS 
    END_DATE=${arr2[0]}
    END_HOUR=${arr2[1]}
    LOG_FILE_NAME="hdp_first_assignment_hit_reprocess_${START_DATE}:${START_HOUR}-${END_DATE}:${END_HOUR}.log"

    _LOG "Reprocessing First Assignment Hit data between [$START_DATE:$START_HOUR to $END_DATE:$END_HOUR] in target: $FAH_DB.$FAH_TABLE"
    hive -hiveconf into.overwrite="overwrite" -hiveconf start.year="${FILTER_YEAR}" -hiveconf start.month="${FILTER_MONTH}" -hiveconf start.date="${START_DATE}" -hiveconf start.hour="${START_HOUR}" -hiveconf end.date="${END_DATE}" -hiveconf end.hour="${END_HOUR}" -hiveconf job.queue="${JOB_QUEUE}" -hiveconf hex.fah.db="${FAH_DB}" -hiveconf hex.fah.table="${FAH_TABLE}" -f $SCRIPT_PATH/insert_ETL_HEX_ASSIGNMENT_HIT.hql >> $HEX_LOGS/$LOG_FILE_NAME 2>&1 
    ERROR_CODE=$?
    if [[ $ERROR_CODE -ne 0 ]]; then
      _LOG "First Assignment Hit load FAILED [ERROR_CODE=$ERROR_CODE]. [see $HEX_LOGS/$LOG_FILE_NAME] for more information."
      _END_PROCESS $RUN_ID $ERROR_CODE
      _FREE_LOCK $HWW_FAH_LOCK_NAME
      exit 1
    fi

    NEW_YEAR=`date --date="${CURR_YEAR}-${CURR_MONTH}-01 00 +1 months" '+%Y'`
    CURR_MONTH=`date --date="${CURR_YEAR}-${CURR_MONTH}-01 00 +1 months" '+%m'`
    CURR_YEAR=$NEW_YEAR
  done
  _LOG "Done Reprocessing"

  if [ -z $LAST_DT ]; then
    _LOG "Updating BOOKMARK (since none existed) as $END_DATE $END_HOUR"
    `_WRITE_PROCESS_CONTEXT "$PROCESS_ID" "BOOKMARK" "$END_DATE $END_HOUR"`
  fi
  _LOG "Setting PROCESSING_TYPE to [D] for next run"
  `_WRITE_PROCESS_CONTEXT "$PROCESS_ID" "PROCESSING_TYPE" "D"`
  
else
  # daily incremental load: uses list files to determine max contiguous delta upto 24 hrs available at source from the bookmark date

  _LOG "Incremental First Assignment Hit data load (BOOKMARK=[$LAST_DT])"
  START_DT=`date --date="${LAST_DT} +1 hours" '+%Y-%m-%d:%H'`
  FILTER_YEAR=`date --date="${LAST_DT} -1 years" '+%Y'`
  FILTER_MONTH=`date --date="${LAST_DT} -1 years" '+%m'`
  CURR_DATE=$LAST_DT
  END_DT=''
  for line in `cat $HEX_LST_PATH/hww_hex_*.lst|sort|grep -A23 $START_DT`;do
    CURR_DATE=`echo $CURR_DATE|sed -e "s/:/ /g"`
    CURR_DATE=`date --date="${CURR_DATE} +1 hours" '+%Y-%m-%d:%H'`
    if [ $line = $CURR_DATE ];
    then
      END_DT=$line
    else
      break
    fi;
  done
  ERROR_CODE=$?
  if [[ $ERROR_CODE -ne 0 ]]; then
    _LOG "First Assignment Hit load FAILED [ERROR_CODE=$ERROR_CODE] while trying to derive delta range"
    _END_PROCESS $RUN_ID $ERROR_CODE
    _FREE_LOCK $HWW_FAH_LOCK_NAME
    exit 1
  fi
  if [ $END_DT ]; then
    OIFS=$IFS
    IFS=':'
    arr2=($START_DT)
    IFS=$OIFS 
    START_DATE=${arr2[0]}
    START_HOUR=${arr2[1]}
    IFS=':'
    arr2=($END_DT)
    IFS=$OIFS 
    END_DATE=${arr2[0]}
    END_HOUR=${arr2[1]}
    LOG_FILE_NAME="hdp_hcom_hex_first_assignment_hit_${START_DATE}:${START_HOUR}-${END_DATE}:${END_HOUR}.log"
    _LOG "Running First Assignment Hit incremental load for period: [$START_DATE:$START_HOUR to $END_DATE:$END_HOUR] (BOOKMARK=[$LAST_DT])"
    hive -hiveconf into.overwrite="into" -hiveconf start.year="${FILTER_YEAR}" -hiveconf start.month="${FILTER_MONTH}" -hiveconf start.date="${START_DATE}" -hiveconf start.hour="${START_HOUR}" -hiveconf end.date="${END_DATE}" -hiveconf end.hour="${END_HOUR}" -hiveconf job.queue="${JOB_QUEUE}" -hiveconf hex.fah.db="${FAH_DB}" -hiveconf hex.fah.table="${FAH_TABLE}" -f $SCRIPT_PATH/insert_ETL_HEX_ASSIGNMENT_HIT.hql >> $HEX_LOGS/$LOG_FILE_NAME 2>&1 && _WRITE_PROCESS_CONTEXT "$PROCESS_ID" "BOOKMARK" "$END_DATE $END_HOUR" 
    ERROR_CODE=$?
    if [[ $ERROR_CODE -ne 0 ]]; then
      _LOG "First Assignment Hit load FAILED [ERROR_CODE=$ERROR_CODE]. [see $HEX_LOGS/$LOG_FILE_NAME] for more information."
      _END_PROCESS $RUN_ID $ERROR_CODE
      _FREE_LOCK $HWW_FAH_LOCK_NAME
      exit 1
    fi
    _LOG "First Assignment Hit incremental load done."
  else
    _LOG "Contiguous delta not found from BOOKMARK=[$LAST_DT]. Nothing to do."
  fi
fi

_END_PROCESS $RUN_ID $ERROR_CODE
_FREE_LOCK $HWW_FAH_LOCK_NAME

