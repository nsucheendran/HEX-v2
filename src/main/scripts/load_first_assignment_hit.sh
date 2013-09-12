#!/bin/bash
#
# load_first_assignment_hit.sh
# Wrapper to load first assignment hit data for HEX
#
# Usage:
#  load_first_assignment_hit.sh
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
SCRIPT_PATH=$HWW_HOME/hdp_hww_hex_etl/scripts/hql/R1
HEX_LST_PATH=/app/etl/HWW/Omniture/listfiles
HEX_LOGS=/usr/etl/HWW/log
HEX_LIB=$HWW_HOME/hdp_hww_hex_etl/jars
HEX_VERSION="1.0-SNAPSHOT"

source $PLAT_HOME/common/sh_helpers.sh
source $PLAT_HOME/common/sh_metadata_storage.sh

HWW_FAH_LOCK_NAME="hdp_first_assignment_hit.lock"
MESSAGE="load_first_assignment_hit.sh failed: Previous script still running"
_ACQUIRE_LOCK $HWW_FAH_LOCK_NAME "$MESSAGE" 30

STAGE_NAME="R1: HEX First Assignment Hit"
_LOG_START_STAGE "$STAGE_NAME"

PROCESS_NAME="ETL HCOM FIRST ASSIGNMENT HIT"
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
if [ $PROCESSING_TYPE = "R" ];
then
  START_YEAR=`_READ_PROCESS_CONTEXT $PROCESS_ID "REPROCESS_START_YEAR"`
  START_MONTH=`_READ_PROCESS_CONTEXT $PROCESS_ID "REPROCESS_START_MONTH"`

  END_YEAR=`date --date="${LAST_DT}" '+%Y'`
  END_MONTH=`date --date="${LAST_DT}" '+%m'`

  # drop monthly partitions that need to be reprocessed. reprocessing can only be specified at a year-month grain.
  CURR_YEAR=$START_YEAR
  CURR_MONTH=$START_MONTH
  while [ "${CURR_YEAR}${CURR_MONTH}" -le "${END_YEAR}${END_MONTH}" ]
  do
    LOG_FILE_NAME="hdp_first_assignment_hit_reprocess_${CURR_YEAR}-${CURR_MONTH}.log"
    time hive -hiveconf part.year="${CURR_YEAR}" -hiveconf part.month="${CURR_MONTH}" -hiveconf job.queue="${JOB_QUEUE}" -hiveconf hex.fah.db="${FAH_DB}" -hiveconf hex.fah.table="${FAH_TABLE}" -f $SCRIPT_PATH/delete_ETL_HEX_ASSIGNMENT_HIT.hql >> $HEX_LOGS/$LOG_FILE_NAME 2>&1
    NEW_YEAR=`date --date="${CURR_YEAR}-${CURR_MONTH} +1 months" '+%Y'`
    CURR_MONTH=`date --date="${CURR_YEAR}-${CURR_MONTH} +1 months" '+%m'`
    CURR_YEAR=$NEW_YEAR
  done

  # reprocess data in monthly chunks upto and including the bookmark date, do not change bookmark in HEMS
  CURR_YEAR=$START_YEAR
  CURR_MONTH=$START_MONTH

  while [ "${CURR_YEAR}${CURR_MONTH}" \< "${END_YEAR}${END_MONTH}" -o "${CURR_YEAR}${CURR_MONTH}" = "${END_YEAR}${END_MONTH}" ]
  do
    START_DT=`date --date="${CURR_YEAR}-${CURR_MONTH}-01 00" '+%Y-%m-%d:%H'`
    END_DT=''
    if [ "${CURR_YEAR}${CURR_MONTH}" \< "${END_YEAR}${END_MONTH}" ]
    then
      END_DT=`date --date="${CURR_YEAR}-${CURR_MONTH}-01 00 +1 months -1 hours" '+%Y-%m-%d:%H'`
    elif [ "${CURR_YEAR}${CURR_MONTH}" = "${END_YEAR}${END_MONTH}" ]
    then
      END_DT=`date --date="${LAST_DT}" '+%Y-%m-%d:%H'` 
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
      LOG_FILE_NAME="hdp_first_assignment_hit_reprocess_${START_DATE}:${START_HOUR}-${END_DATE}:${END_HOUR}.log"
      time hive -hiveconf start.date="${START_DATE}" -hiveconf start.hour="${START_HOUR}" -hiveconf end.date="${END_DATE}" -hiveconf end.hour="${END_HOUR}" -hiveconf job.queue="${JOB_QUEUE}" -hiveconf hex.fah.db="${FAH_DB}" -hiveconf hex.fah.table="${FAH_TABLE}" -hiveconf hex.lib="${HEX_LIB}" -hiveconf hex.version="${HEX_VERSION}" -f $SCRIPT_PATH/insert_ETL_HEX_ASSIGNMENT_HIT.hql >> $HEX_LOGS/$LOG_FILE_NAME 2>&1 
    fi

    NEW_YEAR=`date --date="${CURR_YEAR}-${CURR_MONTH}-01 00 +1 months" '+%Y'`
    CURR_MONTH=`date --date="${CURR_YEAR}-${CURR_MONTH}-01 00 +1 months" '+%m'`
    CURR_YEAR=$NEW_YEAR
  done
  if [ -z $LAST_DT ]; then
    `_WRITE_PROCESS_CONTEXT "$PROCESS_ID" "BOOKMARK" "$END_DATE $END_HOUR"`
  fi
  `_WRITE_PROCESS_CONTEXT "$PROCESS_ID" "PROCESSING_TYPE" "D"`
  
else
  # daily incremental load: uses list files to determine max contiguous delta upto 24 hrs available at source from the bookmark date
  START_DT=`date --date="${LAST_DT} +1 hours" '+%Y-%m-%d:%H'`
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
    LOG_FILE_NAME="hdp_first_assignment_hit_${START_DATE}:${START_HOUR}-${END_DATE}:${END_HOUR}.log"
    time hive -hiveconf start.date="${START_DATE}" -hiveconf start.hour="${START_HOUR}" -hiveconf end.date="${END_DATE}" -hiveconf end.hour="${END_HOUR}" -hiveconf job.queue="${JOB_QUEUE}" -hiveconf hex.fah.db="${FAH_DB}" -hiveconf hex.fah.table="${FAH_TABLE}" -hiveconf hex.lib="${HEX_LIB}" -hiveconf hex.version="${HEX_VERSION}" -f $SCRIPT_PATH/insert_ETL_HEX_ASSIGNMENT_HIT.hql >> $HEX_LOGS/$LOG_FILE_NAME 2>&1 && _WRITE_PROCESS_CONTEXT "$PROCESS_ID" "BOOKMARK" "$END_DATE $END_HOUR" 
  fi
fi


_FREE_LOCK $HWW_FAH_LOCK_NAME

