#!/bin/bash

###############################################################################
#Copyright (C) 2013 Expedia, Inc. All rights reserved.
#
#Description:
#  Installation script for HEX ETL.
#
#Change History:
#  Date        Author         Description
#  ----------  -------------- ------------------------------------
#  2013-09-13  achadha        Created
#  2013-02-24  nsucheendran	  Introduce STEP_LOAD_DB2_SP
###############################################################################
set -u
set +e


export PLAT_HOME=/usr/local/edw/platform
source $PLAT_HOME/common/sh_metadata_storage.sh

HWW_HOME=/usr/etl/HWW
MODULE_NAME=hdp_hww_hex_etl
MODULE_LN=$HWW_HOME/$MODULE_NAME
export MODULE_PATH=$MODULE_LN

CURR_PATH=`dirname $0`

SCRIPT_PATH_OMNI_HIT=$CURR_PATH/../scripts/hql/OMNI_HIT
SCRIPT_PATH_TRANS=$CURR_PATH/../scripts/hql/TRANS
SCRIPT_PATH_FACT=$CURR_PATH/../scripts/hql/FACT
SCRIPT_PATH_AGG=$CURR_PATH/../scripts/hql/AGG
SCRIPT_PATH_SEG=$CURR_PATH/../scripts/hql/SEG
SCRIPT_PATH_REP=$CURR_PATH/../scripts/hql/REP
JAR_PATH=$(ls $CURR_PATH/../jars/${MODULE_NAME}*.jar)
JAR_DEST_PATH=/app/edw/hive/auxlib/$MODULE_NAME.jar

ETL_USER=hwwetl
PLAT_USER=platetl
FAH_TABLE='ETL_HCOM_HEX_FIRST_ASSIGNMENT_HIT'
TRANS_TABLE='ETL_HCOM_HEX_TRANSACTIONS'

ACTIVE_FAH_TABLE='ETL_HCOM_HEX_ACTIVE_FIRST_ASSIGNMENT_HIT'
FACT_STAGE_TABLE='ETL_HCOM_HEX_FACT_STAGING'
FACT_UNPARTED_TABLE='ETL_HCOM_HEX_FACT_UNPARTED'
FACT_TABLE='ETL_HCOM_HEX_FACT'
FACT_AGG_TABLE='RPT_HEXDM_AGG'
FACT_AGG_UNPARTED_TABLE='RPT_HEXDM_AGG_UNPARTED'
REPORT_TABLE='etl_hex_reporting_requirements'
REPORT_FILE='/autofs/edwfileserver/sherlock_in/HEX/HEX2_REPORTING_INPUT.csv'
KEYS_COUNT_LIMIT=100000;
AGG_NUM_REDUCERS=800;
REP_BATCH_SIZE=500;
FACT_LOAD_SPLIT_SIZE=1073741824;
EMAIL_TO='agurumurthi@expedia.com,nsucheendran@expedia.com'
EMAIL_CC='achadha@expedia.com,nsood@expedia.com'
EMAIL_SUCCESS_TO='dwhwwhex@expedia.com'
EMAIL_SUCCESS_CC='agurumurthi@expedia.com'
SEG_NUM_REDUCERS=800;
SEG_UNPARTED_TABLE='RPT_HEXDM_SEG_UNPARTED'
SEG_INPUT_FILE_PATH='/autofs/edwfileserver/sherlock_in/HEX/segmentations.txt'
SEG_TABLE='RPT_HEXDM_SEG'
SEG_EXP_LIST_TABLE='RPT_HEXDM_SEGMENT_EXP_LIST'

AGG_DB='DM'

FAH_DB='ETLDATA'
JOB_QUEUE='hwwetl'
REPROCESS_START_YEAR='2012'
REPROCESS_START_MONTH='11'

FACT_REDUCERS='800'
STEP_LOAD_REPORTING_REQUIREMENTS=1
STEP_LOAD_STAGING_DATA=2
STEP_LOAD_FACT_DATA=3
STEP_LOAD_AGG_DATA=4
STEP_LOAD_SEG_DATA=5
STEP_LOAD_DB2_DATA=6
STEP_LOAD_DB2_SP=7
STEP_LOAD_PARTITIONED_DATA=8
PARTED_SEG_LOAD="true"
PARTED_AGG_LOAD="true"
PARTED_FACT_LOAD="true"
STEP_TO_PROCESS_FROM=1

FAH_PROCESS_NAME="ETL_HCOM_HEX_FIRST_ASSIGNMENT_HIT_TRANS"
_LOG "Configuring process $FAH_PROCESS_NAME ..."

FAH_PROCESS_DESCRIPTION="Loads HEX First Assignment Hit Data"

FAH_PROCESS_ID=$(_GET_PROCESS_ID "$FAH_PROCESS_NAME")
if [ -z "$FAH_PROCESS_ID" ]; then
  _LOG "(re-)creating table $FAH_TABLE ..." 
  _LOG "disable nodrop - OK if errors here." 
  set +o errexit 
  sudo -E -u $ETL_USER hive -e "use $FAH_DB; alter table $FAH_TABLE disable NO_DROP;" 
  set -o errexit 
  _LOG "disable nodrop ended." 
  if sudo -E -u $ETL_USER hdfs dfs -test -e /data/HWW/$FAH_DB/$FAH_TABLE; then 
    _LOG "removing existing table files ... " 
    sudo -E -u $ETL_USER hdfs dfs -rm -R /data/HWW/$FAH_DB/$FAH_TABLE 
    if [ $? -ne 0 ]; then
      _LOG "Error deleting table files. Installation FAILED."
      exit 1
    fi
  fi 
  sudo -E -u $ETL_USER hive -hiveconf job.queue="${JOB_QUEUE}" -hiveconf hex.fah.db="${FAH_DB}" -hiveconf hex.fah.table="${FAH_TABLE}" -f $SCRIPT_PATH_OMNI_HIT/createTable_ETL_HCOM_HEX_ASSIGNMENT_HIT.hql
  if [ $? -ne 0 ]; then
    _LOG "Error creating table. Installation FAILED."
    exit 1
  fi
  _LOG "(re-)creating table $FAH_TABLE Done." 

  
  sudo -E -u $ETL_USER hdfs dfs -chmod -R 775 "/data/HWW/$FAH_DB/${FAH_TABLE}" ;
  sudo -E -u $ETL_USER hdfs dfs -chmod -R 775 "/data/HWW/$FAH_DB/${TRANS_TABLE}" ;

  $PLAT_HOME/tools/metadata/add_process.sh "$FAH_PROCESS_NAME" "$FAH_PROCESS_DESCRIPTION"
  if [ $? -ne 0 ]; then
    _LOG "Error adding process. Installation FAILED."
    exit 1
  fi
  FAH_PROCESS_ID=$(_GET_PROCESS_ID "$FAH_PROCESS_NAME")
  _WRITE_PROCESS_CONTEXT $FAH_PROCESS_ID "FAH_TABLE" "$FAH_TABLE"
  if [ $? -ne 0 ]; then
    _LOG "Error writing process context. Installation FAILED."
    $PLAT_HOME/tools/metadata/delete_process.sh "$FAH_PROCESS_NAME"
    exit 1
  fi
  _WRITE_PROCESS_CONTEXT $FAH_PROCESS_ID "TRANS_TABLE" "$TRANS_TABLE"
  if [ $? -ne 0 ]; then
    _LOG "Error writing process context. Installation FAILED."
    $PLAT_HOME/tools/metadata/delete_process.sh "$FAH_PROCESS_NAME"
    exit 1
  fi
  _WRITE_PROCESS_CONTEXT $FAH_PROCESS_ID "FAH_DB" "$FAH_DB"
  if [ $? -ne 0 ]; then
    _LOG "Error writing process context. Installation FAILED."
    $PLAT_HOME/tools/metadata/delete_process.sh "$FAH_PROCESS_NAME"
    exit 1
  fi
  _WRITE_PROCESS_CONTEXT $FAH_PROCESS_ID "JOB_QUEUE" "$JOB_QUEUE"
  if [ $? -ne 0 ]; then
    _LOG "Error writing process context. Installation FAILED."
    $PLAT_HOME/tools/metadata/delete_process.sh "$FAH_PROCESS_NAME"
    exit 1
  fi
  _WRITE_PROCESS_CONTEXT $FAH_PROCESS_ID "DELTA_CAP" "23"
  if [ $? -ne 0 ]; then
    _LOG "Error writing process context. Installation FAILED."
    $PLAT_HOME/tools/metadata/delete_process.sh "$FAH_PROCESS_NAME"
    exit 1
  fi
  _WRITE_PROCESS_CONTEXT $FAH_PROCESS_ID "EMAIL_TO" "$EMAIL_TO"
  if [ $? -ne 0 ]; then
    _LOG "Error writing process context. Installation FAILED."
    $PLAT_HOME/tools/metadata/delete_process.sh "$FAH_PROCESS_NAME"
    exit 1
  fi
  _WRITE_PROCESS_CONTEXT $FAH_PROCESS_ID "EMAIL_CC" "$EMAIL_CC"
  if [ $? -ne 0 ]; then
    _LOG "Error writing process context. Installation FAILED."
    $PLAT_HOME/tools/metadata/delete_process.sh "$FAH_PROCESS_NAME"
    exit 1
  fi
  _WRITE_PROCESS_CONTEXT $FAH_PROCESS_ID "BOOKMARK" "`date -d " -2 days" "+%Y-%m-%d 00"`"
  if [ $? -ne 0 ]; then
    _LOG "Error writing process context. Installation FAILED."
    $PLAT_HOME/tools/metadata/delete_process.sh "$FAH_PROCESS_NAME"
    exit 1
  fi
  _WRITE_PROCESS_CONTEXT $FAH_PROCESS_ID "PROCESSING_TYPE" "R"
  if [ $? -ne 0 ]; then
    _LOG "Error writing process context. Installation FAILED."
    $PLAT_HOME/tools/metadata/delete_process.sh "$FAH_PROCESS_NAME"
    exit 1
  fi
  _WRITE_PROCESS_CONTEXT $FAH_PROCESS_ID "REPROCESS_SCOPE" "OMNI_HIT_TRANS"
  if [ $? -ne 0 ]; then
    _LOG "Error writing process context. Installation FAILED."
    $PLAT_HOME/tools/metadata/delete_process.sh "$FAH_PROCESS_NAME"
    exit 1
  fi
  _WRITE_PROCESS_CONTEXT $FAH_PROCESS_ID "REPROCESS_START_YEAR" "$REPROCESS_START_YEAR"
  if [ $? -ne 0 ]; then
    _LOG "Error writing process context. Installation FAILED."
    $PLAT_HOME/tools/metadata/delete_process.sh "$FAH_PROCESS_NAME"
    exit 1
  fi
  _WRITE_PROCESS_CONTEXT $FAH_PROCESS_ID "REPROCESS_START_MONTH" "$REPROCESS_START_MONTH"
  if [ $? -ne 0 ]; then
    _LOG "Error writing process context. Installation FAILED."
    $PLAT_HOME/tools/metadata/delete_process.sh "$FAH_PROCESS_NAME"
    exit 1
  fi
else
  _LOG "Process $FAH_PROCESS_NAME already exists"
fi

_LOG "Process $FAH_PROCESS_NAME configured successfully"

##########################
# BKG_TRANS Deployment
##########################

TRANS_PROCESS_NAME="ETL_HCOM_HEX_TRANSACTIONS_BKG"
_LOG "Configuring process $TRANS_PROCESS_NAME ..."

TRANS_PROCESS_DESCRIPTION="Loads HEX Booking Transactions Data"

TRANS_PROCESS_ID=$(_GET_PROCESS_ID "$TRANS_PROCESS_NAME")
if [ -z "$TRANS_PROCESS_ID" ]; then
  
  _LOG "(re-)creating table $TRANS_TABLE ..." 
  _LOG "disable nodrop - OK if errors here." 
  set +o errexit 
  sudo -E -u $ETL_USER hive -e "use $FAH_DB; alter table $TRANS_TABLE disable NO_DROP;" 
  set -o errexit 
  _LOG "disable nodrop ended." 
  if sudo -E -u $ETL_USER hdfs dfs -test -e /data/HWW/$FAH_DB/$TRANS_TABLE; then 
    _LOG "removing existing table files ... " 
    sudo -E -u $ETL_USER hdfs dfs -rm -R /data/HWW/$FAH_DB/$TRANS_TABLE 
    if [ $? -ne 0 ]; then
      _LOG "Error deleting table files. Installation FAILED."
      exit 1
    fi
  fi 
  sudo -E -u $ETL_USER hive -hiveconf job.queue="${JOB_QUEUE}" -hiveconf hex.fah.db="${FAH_DB}" -hiveconf hex.trans.table="${TRANS_TABLE}" -f $SCRIPT_PATH_TRANS/createTable_ETL_HCOM_HEX_TRANSACTIONS.hql
  if [ $? -ne 0 ]; then
    _LOG "Error creating table. Installation FAILED."
    exit 1
  fi
  _LOG "(re-)creating table $TRANS_TABLE Done." 

  $PLAT_HOME/tools/metadata/add_process.sh "$TRANS_PROCESS_NAME" "$TRANS_PROCESS_DESCRIPTION"
  if [ $? -ne 0 ]; then
    _LOG "Error adding process. Installation FAILED."
    exit 1
  fi
  TRANS_PROCESS_ID=$(_GET_PROCESS_ID "$TRANS_PROCESS_NAME")
  _WRITE_PROCESS_CONTEXT $TRANS_PROCESS_ID "TRANS_TABLE" "$TRANS_TABLE"
  if [ $? -ne 0 ]; then
    _LOG "Error writing process context. Installation FAILED."
    $PLAT_HOME/tools/metadata/delete_process.sh "$TRANS_PROCESS_NAME"
    exit 1
  fi
  _WRITE_PROCESS_CONTEXT $TRANS_PROCESS_ID "TRANS_DB" "$FAH_DB"
  if [ $? -ne 0 ]; then
    _LOG "Error writing process context. Installation FAILED."
    $PLAT_HOME/tools/metadata/delete_process.sh "$TRANS_PROCESS_NAME"
    exit 1
  fi
  _WRITE_PROCESS_CONTEXT $TRANS_PROCESS_ID "JOB_QUEUE" "$JOB_QUEUE"
  if [ $? -ne 0 ]; then
    _LOG "Error writing process context. Installation FAILED."
    $PLAT_HOME/tools/metadata/delete_process.sh "$TRANS_PROCESS_NAME"
    exit 1
  fi
  _WRITE_PROCESS_CONTEXT $TRANS_PROCESS_ID "EMAIL_TO" "$EMAIL_TO"
  if [ $? -ne 0 ]; then
    _LOG "Error writing process context. Installation FAILED."
    $PLAT_HOME/tools/metadata/delete_process.sh "$TRANS_PROCESS_NAME"
    exit 1
  fi
  _WRITE_PROCESS_CONTEXT $TRANS_PROCESS_ID "EMAIL_CC" "$EMAIL_CC"
  if [ $? -ne 0 ]; then
    _LOG "Error writing process context. Installation FAILED."
    $PLAT_HOME/tools/metadata/delete_process.sh "$TRANS_PROCESS_NAME"
    exit 1
  fi
  _WRITE_PROCESS_CONTEXT $TRANS_PROCESS_ID "BOOKMARK" "`date -d " -2 days" "+%Y-%m-%d"`"
  if [ $? -ne 0 ]; then
    _LOG "Error writing process context. Installation FAILED."
    $PLAT_HOME/tools/metadata/delete_process.sh "$TRANS_PROCESS_NAME"
    exit 1
  fi
  _WRITE_PROCESS_CONTEXT $TRANS_PROCESS_ID "PROCESSING_TYPE" "R"
  if [ $? -ne 0 ]; then
    _LOG "Error writing process context. Installation FAILED."
    $PLAT_HOME/tools/metadata/delete_process.sh "$TRANS_PROCESS_NAME"
    exit 1
  fi
  _WRITE_PROCESS_CONTEXT $TRANS_PROCESS_ID "REPROCESS_START_YEAR" "$REPROCESS_START_YEAR"
  if [ $? -ne 0 ]; then
    _LOG "Error writing process context. Installation FAILED."
    $PLAT_HOME/tools/metadata/delete_process.sh "$TRANS_PROCESS_NAME"
    exit 1
  fi
  _WRITE_PROCESS_CONTEXT $TRANS_PROCESS_ID "REPROCESS_START_MONTH" "$REPROCESS_START_MONTH"
  if [ $? -ne 0 ]; then
    _LOG "Error writing process context. Installation FAILED."
    $PLAT_HOME/tools/metadata/delete_process.sh "$TRANS_PROCESS_NAME"
    exit 1
  fi
  # 
  sudo -E -u $PLAT_USER hdfs dfs -rm -f /etl/common/ETLDATA/meta-inf/V_ETLDM_HCOM_BKG_ORDER_XREF_HEX.mf
  sudo -E -u $ETL_USER hdfs dfs -rm -r -f /data/common/ETLDATA/ETLDM_HCOM_BKG_ORDER_XREF_HEX
  sudo -E -u $ETL_USER hive -e "use ETLDATA; drop table if exists ETLDM_HCOM_BKG_ORDER_XREF_HEX;"

else
  _LOG "Process $TRANS_PROCESS_NAME already exists"
fi

_LOG "Process $TRANS_PROCESS_NAME configured successfully"


##########################
# FACT load Deployment
##########################

FACT_PROCESS_NAME="ETL_HCOM_HEX_FACT"
_LOG "Configuring process $FACT_PROCESS_NAME ..."

FACT_PROCESS_DESCRIPTION="Loads HEX FACT Data"

FACT_PROCESS_ID=$(_GET_PROCESS_ID "$FACT_PROCESS_NAME")
if [ -z "$FACT_PROCESS_ID" ]; then
  
_LOG "(re-)creating table $SEG_EXP_LIST_TABLE ..." 
_LOG "disable nodrop - OK if errors here." 
set +o errexit 
sudo -E -u $ETL_USER hive -e "use $AGG_DB; alter table $SEG_EXP_LIST_TABLE disable NO_DROP;" 
set -o errexit 
_LOG "disable nodrop ended." 
if sudo -E -u $ETL_USER hdfs dfs -test -e /data/HWW/$AGG_DB/$SEG_EXP_LIST_TABLE; then 
  _LOG "removing existing table files ... " 
  sudo -E -u $ETL_USER hdfs dfs -rm -R /data/HWW/$AGG_DB/$SEG_EXP_LIST_TABLE 
  if [ $? -ne 0 ]; then
    _LOG "Error deleting table files. Installation FAILED."
    exit 1
  fi
fi 
sudo -E -u $ETL_USER hive -hiveconf job.queue="${JOB_QUEUE}" -hiveconf hex.db="${AGG_DB}" -hiveconf hex.table="${SEG_EXP_LIST_TABLE}" -f $SCRIPT_PATH_REP/createTable_SEG_EXP_LIST_TABLE.hql
if [ $? -ne 0 ]; then
  _LOG "Error creating table. Installation FAILED."
  exit 1
fi
_LOG "(re-)creating table $SEG_EXP_LIST_TABLE Done." 	

 
  _LOG "(re-)creating table $SEG_TABLE ..." 
  _LOG "disable nodrop - OK if errors here." 
  set +o errexit 
  sudo -E -u $ETL_USER hive -e "use $AGG_DB; alter table $SEG_TABLE disable NO_DROP;" 
  set -o errexit 
  _LOG "disable nodrop ended." 
  if sudo -E -u $ETL_USER hdfs dfs -test -e /data/HWW/$AGG_DB/$SEG_TABLE; then 
    _LOG "removing existing table files ... " 
    sudo -E -u $ETL_USER hdfs dfs -rm -R /data/HWW/$AGG_DB/$SEG_TABLE 
    if [ $? -ne 0 ]; then
      _LOG "Error deleting table files. Installation FAILED."
      exit 1
    fi
  fi 
  sudo -E -u $ETL_USER hive -hiveconf job.queue="${JOB_QUEUE}" -hiveconf hex.db="${AGG_DB}" -hiveconf hex.table="${SEG_TABLE}" -f $SCRIPT_PATH_SEG/createTable_ETL_HCOM_HEX_SEG.hql
  if [ $? -ne 0 ]; then
    _LOG "Error creating table. Installation FAILED."
    exit 1
  fi
  _LOG "(re-)creating table $SEG_TABLE Done." 	


  _LOG "(re-)creating table $SEG_UNPARTED_TABLE ..." 
  _LOG "disable nodrop - OK if errors here." 
  set +o errexit 
  sudo -E -u $ETL_USER hive -e "use $AGG_DB; alter table $SEG_UNPARTED_TABLE disable NO_DROP;" 
  set -o errexit 
  _LOG "disable nodrop ended." 
  if sudo -E -u $ETL_USER hdfs dfs -test -e /data/HWW/$AGG_DB/$SEG_UNPARTED_TABLE; then 
    _LOG "removing existing table files ... " 
    sudo -E -u $ETL_USER hdfs dfs -rm -R /data/HWW/$AGG_DB/$SEG_UNPARTED_TABLE 
    if [ $? -ne 0 ]; then
      _LOG "Error deleting table files. Installation FAILED."
      exit 1
    fi
  fi 
  sudo -E -u $ETL_USER hive -hiveconf job.queue="${JOB_QUEUE}" -hiveconf hex.db="${AGG_DB}" -hiveconf hex.table="${SEG_UNPARTED_TABLE}" -f $SCRIPT_PATH_SEG/createTable_RPT_HEXDM_SEG_UNPARTED.hql
  if [ $? -ne 0 ]; then
    _LOG "Error creating table. Installation FAILED."
    exit 1
  fi
  _LOG "(re-)creating table $SEG_UNPARTED_TABLE Done." 
  _LOG "(re-)creating table $ACTIVE_FAH_TABLE ..."
  _LOG "disable nodrop - OK if errors here." 
  set +o errexit 
  sudo -E -u $ETL_USER hive -e "use $FAH_DB; alter table $ACTIVE_FAH_TABLE disable NO_DROP;" 
  set -o errexit 
  _LOG "disable nodrop ended." 
  if sudo -E -u $ETL_USER hdfs dfs -test -e /data/HWW/$FAH_DB/$ACTIVE_FAH_TABLE; then 
    _LOG "removing existing table files ... " 
    sudo -E -u $ETL_USER hdfs dfs -rm -R /data/HWW/$FAH_DB/$ACTIVE_FAH_TABLE 
    if [ $? -ne 0 ]; then
      _LOG "Error deleting table files. Installation FAILED."
      exit 1
    fi
  fi 
  sudo -E -u $ETL_USER hive -hiveconf job.queue="${JOB_QUEUE}" -hiveconf hex.db="${FAH_DB}" -hiveconf hex.table="${ACTIVE_FAH_TABLE}" -f $SCRIPT_PATH_FACT/createTable_ETL_HCOM_HEX_ACTIVE_FIRST_ASSIGNMENT_HITS.hql
  if [ $? -ne 0 ]; then
    _LOG "Error creating table. Installation FAILED."
    exit 1
  fi
  _LOG "(re-)creating table $ACTIVE_FAH_TABLE Done." 


  _LOG "(re-)creating table $FACT_STAGE_TABLE ..." 
  _LOG "disable nodrop - OK if errors here." 
  set +o errexit 
  sudo -E -u $ETL_USER hive -e "use $FAH_DB; alter table $FACT_STAGE_TABLE disable NO_DROP;" 
  set -o errexit 
  _LOG "disable nodrop ended." 
  if sudo -E -u $ETL_USER hdfs dfs -test -e /data/HWW/$FAH_DB/$FACT_STAGE_TABLE; then 
    _LOG "removing existing table files ... " 
    sudo -E -u $ETL_USER hdfs dfs -rm -R /data/HWW/$FAH_DB/$FACT_STAGE_TABLE 
    if [ $? -ne 0 ]; then
      _LOG "Error deleting table files. Installation FAILED."
      exit 1
    fi
  fi 
  sudo -E -u $ETL_USER hive -hiveconf job.queue="${JOB_QUEUE}" -hiveconf hex.db="${FAH_DB}" -hiveconf hex.table="${FACT_STAGE_TABLE}" -f $SCRIPT_PATH_FACT/createTable_ETL_HCOM_HEX_FACT_STAGE.hql
  if [ $? -ne 0 ]; then
    _LOG "Error creating table. Installation FAILED."
    exit 1
  fi
  _LOG "(re-)creating table $FACT_STAGE_TABLE Done." 

  _LOG "(re-)creating table $FACT_TABLE ..." 
  _LOG "disable nodrop - OK if errors here." 
  set +o errexit 
  sudo -E -u $ETL_USER hive -e "use $FAH_DB; alter table $FACT_TABLE disable NO_DROP;" 
  set -o errexit 
  _LOG "disable nodrop ended." 
  if sudo -E -u $ETL_USER hdfs dfs -test -e /data/HWW/$FAH_DB/$FACT_TABLE; then 
    _LOG "removing existing table files ... " 
    sudo -E -u $ETL_USER hdfs dfs -rm -R /data/HWW/$FAH_DB/$FACT_TABLE 
    if [ $? -ne 0 ]; then
      _LOG "Error deleting table files. Installation FAILED."
      exit 1
    fi
  fi 
  sudo -E -u $ETL_USER hive -hiveconf job.queue="${JOB_QUEUE}" -hiveconf hex.db="${FAH_DB}" -hiveconf hex.table="${FACT_TABLE}" -f $SCRIPT_PATH_FACT/createTable_ETL_HCOM_HEX_FACT.hql
  if [ $? -ne 0 ]; then
    _LOG "Error creating table. Installation FAILED."
    exit 1
  fi
  _LOG "(re-)creating table $FACT_TABLE Done." 

  _LOG "(re-)creating table $FACT_UNPARTED_TABLE ..." 
  _LOG "disable nodrop - OK if errors here." 
  set +o errexit 
  sudo -E -u $ETL_USER hive -e "use $FAH_DB; alter table $FACT_UNPARTED_TABLE disable NO_DROP;" 
  set -o errexit 
  _LOG "disable nodrop ended." 
  if sudo -E -u $ETL_USER hdfs dfs -test -e /data/HWW/$FAH_DB/$FACT_UNPARTED_TABLE; then 
    _LOG "removing existing table files ... " 
    sudo -E -u $ETL_USER hdfs dfs -rm -R /data/HWW/$FAH_DB/$FACT_UNPARTED_TABLE 
    if [ $? -ne 0 ]; then
      _LOG "Error deleting table files. Installation FAILED."
      exit 1
    fi
  fi 
  sudo -E -u $ETL_USER hive -hiveconf job.queue="${JOB_QUEUE}" -hiveconf hex.db="${FAH_DB}" -hiveconf hex.table="${FACT_UNPARTED_TABLE}" -f $SCRIPT_PATH_FACT/createTable_ETL_HCOM_HEX_FACT_UNPARTED.hql
  if [ $? -ne 0 ]; then
    _LOG "Error creating table. Installation FAILED."
    exit 1
  fi
  _LOG "(re-)creating table $FACT_UNPARTED_TABLE Done." 



  _LOG "(re-)creating table $FACT_AGG_TABLE ..." 
  _LOG "disable nodrop - OK if errors here." 
  set +o errexit 
  sudo -E -u $ETL_USER hive -e "use $AGG_DB; alter table $FACT_AGG_TABLE disable NO_DROP;" 
  set -o errexit 
  _LOG "disable nodrop ended." 
  if sudo -E -u $ETL_USER hdfs dfs -test -e /data/HWW/$AGG_DB/$FACT_AGG_TABLE; then 
    _LOG "removing existing table files ... " 
    sudo -E -u $ETL_USER hdfs dfs -rm -R /data/HWW/$AGG_DB/$FACT_AGG_TABLE 
    if [ $? -ne 0 ]; then
      _LOG "Error deleting table files. Installation FAILED."
      exit 1
    fi
  fi 
  sudo -E -u $ETL_USER hive -hiveconf job.queue="${JOB_QUEUE}" -hiveconf hex.db="${AGG_DB}" -hiveconf hex.table="${FACT_AGG_TABLE}" -f $SCRIPT_PATH_AGG/createTable_ETL_HCOM_HEX_AGG.hql
  if [ $? -ne 0 ]; then
    _LOG "Error creating table. Installation FAILED."
    exit 1
  fi
  _LOG "(re-)creating table $FACT_AGG_TABLE Done." 	
	

  _LOG "(re-)creating table $FACT_AGG_UNPARTED_TABLE ..." 
  _LOG "disable nodrop - OK if errors here." 
  set +o errexit 
  sudo -E -u $ETL_USER hive -e "use $AGG_DB; alter table $FACT_AGG_UNPARTED_TABLE disable NO_DROP;" 
  set -o errexit 
  _LOG "disable nodrop ended." 
  if sudo -E -u $ETL_USER hdfs dfs -test -e /data/HWW/$AGG_DB/$FACT_AGG_UNPARTED_TABLE; then 
    _LOG "removing existing table files ... " 
    sudo -E -u $ETL_USER hdfs dfs -rm -R /data/HWW/$AGG_DB/$FACT_AGG_UNPARTED_TABLE 
    if [ $? -ne 0 ]; then
      _LOG "Error deleting table files. Installation FAILED."
      exit 1
    fi
  fi 
  sudo -E -u $ETL_USER hive -hiveconf job.queue="${JOB_QUEUE}" -hiveconf hex.db="${AGG_DB}" -hiveconf hex.agg.unparted.table="${FACT_AGG_UNPARTED_TABLE}" -f $SCRIPT_PATH_AGG/createTable_ETL_HCOM_HEX_AGG_UNPARTED.hql
  if [ $? -ne 0 ]; then
    _LOG "Error creating table. Installation FAILED."
    exit 1
  fi
  _LOG "(re-)creating table $FACT_AGG_UNPARTED_TABLE Done." 	
  
  $PLAT_HOME/tools/metadata/add_process.sh "$FACT_PROCESS_NAME" "$FACT_PROCESS_DESCRIPTION"
  if [ $? -ne 0 ]; then
    _LOG "Error adding process. Installation FAILED."
    exit 1
  fi

  FACT_PROCESS_ID=$(_GET_PROCESS_ID "$FACT_PROCESS_NAME")
  _WRITE_PROCESS_CONTEXT $FACT_PROCESS_ID "ACTIVE_FAH_TABLE" "$ACTIVE_FAH_TABLE"
  if [ $? -ne 0 ]; then
    _LOG "Error writing process context. Installation FAILED."
    $PLAT_HOME/tools/metadata/delete_process.sh "$FACT_PROCESS_NAME"
    exit 1
  fi

  _WRITE_PROCESS_CONTEXT $FACT_PROCESS_ID "FACT_STAGE_TABLE" "$FACT_STAGE_TABLE"
  if [ $? -ne 0 ]; then
    _LOG "Error writing process context. Installation FAILED."
    $PLAT_HOME/tools/metadata/delete_process.sh "$FACT_PROCESS_NAME"
    exit 1
  fi
  _WRITE_PROCESS_CONTEXT $FACT_PROCESS_ID "FAH_PROCESS_ID" "$FAH_PROCESS_ID"
  if [ $? -ne 0 ]; then
    _LOG "Error writing process context. Installation FAILED."
    $PLAT_HOME/tools/metadata/delete_process.sh "$FACT_PROCESS_NAME"
    exit 1
  fi
  _WRITE_PROCESS_CONTEXT $FACT_PROCESS_ID "BKG_PROCESS_ID" "$TRANS_PROCESS_ID"
  if [ $? -ne 0 ]; then
    _LOG "Error writing process context. Installation FAILED."
    $PLAT_HOME/tools/metadata/delete_process.sh "$FACT_PROCESS_NAME"
    exit 1
  fi
  
  _WRITE_PROCESS_CONTEXT $FACT_PROCESS_ID "STAGE_DB" "$FAH_DB"
  if [ $? -ne 0 ]; then
    _LOG "Error writing process context. Installation FAILED."
    $PLAT_HOME/tools/metadata/delete_process.sh "$FACT_PROCESS_NAME"
    exit 1
  fi
  _WRITE_PROCESS_CONTEXT $FACT_PROCESS_ID "JOB_QUEUE" "$JOB_QUEUE"
  if [ $? -ne 0 ]; then
    _LOG "Error writing process context. Installation FAILED."
    $PLAT_HOME/tools/metadata/delete_process.sh "$FACT_PROCESS_NAME"
    exit 1
  fi

  _WRITE_PROCESS_CONTEXT $FACT_PROCESS_ID "EMAIL_TO" "$EMAIL_TO"
  if [ $? -ne 0 ]; then
    _LOG "Error writing process context. Installation FAILED."
    $PLAT_HOME/tools/metadata/delete_process.sh "$FACT_PROCESS_NAME"
    exit 1
  fi
  _WRITE_PROCESS_CONTEXT $FACT_PROCESS_ID "EMAIL_CC" "$EMAIL_CC"
  if [ $? -ne 0 ]; then
    _LOG "Error writing process context. Installation FAILED."
    $PLAT_HOME/tools/metadata/delete_process.sh "$FACT_PROCESS_NAME"
    exit 1
  fi
  _WRITE_PROCESS_CONTEXT $FACT_PROCESS_ID "SRC_BOOKMARK_OMNI" "2012-10-31 00"
  if [ $? -ne 0 ]; then
    _LOG "Error writing process context. Installation FAILED."
    $PLAT_HOME/tools/metadata/delete_process.sh "$FACT_PROCESS_NAME"
    exit 1
  fi
  _WRITE_PROCESS_CONTEXT $FACT_PROCESS_ID "SRC_BOOKMARK_BKG" "2012-10-31"
  if [ $? -ne 0 ]; then
    _LOG "Error writing process context. Installation FAILED."
    $PLAT_HOME/tools/metadata/delete_process.sh "$FACT_PROCESS_NAME"
    exit 1
  fi
  _WRITE_PROCESS_CONTEXT $FACT_PROCESS_ID "PROCESSING_TYPE" "R"
  if [ $? -ne 0 ]; then
    _LOG "Error writing process context. Installation FAILED."
    $PLAT_HOME/tools/metadata/delete_process.sh "$FACT_PROCESS_NAME"
    exit 1
  fi
  _WRITE_PROCESS_CONTEXT $FACT_PROCESS_ID "REPROCESS_START_YEAR" "$REPROCESS_START_YEAR"
  if [ $? -ne 0 ]; then
    _LOG "Error writing process context. Installation FAILED."
    $PLAT_HOME/tools/metadata/delete_process.sh "$FACT_PROCESS_NAME"
    exit 1
  fi
  _WRITE_PROCESS_CONTEXT $FACT_PROCESS_ID "REPROCESS_START_MONTH" "$REPROCESS_START_MONTH"
  if [ $? -ne 0 ]; then
    _LOG "Error writing process context. Installation FAILED."
    $PLAT_HOME/tools/metadata/delete_process.sh "$FACT_PROCESS_NAME"
    exit 1
  fi
else
  _LOG "Process $FACT_PROCESS_NAME already exists"
fi

_WRITE_PROCESS_CONTEXT $FACT_PROCESS_ID "EMAIL_SUCCESS_TO" "$EMAIL_SUCCESS_TO"
if [ $? -ne 0 ]; then
  _LOG "Error writing process context. Installation FAILED."
  $PLAT_HOME/tools/metadata/delete_process.sh "$FAH_PROCESS_NAME"
  exit 1
fi

_WRITE_PROCESS_CONTEXT $FACT_PROCESS_ID "EMAIL_SUCCESS_CC" "$EMAIL_SUCCESS_CC"
if [ $? -ne 0 ]; then
  _LOG "Error writing process context. Installation FAILED."
  $PLAT_HOME/tools/metadata/delete_process.sh "$FAH_PROCESS_NAME"
  exit 1
fi
_WRITE_PROCESS_CONTEXT $FACT_PROCESS_ID "SEG_EXP_LIST_TABLE" "$SEG_EXP_LIST_TABLE"
if [ $? -ne 0 ]; then
  _LOG "Error writing process context. Installation FAILED."
  $PLAT_HOME/tools/metadata/delete_process.sh "$FACT_PROCESS_NAME"
  exit 1
fi
_WRITE_PROCESS_CONTEXT $FACT_PROCESS_ID "REPORT_FILE" "$REPORT_FILE"
if [ $? -ne 0 ]; then
  _LOG "Error writing process context. Installation FAILED."
  $PLAT_HOME/tools/metadata/delete_process.sh "$FACT_PROCESS_NAME"
  exit 1
fi
_WRITE_PROCESS_CONTEXT $FACT_PROCESS_ID "STEP_LOAD_REPORTING_REQUIREMENTS" "$STEP_LOAD_REPORTING_REQUIREMENTS"
if [ $? -ne 0 ]; then
  _LOG "Error writing process context. Installation FAILED."
  $PLAT_HOME/tools/metadata/delete_process.sh "$FACT_PROCESS_NAME"
  exit 1
fi
_WRITE_PROCESS_CONTEXT $FACT_PROCESS_ID "STEP_LOAD_STAGING_DATA" "$STEP_LOAD_STAGING_DATA"
if [ $? -ne 0 ]; then
  _LOG "Error writing process context. Installation FAILED."
  $PLAT_HOME/tools/metadata/delete_process.sh "$FACT_PROCESS_NAME"
  exit 1
fi
_WRITE_PROCESS_CONTEXT $FACT_PROCESS_ID "STEP_LOAD_FACT_DATA" "$STEP_LOAD_FACT_DATA"
if [ $? -ne 0 ]; then
  _LOG "Error writing process context. Installation FAILED."
  $PLAT_HOME/tools/metadata/delete_process.sh "$FACT_PROCESS_NAME"
  exit 1
fi
_WRITE_PROCESS_CONTEXT $FACT_PROCESS_ID "STEP_LOAD_AGG_DATA" "$STEP_LOAD_AGG_DATA"
if [ $? -ne 0 ]; then
  _LOG "Error writing process context. Installation FAILED."
  $PLAT_HOME/tools/metadata/delete_process.sh "$FACT_PROCESS_NAME"
  exit 1
fi
_WRITE_PROCESS_CONTEXT $FACT_PROCESS_ID "STEP_LOAD_SEG_DATA" "$STEP_LOAD_SEG_DATA"
if [ $? -ne 0 ]; then
  _LOG "Error writing process context. Installation FAILED."
  $PLAT_HOME/tools/metadata/delete_process.sh "$FACT_PROCESS_NAME"
  exit 1
fi
_WRITE_PROCESS_CONTEXT $FACT_PROCESS_ID "STEP_LOAD_DB2_DATA" "$STEP_LOAD_DB2_DATA"
if [ $? -ne 0 ]; then
  _LOG "Error writing process context. Installation FAILED."
  $PLAT_HOME/tools/metadata/delete_process.sh "$FACT_PROCESS_NAME"
  exit 1
fi
_WRITE_PROCESS_CONTEXT $FACT_PROCESS_ID "STEP_LOAD_DB2_SP" "$STEP_LOAD_DB2_SP"
if [ $? -ne 0 ]; then
  _LOG "Error writing process context. Installation FAILED."
  $PLAT_HOME/tools/metadata/delete_process.sh "$FACT_PROCESS_NAME"
  exit 1
fi
_WRITE_PROCESS_CONTEXT $FACT_PROCESS_ID "STEP_LOAD_PARTITIONED_DATA" "$STEP_LOAD_PARTITIONED_DATA"
if [ $? -ne 0 ]; then
  _LOG "Error writing process context. Installation FAILED."
  $PLAT_HOME/tools/metadata/delete_process.sh "$FACT_PROCESS_NAME"
  exit 1
fi
_WRITE_PROCESS_CONTEXT $FACT_PROCESS_ID "PARTED_SEG_LOAD" "$PARTED_SEG_LOAD"
if [ $? -ne 0 ]; then
  _LOG "Error writing process context. Installation FAILED."
  $PLAT_HOME/tools/metadata/delete_process.sh "$FACT_PROCESS_NAME"
  exit 1
fi
_WRITE_PROCESS_CONTEXT $FACT_PROCESS_ID "PARTED_AGG_LOAD" "$PARTED_AGG_LOAD"
if [ $? -ne 0 ]; then
  _LOG "Error writing process context. Installation FAILED."
  $PLAT_HOME/tools/metadata/delete_process.sh "$FACT_PROCESS_NAME"
  exit 1
fi
_WRITE_PROCESS_CONTEXT $FACT_PROCESS_ID "PARTED_FACT_LOAD" "$PARTED_FACT_LOAD"
if [ $? -ne 0 ]; then
  _LOG "Error writing process context. Installation FAILED."
  $PLAT_HOME/tools/metadata/delete_process.sh "$FACT_PROCESS_NAME"
  exit 1
fi
_WRITE_PROCESS_CONTEXT $FACT_PROCESS_ID "STEP_TO_PROCESS_FROM" "$STEP_TO_PROCESS_FROM"
if [ $? -ne 0 ]; then
  _LOG "Error writing process context. Installation FAILED."
  $PLAT_HOME/tools/metadata/delete_process.sh "$FACT_PROCESS_NAME"
  exit 1
fi

_WRITE_PROCESS_CONTEXT $FACT_PROCESS_ID "SEG_NUM_REDUCERS" "$SEG_NUM_REDUCERS"
if [ $? -ne 0 ]; then
  _LOG "Error writing process context. Installation FAILED."
  $PLAT_HOME/tools/metadata/delete_process.sh "$FACT_PROCESS_NAME"
  exit 1
fi
_WRITE_PROCESS_CONTEXT $FACT_PROCESS_ID "SEG_UNPARTED_TABLE" "$SEG_UNPARTED_TABLE"
if [ $? -ne 0 ]; then
  _LOG "Error writing process context. Installation FAILED."
  $PLAT_HOME/tools/metadata/delete_process.sh "$FACT_PROCESS_NAME"
  exit 1
fi
_WRITE_PROCESS_CONTEXT $FACT_PROCESS_ID "SEG_INPUT_FILE_PATH" "$SEG_INPUT_FILE_PATH"
if [ $? -ne 0 ]; then
  _LOG "Error writing process context. Installation FAILED."
  $PLAT_HOME/tools/metadata/delete_process.sh "$FACT_PROCESS_NAME"
  exit 1
fi
_WRITE_PROCESS_CONTEXT $FACT_PROCESS_ID "SEG_TABLE" "$SEG_TABLE"
if [ $? -ne 0 ]; then
  _LOG "Error writing process context. Installation FAILED."
  $PLAT_HOME/tools/metadata/delete_process.sh "$FACT_PROCESS_NAME"
  exit 1
fi

_WRITE_PROCESS_CONTEXT $FACT_PROCESS_ID "KEYS_COUNT_LIMIT" "$KEYS_COUNT_LIMIT"
if [ $? -ne 0 ]; then
  _LOG "Error writing process context. Installation FAILED."
  $PLAT_HOME/tools/metadata/delete_process.sh "$FACT_PROCESS_NAME"
  exit 1
fi
_WRITE_PROCESS_CONTEXT $FACT_PROCESS_ID "AGG_NUM_REDUCERS" "$AGG_NUM_REDUCERS"
if [ $? -ne 0 ]; then
  _LOG "Error writing process context. Installation FAILED."
  $PLAT_HOME/tools/metadata/delete_process.sh "$FACT_PROCESS_NAME"
  exit 1
fi
_WRITE_PROCESS_CONTEXT $FACT_PROCESS_ID "REP_BATCH_SIZE" "$REP_BATCH_SIZE"
if [ $? -ne 0 ]; then
  _LOG "Error writing process context. Installation FAILED."
  $PLAT_HOME/tools/metadata/delete_process.sh "$FACT_PROCESS_NAME"
  exit 1
fi
_WRITE_PROCESS_CONTEXT $FACT_PROCESS_ID "AGG_TABLE" "$FACT_AGG_TABLE"
if [ $? -ne 0 ]; then
  _LOG "Error writing process context. Installation FAILED."
  $PLAT_HOME/tools/metadata/delete_process.sh "$FACT_PROCESS_NAME"
  exit 1
fi
_WRITE_PROCESS_CONTEXT $FACT_PROCESS_ID "FACT_AGG_UNPARTED_TABLE" "$FACT_AGG_UNPARTED_TABLE"
if [ $? -ne 0 ]; then
  _LOG "Error writing process context. Installation FAILED."
  $PLAT_HOME/tools/metadata/delete_process.sh "$FACT_PROCESS_NAME"
  exit 1
fi
_WRITE_PROCESS_CONTEXT $FACT_PROCESS_ID "AGG_DB" "$AGG_DB"
if [ $? -ne 0 ]; then
  _LOG "Error writing process context. Installation FAILED."
  $PLAT_HOME/tools/metadata/delete_process.sh "$FACT_PROCESS_NAME"
  exit 1
fi

_WRITE_PROCESS_CONTEXT $FACT_PROCESS_ID "REPORT_TABLE" "$REPORT_TABLE"
if [ $? -ne 0 ]; then
  _LOG "Error writing process context. Installation FAILED."
  $PLAT_HOME/tools/metadata/delete_process.sh "$FACT_PROCESS_NAME"
  exit 1
fi
_WRITE_PROCESS_CONTEXT $FACT_PROCESS_ID "FACT_TABLE" "$FACT_TABLE"
if [ $? -ne 0 ]; then
  _LOG "Error writing process context. Installation FAILED."
  $PLAT_HOME/tools/metadata/delete_process.sh "$FACT_PROCESS_NAME"
  exit 1
fi
_WRITE_PROCESS_CONTEXT $FACT_PROCESS_ID "FACT_TABLE_UNPARTED" "$FACT_UNPARTED_TABLE"
if [ $? -ne 0 ]; then
  _LOG "Error writing process context. Installation FAILED."
  $PLAT_HOME/tools/metadata/delete_process.sh "$FACT_PROCESS_NAME"
  exit 1
fi
_WRITE_PROCESS_CONTEXT $FACT_PROCESS_ID "JAR_PATH" "$JAR_DEST_PATH"
if [ $? -ne 0 ]; then
  _LOG "Error writing process context. Installation FAILED."
  $PLAT_HOME/tools/metadata/delete_process.sh "$FACT_PROCESS_NAME"
  exit 1
fi
_WRITE_PROCESS_CONTEXT $FACT_PROCESS_ID "FACT_REDUCERS" "$FACT_REDUCERS"
if [ $? -ne 0 ]; then
  _LOG "Error writing process context. Installation FAILED."
  $PLAT_HOME/tools/metadata/delete_process.sh "$FACT_PROCESS_NAME"
  exit 1
fi
_WRITE_PROCESS_CONTEXT $FACT_PROCESS_ID "FACT_LOAD_SPLIT_SIZE" "$FACT_LOAD_SPLIT_SIZE"
if [ $? -ne 0 ]; then
  _LOG "Error writing process context. Installation FAILED."
  $PLAT_HOME/tools/metadata/delete_process.sh "$FACT_PROCESS_NAME"
  exit 1
fi

###########################
# DB2 Load configuration
###########################
_WRITE_PROCESS_CONTEXT $FACT_PROCESS_ID "REP_REQ_SRC_HDFS_PATH" "/user/hive/warehouse/etldata.db/etl_hex_reporting_requirements/00*"
_WRITE_PROCESS_CONTEXT $FACT_PROCESS_ID "REP_REQ_TGT_DB2_TABLE" "STGLDR.HEX_REPORTING_REQUIREMENTS"
_WRITE_PROCESS_CONTEXT $FACT_PROCESS_ID "REP_REQ_INPUT_TYPE" "DIRECT"

_WRITE_PROCESS_CONTEXT $FACT_PROCESS_ID "SEG_SRC_HDFS_PATH" "/data/HWW/$AGG_DB/$SEG_UNPARTED_TABLE/part*"
_WRITE_PROCESS_CONTEXT $FACT_PROCESS_ID "SEG_TGT_DB2_TABLE" "DM.RPT_HEXDM_AGG_SEGMENT"
_WRITE_PROCESS_CONTEXT $FACT_PROCESS_ID "SEG_INPUT_TYPE" "DIRECT"

_WRITE_PROCESS_CONTEXT $FACT_PROCESS_ID "EXP_SRC_HDFS_PATH" "/data/HWW/$AGG_DB/$SEG_EXP_LIST_TABLE/00*"
_WRITE_PROCESS_CONTEXT $FACT_PROCESS_ID "EXP_TGT_DB2_TABLE" "STGLDR.RPT_HEXDM_SEGMENT_EXP_LIST"
_WRITE_PROCESS_CONTEXT $FACT_PROCESS_ID "EXP_INPUT_TYPE" "DIRECT"

_WRITE_PROCESS_CONTEXT $FACT_PROCESS_ID "LOAD_DB2" "Y"
_WRITE_PROCESS_CONTEXT $FACT_PROCESS_ID "TOGGLE_DB2" "Y"

_LOG "Process $FACT_PROCESS_NAME configured successfully"




############################
# script/jar deployment
############################


# recreate the symbolic link to the deployed code 
if [[ -r $MODULE_LN ]]; then 
  sudo -u $ETL_USER rm $MODULE_LN 
fi 
sudo -u $ETL_USER ln -sf $MODULE_DIR/scripts $MODULE_LN 

ln -sf "$JAR_PATH" "$JAR_DEST_PATH"

_LOG "Module hdp_hww_hex_etl deployed successfully"
