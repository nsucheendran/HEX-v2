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
JAR_PATH=$(ls $CURR_PATH/../jars/${MODULE_NAME}*.jar)
JAR_DEST_PATH=/app/edw/hive/auxlib/$MODULE_NAME.jar

ETL_USER=hwwetl
PLAT_USER=platetl
FAH_TABLE='ETL_HCOM_HEX_FIRST_ASSIGNMENT_HIT'
TRANS_TABLE='ETL_HCOM_HEX_TRANSACTIONS'

ACTIVE_FAH_TABLE='ETL_HCOM_HEX_ACTIVE_FIRST_ASSIGNMENT_HIT'
FACT_STAGE_TABLE='ETL_HCOM_HEX_FACT_STAGING'
FACT_TABLE='ETL_HCOM_HEX_FACT'
REPORT_TABLE='etl_hex_reporting_requirements'
REPORT_FILE='/autofs/edwfileserver/sherlock_in/HEX/HEXV2UAT/HEX_REPORTING_INPUT.csv'
EMAIL_TO='agurumurthi@expedia.com,nsucheendran@expedia.com'
EMAIL_CC='achadha@expedia.com,nsood@expedia.com'

HEX_DB='DM'

FAH_DB='ETLDATA'
JOB_QUEUE='hwwetl'
REPROCESS_START_YEAR='2012'
REPROCESS_START_MONTH='11'

FACT_REDUCERS='100'


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


FACT_PROCESS_ID=$(_GET_PROCESS_ID "$FACT_PROCESS_NAME")
if [ -z "$FACT_PROCESS_ID" ]; then

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
  _WRITE_PROCESS_CONTEXT $FACT_PROCESS_ID "REPORT_TABLE" "$REPORT_TABLE"
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

_WRITE_PROCESS_CONTEXT $FACT_PROCESS_ID "FACT_TABLE" "$FACT_TABLE"
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
