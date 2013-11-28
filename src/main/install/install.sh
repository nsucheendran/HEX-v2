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
JAR_PATH=$(ls $CURR_PATH/../jars/${MODULE_NAME}*.jar)
JAR_DEST_PATH=/app/edw/hive/auxlib/$MODULE_NAME.jar

ETL_USER=hwwetl
PLAT_USER=platetl
FAH_TABLE='ETL_HCOM_HEX_FIRST_ASSIGNMENT_HIT'
TRANS_TABLE='ETL_HCOM_HEX_TRANSACTIONS'
FAH_DB='ETLDATA'
JOB_QUEUE='hwwetl'
REPROCESS_START_YEAR='2012'
REPROCESS_START_MONTH='11'

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

FAH_PROCESS_NAME="ETL_HCOM_HEX_FIRST_ASSIGNMENT_HIT_TRANS"
_LOG "Configuring process $FAH_PROCESS_NAME ..."

FAH_PROCESS_DESCRIPTION="Loads HEX First Assignment Hit Data"

FAH_PROCESS_ID=$(_GET_PROCESS_ID "$FAH_PROCESS_NAME")
if [ -z "$FAH_PROCESS_ID" ]; then
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
else
  _LOG "Process $FAH_PROCESS_NAME already exists"
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

_LOG "Process $FAH_PROCESS_NAME configured successfully"

##########################
# BKG_TRANS Deployment
##########################

TRANS_PROCESS_NAME="ETL_HCOM_HEX_TRANSACTIONS_BKG"
_LOG "Configuring process $TRANS_PROCESS_NAME ..."

TRANS_PROCESS_DESCRIPTION="Loads HEX Booking Transactions Data"

TRANS_PROCESS_ID=$(_GET_PROCESS_ID "$TRANS_PROCESS_NAME")
if [ -z "$TRANS_PROCESS_ID" ]; then
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
else
  _LOG "Process $TRANS_PROCESS_NAME already exists"
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

_LOG "Process $TRANS_PROCESS_NAME configured successfully"

# recreate the symbolic link to the deployed code 
if [[ -r $MODULE_LN ]]; then 
  sudo -u $ETL_USER rm $MODULE_LN 
fi 
sudo -u $ETL_USER ln -sf $MODULE_DIR/scripts $MODULE_LN 

ln -sf "$JAR_PATH" "$JAR_DEST_PATH"

_LOG "Module hdp_hww_hex_etl deployed successfully"
