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
MODULE_LN=$HWW_HOME/$MODULE_NAME/scripts
export MODULE_PATH=$MODULE_LN

CURR_PATH=`dirname $0`

SCRIPT_PATH=$CURR_PATH/../scripts/hql/R1
JAR_PATH=$(ls $CURR_PATH/../jars/${MODULE_NAME}*.jar)
JAR_DEST_PATH=/app/edw/hive/auxlib/$MODULE_NAME.jar

ETL_USER=hwwetl
FAH_TABLE='ETL_HCOM_HEX_ASSIGNMENT_HIT'
FAH_DB='etldata'
JOB_QUEUE='hwwetl'
REPROCESS_START_YEAR='2012'
REPROCESS_START_MONTH='11'

FAH_PROCESS_NAME="ETL_HCOM_HEX_FIRST_ASSIGNMENT_HIT"
_LOG "Configuring process $FAH_PROCESS_NAME ..."

FAH_PROCESS_DESCRIPTION="Loads HEX First Assignment Hit Data"

FAH_PROCESS_ID=$(_GET_PROCESS_ID "$FAH_PROCESS_NAME")
if [ -z "$FAH_PROCESS_ID" ]; then
    hive -hiveconf job.queue="${JOB_QUEUE}" -hiveconf hex.fah.db="${FAH_DB}" -hiveconf hex.fah.table="${FAH_TABLE}" -f $SCRIPT_PATH/createTable_ETL_HCOM_HEX_ASSIGNMENT_HIT.hql
    $PLAT_HOME/tools/metadata/add_process.sh "$FAH_PROCESS_NAME" "$FAH_PROCESS_DESCRIPTION"
    FAH_PROCESS_ID=$(_GET_PROCESS_ID "$FAH_PROCESS_NAME")
else
    _LOG "Process $FAH_PROCESS_NAME already exists"
fi
`_WRITE_PROCESS_CONTEXT $FAH_PROCESS_ID "FAH_TABLE" "$FAH_TABLE"`
`_WRITE_PROCESS_CONTEXT $FAH_PROCESS_ID "FAH_DB" "$FAH_DB"`
`_WRITE_PROCESS_CONTEXT $FAH_PROCESS_ID "JOB_QUEUE" "$JOB_QUEUE"`
`_WRITE_PROCESS_CONTEXT $FAH_PROCESS_ID "BOOKMARK" ""`
`_WRITE_PROCESS_CONTEXT $FAH_PROCESS_ID "PROCESSING_TYPE" "R"`
`_WRITE_PROCESS_CONTEXT $FAH_PROCESS_ID "REPROCESS_START_YEAR" "$REPROCESS_START_YEAR"`
`_WRITE_PROCESS_CONTEXT $FAH_PROCESS_ID "REPROCESS_START_MONTH" "$REPROCESS_START_MONTH"`

# recreate the symbolic link to the deployed code 
if [[ -r $MODULE_LN ]]; then 
  sudo -u $ETL_USER rm $MODULE_LN 
fi 
sudo -u $ETL_USER ln -sf $MODULE_DIR $MODULE_LN 

ln -sf "$JAR_PATH" "$JAR_DEST_PATH"

_LOG "Process $FAH_PROCESS_NAME configured successfully"

