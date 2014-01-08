#!/bin/bash

########################################################
#
# hdp_hex_db2_refresh.sh
#
# Description:
#	Wrapper script to load data sets from Hadoop to DB2 to satisfy HEX Reporting requirements. 
#
# Usage:
#   hdp_hex_db2_refresh.sh <PROCESS_NAME>
# Exit code:
#   0 on success
#   1 on failure
#
# Revision History
#
# Date          Author          Description
# ############# ############### ################
# 2013-12-09    Creation       Jira No - 94607
#
########################################################
set -o errexit
set -o pipefail
set -o nounset
umask 0002

# ENV parameters
PLAT_HOME=/usr/local/edw/platform
LOADERPATH=/usr/etl/HWW/common
DB2LOGIN="$HOME/dbconf"
HDPENV="$HOME/hdpenv.conf"
LOADERSCRIPT=$LOADERPATH/HWW_pipeloader_str.bash

# Runtime / meta parameters
SCRIPTPATH=${0%/*}
SCRIPTNAME=${0##*/}

#Read Process Name
PROCESS_NAME=$1

#Initialize
source $PLAT_HOME/common/sh_helpers.sh
source $PLAT_HOME/common/sh_metadata_storage.sh
source $HDPENV
source $DB2LOGIN
export DB_NAME=$DBNAME
export DB_USER=$USERID
export DB_PASS=$PASSWD


#HEMS:Acquire Lock and Read Parameters
HWW_LOCK_NAME="hdp_hex_DB2_refresh$PROCESS_NAME.lock"
MESSAGE="hdp_hex_db2_refresh.sh failed: Previous script still running"
_ACQUIRE_LOCK $HWW_LOCK_NAME "$MESSAGE" 30

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
  _LOG_PROCESS_DETAIL $RUN_ID "$PROCESS_NAME:STATUS" "STARTED"
fi

HDFSETLPATH=`_READ_PROCESS_CONTEXT $PROCESS_ID "SRC_HDFS_PATH"`
TGTTBL=`_READ_PROCESS_CONTEXT $PROCESS_ID "TGT_DB2_TABLE"`
INPUTTYPE=`_READ_PROCESS_CONTEXT $PROCESS_ID "INPUT_TYPE"`

trap "_LOG 'SIGINT | SIGTERM | SIGKILL detected.  Aborting, but MapRed job may need cleanup.' ; exit 255" \
  SIGINT SIGTERM SIGKILL

#Connect to DB2 and create the table
_LOG "Create the table in DB2"
_DBCONNECT $DB2LOGIN
db2 -tvf $SCRIPTPATH/../sql/$TGTTBL.sql
if [ $? -ge 4 ] ; then
  _LOG "Error: SQL merge step failure, rolling back."
  exit 1
fi

#Disconnect from DB2 and log the count in HEMS.
_DBDISCONNECT



#Start the job and logging
_LOG "============ Starting $SCRIPTNAME $TGTTBL==============="
_LOG "PWD: [$PWD]"
_LOG "DB2LOGIN: [$DB2LOGIN]"
_LOG "HDPENV: [$HDPENV]"
_LOG "STRMJAR: [$STRMJAR]"
_LOG "HDPNAMENODE: [$HDPNAMENODE]"
_LOG "LOADERPATH: [$LOADERPATH]"
_LOG "LOADERSCRIPT: [$LOADERSCRIPT]"
_LOG "SCRIPTPATH: [$SCRIPTPATH]"
_LOG "SCRIPTNAME: [$SCRIPTNAME]"
_LOG "TGTTBL: [$TGTTBL]"
_LOG "HDFSETLPATH: [$HDFSETLPATH]"


#Check the count of records from HDFS
if [ $INPUTTYPE = "LIST" ]; then
HDPFILEROWCOUNT=$(hadoop fs -cat `cat $HDFSETLPATH` | wc -l)
else
HDPFILEROWCOUNT=$(hadoop fs -cat $HDFSETLPATH | wc -l)
fi;

if [ $HDPFILEROWCOUNT -eq 0 ]; then
  echo "Warning: ETL result is empty, no work to do; exiting."
  _LOG_PROCESS_DETAIL $RUN_ID "$PROCESS_NAME:STATUS" "NO DATA"
   exit 0
else
  echo "Data found in source; continuing process."
fi;

_LOG_PROCESS_DETAIL $RUN_ID "$PROCESS_NAME:HDP Count" "$HDPFILEROWCOUNT"
_LOG " Total Records in the Source File :  $HDPFILEROWCOUNT"

#Invoke Pipeloader
_LOG "DB2 integration : $LOADERSCRIPT $DB2LOGIN $HDFSETLPATH $TGTTBL $HDPNAMENODE $INPUTTYPE"
$LOADERSCRIPT $DB2LOGIN $HDFSETLPATH $TGTTBL $HDPNAMENODE $INPUTTYPE


#Connect to DB2 and check Count of records Loaded
_LOG "Update the count from DB2 to HEMS"
_DBCONNECT $DB2LOGIN
set +o errexit
DCOUNT=`db2 -x "select count(*) from $TGTTBL"`
if [ $? -ge 4 ] ; then
  _LOG "Error: SQL merge step failure, rolling back."
  exit 1
fi

#Disconnect from DB2 and log the count in HEMS.
set -o errexit
_DBDISCONNECT

_LOG_PROCESS_DETAIL $RUN_ID "$PROCESS_NAME:DB2 Count" "$DCOUNT"

_LOG "Create partitions for DM.RPT_HEXDM_AGG_SEGMENT_COMP"

#Connect to DB2 and invoke the stored procedure to create partitions for DM.RPT_HEXDM_AGG_SEGMENT_COMP
_DBCONNECT $DB2LOGIN
db2 -x "call ETL.SP_HEX_COMPLETED_CREATE_PARTITION()"
if [ $? -eq 8 ] ; then
  _LOG "Error:Check etl.etl_sproc_error for more information"
  exit 1
fi

_LOG "Load data into Live and Completed tables"

#Invoke the procedure to insert data into Live and Completed Tables
db2 -x "call ETL.SP_RPT_HEXDM_AGG_SEGMENT_LOAD()"
if [ $? -eq 8 ] ; then
  _LOG "Error:Check etl.etl_sproc_error for more information"
  exit 1
fi


#Disconnect from DB2 
_DBDISCONNECT

_LOG_PROCESS_DETAIL $RUN_ID "$PROCESS_NAME:STATUS" "COMPLETED"

_LOG "============ $SCRIPTNAME $TGTTBL completed ==============="

_FREE_LOCK $HWW_LOCK_NAME
