#!/bin/bash

########################################################
#
# Description: Invokes the stored procedures passed as parameters
#
# Usage:
# Exit code:
#   0 on success
#   1 on failure
#
# Revision History
#
# Date          Author          Description
# ############# ############### ################
# 2013-12-17	nsucheendran	 Creation
########################################################
set -o errexit
set -o pipefail
set -o nounset
umask 0002

# ENV parameters
PLAT_HOME=/usr/local/edw/platform
DB2LOGIN="$HOME/dbconf"

# Runtime / meta parameters
SCRIPTPATH=${0%/*}
SCRIPTNAME=${0##*/}
SP=$1

#Initialize
source $PLAT_HOME/common/sh_helpers.sh
source $PLAT_HOME/common/sh_metadata_storage.sh
source $DB2LOGIN
export DB_NAME=$DBNAME
export DB_USER=$USERID
export DB_PASS=$PASSWD



#Connect to DB2 
_DBCONNECT $DB2LOGIN
db2 -x "call $SP()"
if [ $? -eq 8 ] ; then
  _LOG "Error"
  exit 1
fi

#Disconnect from DB2 
_DBDISCONNECT
