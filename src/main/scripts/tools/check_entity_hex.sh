#!/bin/bash

export HWW_HOME=/usr/etl/HWW
source $HWW_HOME/common/sh_hww_helpers.sh

export ENTITY=$1

_DBCONNECT

STATUS=$($DB2_HOME/sqllib/bin/db2 -x "select count(*) from etl.etl_fileprocess where entity = '$ENTITY' and data = '$END_DATE' with ur")
ERROR_CODE=$?

_DBDISCONNECT

if [[ $ERROR_CODE -eq 0 && $STATUS -eq 1 ]]; then
  echo "[$ENTITY] has completed"
  exit 0
else
  echo "[$ENTITY] has not completed"
  exit 1
fi
