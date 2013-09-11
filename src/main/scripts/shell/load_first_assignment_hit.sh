#!/bin/bash

set -e
mkdir -p $HEX_LOGS
touch $HEX_LOGS/hex.fah.local.date
source $HEX_HOME/conf/hex_etl.cfg

BOOKMARK=$HEX_LOGS/hex.fah.local.date
LAST_DT=`cat ${BOOKMARK}`
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
  echo "$START_DATE ($START_HOUR) - $END_DATE ($END_HOUR)"
  time hive -hiveconf start.date="${START_DATE}" -hiveconf start.hour="${START_HOUR}" -hiveconf end.date="${END_DATE}" -hiveconf end.hour="${END_HOUR}" -hiveconf job.queue="${JOB_QUEUE}" -hiveconf hex.fah.db="${FAH_DB}" -hiveconf hex.fah.table="${FAH_TABLE}" -hiveconf hex.lib="${HEX_LIB}" -hiveconf hex.version="${HEX_VERSION}" -f $HEX_DML/insert_ETL_HEX_ASSIGNMENT_HIT.hql >> $HEX_LOGS/log.txt 2>&1 && echo "$END_DATE $END_HOUR" > $BOOKMARK
fi
