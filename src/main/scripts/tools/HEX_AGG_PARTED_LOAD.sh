set -m
export PLAT_HOME=/usr/local/edw/platform
export HWW_HOME=/usr/etl/HWW

source $PLAT_HOME/common/sh_helpers.sh
source $PLAT_HOME/common/sh_metadata_storage.sh

SCRIPT_PATH_AGG=$HWW_HOME/hdp_hww_hex_etl/hql/AGG

JOB_QUEUE=$1;
FACT_LOAD_SPLIT_SIZE=$2;
AGG_DB=$3;
AGG_TABLE=$4;
FACT_AGG_UNPARTED_TABLE=$5;
AGG_NUM_REDUCERS=$6;
HEX_LOGS=$7;
LOG_FILE_NAME=$8;
PROCESS_ID=$9;
  
hive -hiveconf job.queue="${JOB_QUEUE}" -hiveconf split.size="${FACT_LOAD_SPLIT_SIZE}" -hiveconf hex.db="${AGG_DB}" -hiveconf hex.agg.table="${AGG_TABLE}" -hiveconf hex.agg.unparted.table="${FACT_AGG_UNPARTED_TABLE}" -hiveconf agg.num.reduce.tasks="${AGG_NUM_REDUCERS}" -f $SCRIPT_PATH_AGG/insert_ETL_HCOM_HEX_AGG.hql >> $HEX_LOGS/$LOG_FILE_NAME 2>&1
ERROR_CODE=$?
if [[ $ERROR_CODE -ne 0 ]]; then
  _LOG "HEX_SEG: Partitioned Agg table load FAILED [ERROR_CODE=$ERROR_CODE]. See [$HEX_LOGS/$LOG_FILE_NAME] for more information." $HEX_LOGS/LNX-HCOM_HEX_FACT.log
  _WRITE_PROCESS_CONTEXT "$PROCESS_ID" "PARTED_AGG_LOAD" "false"
  exit 1
fi
_WRITE_PROCESS_CONTEXT "$PROCESS_ID" "PARTED_AGG_LOAD" "true"
_LOG "Agg Partition Load Completed successfully" $HEX_LOGS/LNX-HCOM_HEX_FACT.log