set -m
export PLAT_HOME=/usr/local/edw/platform
export HWW_HOME=/usr/etl/HWW

source $PLAT_HOME/common/sh_helpers.sh
source $PLAT_HOME/common/sh_metadata_storage.sh

SCRIPT_PATH=$HWW_HOME/hdp_hww_hex_etl/hql/FACT

JOB_QUEUE=$1;
FACT_LOAD_SPLIT_SIZE=$2;
STAGE_DB=$3;
FACT_TABLE=$4;
HEX_LOGS=$5;
LOG_FILE_NAME=$6;
PROCESS_ID=$7;
  
hive -hiveconf job.queue="${JOB_QUEUE}" -hiveconf split.size="${FACT_LOAD_SPLIT_SIZE}" -hiveconf hex.db="${STAGE_DB}" -hiveconf hex.table="${FACT_TABLE}" -f $SCRIPT_PATH/insertTable_ETL_HCOM_HEX_FACT.hql >> $HEX_LOGS/$LOG_FILE_NAME 2>&1
ERROR_CODE=$?
if [[ $ERROR_CODE -ne 0 ]]; then
  _LOG "HEX_SEG: Partitioned Fact table load FAILED [ERROR_CODE=$ERROR_CODE]. See [$HEX_LOGS/$LOG_FILE_NAME] for more information." $HEX_LOGS/LNX-HCOM_HEX_FACT.log
  _WRITE_PROCESS_CONTEXT "$PROCESS_ID" "PARTED_FACT_LOAD_FAILED" "true"
  exit 1
fi
_WRITE_PROCESS_CONTEXT "$PROCESS_ID" "PARTED_FACT_LOAD_FAILED" "false"
_LOG "Fact Partition Load Completed successfully" $HEX_LOGS/LNX-HCOM_HEX_FACT.log