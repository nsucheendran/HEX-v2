#!/bin/bash

    START_DT=`date --date="${CURR_YEAR}-${CURR_MONTH}-01" '+%Y-%m-%d'`
    if [ "${CURR_YEAR}${CURR_MONTH}" \< "${END_YEAR}${END_MONTH}" ]
    then
      END_DT=`date --date="${CURR_YEAR}-${CURR_MONTH}-01 +1 months -1 days" '+%Y-%m-%d'`
    else
      END_DT=`date --date="${LAST_DT}" '+%Y-%m-%d'`
    fi
  
    LOG_FILE_NAME="hdp_transactions_bkg_reprocess_${START_DT}-${END_DT}.log"

    _LOG "Reprocessing Booking Transactions data between [$START_DT to $END_DT] in target: $TRANS_BKG_DB.$TRANS_BKG_TABLE"


