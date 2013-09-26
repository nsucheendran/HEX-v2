#!/bin/bash

_LOG "R3: Booking Transactions load FAILED [ERROR_CODE=$ERROR_CODE]. [see $HEX_LOGS/$LOG_FILE_NAME] for more information."
_END_PROCESS $RUN_ID $ERROR_CODE
_FREE_LOCK $HWW_TRANS_BKG_LOCK_NAME
