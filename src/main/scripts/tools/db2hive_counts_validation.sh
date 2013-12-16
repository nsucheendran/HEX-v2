#!/bin/bash

export PLAT_HOME=/usr/local/edw/platform
source $PLAT_HOME/common/sh_helpers.sh

SCRIPTS_FOLDER=$PLAT_HOME/tools/data_validation

source $SCRIPTS_FOLDER/data_validation_helper.sh
source $PLAT_HOME/common/sh_metadata_storage.sh

BOOKMARK_PATTERN='+%Y-%m-%d'
PROCESS_NAME="ETL_HCOM_HEX_TRANSACTIONS_BKG"

ERROR_CODE=0

PROCESS_ID=$(_GET_PROCESS_ID "$PROCESS_NAME");
RETURN_CODE="$?"

if [ "$PROCESS_ID" == "" ] || (( $RETURN_CODE != 0 )); then
  _LOG "ERROR: Process [$PROCESS_NAME] does not exist in HEMS"
  ERROR_CODE=1
  exit 1;
fi

BOOKMARK=`_READ_PROCESS_CONTEXT $PROCESS_ID "BOOKMARK"`
PROCESSING_TYPE=`_READ_PROCESS_CONTEXT $PROCESS_ID "PROCESSING_TYPE"`

_LOG "PROCESSING_TYPE=$PROCESSING_TYPE"
_LOG "BOOKMARK=$BOOKMARK"

if [ $PROCESSING_TYPE = "R" ]; then
  BOOKMARK=$BOOKMARK
else
  BOOKMARK=`date --date="${BOOKMARK} +1 days" '+%Y-%m-%d'`
fi

ERROR_CODE=0 

_GET_DB2_CREDENTIALS $DS_NAME
ERROR_CODE=$((10#$ERROR_CODE+$?))

DB2_LOG_FILE=/tmp/count_validation_db2_log.$DB2_TABLE; 

if (( $ERROR_CODE == 0 )); then
    _LOG "Validation of synchronization of $HIVE_TABLE starts ..."
    if !  _DBCONNECT > $DB2_LOG_FILE; then
        _LOG "Unable to connect to database";
        cat $DB2_LOG_FILE
        exit 1
    fi
fi

if (( $ERROR_CODE == 0 )); then
    TEST_CASE_DB2=" select count(*) from $DB2_TABLE " ;
    TEST_CASE_HIVE=" select count(*) from $HIVE_TABLE "; 

    if [[ $HIVE_PARTITION_KEY != "" ]]; then
        echo "get partition and bookmark"
        HIVE_PARTITION=`date --date="${BOOKMARK}" "${HIVE_PARTITION_PATTERN}"`
        DB2_UNIT=`date --date="${BOOKMARK}" "${DB2_UNIT_PATTERN}"`
        ERROR_CODE=$?
        TEST_CASE_DB2=$(_ADD_WHERE_CLAUSE "$TEST_CASE_DB2" " date($PARTITION_BY_FIELD)=date('$DB2_UNIT')");
        TEST_CASE_HIVE=$(_ADD_WHERE_CLAUSE "$TEST_CASE_HIVE" " $HIVE_PARTITION_KEY='$HIVE_PARTITION' and $PROCESS_BOOKMARK_FIELD='$DB2_UNIT'"); 
    fi
    
    if [[ $WHERE_CLAUSE != "" ]]; then
        TEST_CASE_DB2=$(_ADD_WHERE_CLAUSE "$TEST_CASE_DB2" "$WHERE_CLAUSE");
    fi

    TEST_CASE_DB2="$TEST_CASE_DB2 with UR"
    
    _LOG "DB2 test_case: $TEST_CASE_DB2";
    _LOG "Hive test_case: $TEST_CASE_HIVE";

    DB2_RES_FILE=/tmp/count_validation_db2.$DB2_TABLE; 
    HIVE_RES_FILE=/tmp/count_validation_hive.$HIVE_TABLE; 
fi

if (( $ERROR_CODE == 0 )); then
    _HIVE_AGAINST_DB2;
    ERROR_CODE=$((10#$ERROR_CODE+$?))
fi

if (( $ERROR_CODE == 0 )); then
    _COMPARE_NUMBERS;
    COMP_RES=$(_VALIDATE_PAIR "$EXPECTED_RES" "$HIVE_RES");

    case $COMP_RES in
        "PASSED")
            _LOG "PASSED $MESSAGE";
        ;;
        "FAILED") 
            _LOG "FAILED $MESSAGE";
            ERROR_CODE=1
        ;;
        *)
            _LOG "error occured" 
            ERROR_CODE=1
        ;;
    esac
fi

_DBDISCONNECT > $DB2_LOG_FILE; 

    rm $DB2_LOG_FILE
    rm $DB2_RES_FILE
    rm $HIVE_RES_FILE

_LOG "Validation of synchronization of $HIVE_TABLE finished"

exit $ERROR_CODE
