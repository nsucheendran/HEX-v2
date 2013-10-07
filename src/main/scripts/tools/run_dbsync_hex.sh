#!/bin/bash

#Platform variables
export ETL_USER=platetl
export PLAT_HOME=/usr/local/edw/platform

#DBSYNC variables
export DS_NAME=EDW_DB2
export FILE_LAYOUT="SEQUENCE"
export COMPRESSION_TYPE="snappy"
export HIVE_TABLE="$HIVE_SCHEMA.$HIVE_TABLE_NAME"
export META_DATA_DIR="/etl/common/$HIVE_SCHEMA/meta-inf"
export IMPORT_TARGET_DIR="/raw/common/$HIVE_SCHEMA/$HIVE_TABLE_NAME"
export HIVE_TABLE_DIR="/data/common/$HIVE_SCHEMA/$HIVE_TABLE_NAME"
export TEMP_DIR="/tmp/common/$HIVE_SCHEMA/$HIVE_TABLE_NAME"
export DB2_TABLE=$DB2_TABLE_NAME

cd /tmp
source $PLAT_HOME/tools/sh_dbsync/dbsync_helpers.sh
_SYNCHRONIZE_TABLE || exit 1

# validation doesn't work as expected (always validates only the latest partition)
# $PLAT_HOME/tools/data_validation/db2hive_counts_validation.sh || exit 1
