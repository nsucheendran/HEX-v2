#!/bin/bash
####################################################
#
# Hadoop to DB2 named pipe loader V.2
# Performs local load from admin node using pipe fed with compressed data
#
# Perforce keyword expansion:
#   $Author: pkim ( modified by sk) $
#   $Date: 2011/10/14 $
#   $Id: //depot/users/skukunooru/Hadoop/PSG_pipeloader_str.bash#1 $
#   $Revision: #1 $
#
# Usage: EDW_pipeloader_str.bash <DBconfigfile> <HDFS path> <DB2 table> <HDFS URL:port>
#
# Note: Hadoop fs -cat will give exit code of 0 even if source file doesn't exist if
#  wildcards are used.  For example, hadoop fs -cat /mydir/myfile* won't give an error
#  even if there are no files called myfile* in /mydir.  W/O wildcards, it behaves as expected, giving
#  error code for non-existant files.
#
# Note2: passwordless ssh needs to be established between invoking login and another service account
#  on the DB2 admin side.  There on the DB2 side, $HOME/.DB2_connect_etlload needs to be created
#  (and chmod 400 protected) that contains the connection sql for a loader login to DB2.  Because of this,
#  needs the env variables SSHUSERID (the ssh user, not the DB login) and DB2LOADMSGFILEPATH (this is optional) set.
#
# user          date            comment
# ############# ############### ####################
# pkim          2011-07-25      creation
# pkim          2011-09-30      moved to common dir, using ETLCOMMONSCR env var
# pkim          2011-10-12      branched from V1
# pkim          2011-11-02      corrected DB2 exit code, renaming to EWW to keep it from being exposed just yet
# pkim          2012-09-26      added delete step for old msg files prior to load
# skukunooru    2013-11-05      added warning count to the load command
# skukunooru    2013-11-25      added hive null cleanup to the stream
# nsucheendran  2013-12-12      Customize for HWW
#
####################################################
set -o nounset
source $ETLCOMMONSCR/EDW_helpers.bash
SCRIPTPATH=${0%/*}
SCRIPTNAME=${0##*/}

if [ $# -ne 5 ] ; then
  _LOG "Error: incorrect usage."
  _LOG "Usage: $SCRIPTNAME <DBconfigfile> <HDFS path> <DB2 table> <HDFS URL:port>"
  exit 1
fi

_LOG "============== Process start [$$] ============="

DBFILE=$1
HDFSSRC=$2
DB2TGT=$3
HDPNAMENODE=$4
INPUTTYPE=$5
EXITCODEFILE="/tmp/$SCRIPTNAME.$DB2TGT.tmp"
echo "0" > $EXITCODEFILE

FIFO="/tmp/$SCRIPTNAME.$DB2TGT.fifo"
PIPE_PID_FILE="/tmp/$SCRIPTNAME.$DB2TGT.PIPE_PID"
LOAD_PID_FILE="/tmp/$SCRIPTNAME.$DB2TGT.LOAD_PID"

# get SSHUSERID, DBNAME, DBHOST, maybe even $DB2LOADMSGFILEPATH
if [ ! -e $DBFILE ] ; then
  _LOG "Error: DBconfigfile [$DBFILE] does not exist."
  exit 1
fi
source $DBFILE
MSGFILE="${DB2LOADMSGFILEPATH:-/tmp}/$SCRIPTNAME.$DB2TGT.msg"

PIPE_THREAD () {
  
  if [ $INPUTTYPE = "LIST" ]; then
    hadoop fs -fs $HDPNAMENODE -cat `cat $HDFSSRC` | sed 's/\\N//g' | gzip | ssh $SSHUSERID@$DBHOST "
    set -o errexit
    echo \$\$ > $PIPE_PID_FILE
    echo \"Pipe subshell at $DBHOST: [\$\$]\"
    rm -f $FIFO >/dev/null 2>&1
    mkfifo $FIFO
    cat - | gunzip > $FIFO
  "
else 
    hadoop fs -fs $HDPNAMENODE -cat $HDFSSRC | sed 's/\\N//g' | gzip | ssh $SSHUSERID@$DBHOST "
    set -o errexit
    echo \$\$ > $PIPE_PID_FILE
    echo \"Pipe subshell at $DBHOST: [\$\$]\"
    rm -f $FIFO >/dev/null 2>&1
    mkfifo $FIFO
    cat - | gunzip > $FIFO
  "
fi;
  PIPE_RC="${PIPESTATUS[*]}"
  _LOG "PIPE thread exit code: [$PIPE_RC]"
  if [ "$PIPE_RC" != "0 0 0 0" ] ; then
    # Kill LOAD thread on remote server since this thread wasn't successful
    ssh $SSHUSERID@$DBHOST "test -e $LOAD_PID_FILE && cat $LOAD_PID_FILE | xargs pkill -P"
    echo "2" > $EXITCODEFILE
  fi
  ssh $SSHUSERID@$DBHOST "rm -f $PIPE_PID_FILE $FIFO"
}

LOAD_THREAD () {
  ssh $SSHUSERID@$DBHOST "
    set -o errexit
    echo \$\$ > $LOAD_PID_FILE
    echo \"DB2 load subshell PID at $DBHOST: [\$\$]\"
    db2 -tvf ~/.DB2_connect_etlload
    set +o errexit
    rm -f ${MSGFILE}*
    db2 -v 'load from $FIFO of del modified by coldel0x09 nochardel timestampformat=\"yyyy-mm-dd hh:mm\" codepage=1208 warningcount 50 messages $MSGFILE 
      replace into $DB2TGT statistics use profile nonrecoverable'
    DB2_RC=\$?
    db2 terminate
    chmod 666 ${MSGFILE}* >/dev/null 2>&1 || echo 'Warning: cannot change permission of [${MSGFILE}*].'
    echo \"Exiting with DB2_RC: [\$DB2_RC]\"
    if [ \$DB2_RC -gt 2 ] ; then
      exit \$DB2_RC
    else
      echo \"DB2_RC less than 3 are warnings.  Successful exit.\"
      exit 0
    fi
  "
  LOAD_RC=$?
  _LOG "LOAD thread exit code: [$LOAD_RC]"
  if [ $LOAD_RC -ne 0 ] ; then
    # Kill PIPE thread on remote server since this thread wasn't successful
    ssh $SSHUSERID@$DBHOST "test -e $PIPE_PID_FILE && cat $PIPE_PID_FILE | xargs pkill -P"
    echo "2" > $EXITCODEFILE
  fi
  ssh $SSHUSERID@$DBHOST "rm -f $LOAD_PID_FILE"
}

PIPE_THREAD &
LOAD_THREAD &

# set trap to remote cleanup in case of interruption
# killing one thread should trigger killing the other too
trap "_LOG 'SIGINT/SIGTERM/SIGKILL detected: issuing remote kills.'
  ssh $SSHUSERID@$DBHOST '
    echo \"Killing PIPE thread: \$(cat $PIPE_PID_FILE)\"
    cat $PIPE_PID_FILE | xargs pkill -P'
  _LOG 'Waiting for cleanup to finish...'
  wait
  _LOG 'Cleanup complete, exiting with RC=[255].'
  exit 255
" SIGINT SIGTERM SIGKILL

wait
_LOG "DB2 LOAD logs at: [$DBHOST:$MSGFILE]"
EXIT_CODE=$(cat $EXITCODEFILE)

if [ $EXIT_CODE -eq 0 ] ; then
        ssh $SSHUSERID@$DBHOST "rm -f $MSGFILE*"
fi

rm -f $EXITCODEFILE
_LOG "============== Process completed, exiting with code [$EXIT_CODE] ============="
exit $EXIT_CODE
