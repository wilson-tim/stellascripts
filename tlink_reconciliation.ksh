#!/bin/ksh
#set -x

#############################################################################################
#
#
#
#--------------------------------------------------------------------------------------------
# Change
# ------
#
# Date          Who     Description
# ----          ---     -----------
# 19/04/2006    Jyoti    Created script
#
#############################################################################################
. /Oracle/SECTOR/bin/set_ora_env.ksh
#
#
PATH=$PATH:$ORACLE_HOME/bin:/opt/bin:/usr/ccs/bin
export PATH
LD_LIBRARY_PATH=$ORACLE_HOME/lib:/usr/java/lib ; export LD_LIBRARY_PATH
#
# set up directory variables
#
. /Oracle/SECTOR/scripts/stella/set_paths.ksh
# 
function report
{
   echo `date +%Y/%m/%d---%H:%M:%S` $*
}

today_d=`date +%Y%b%d`
today_d=`echo $today_d | sed /.*/y/ADFJMNOS/adfjmnos/`
mail_list="${appdir}/appsupportmail.lst"
user_mail_list="${appdir}/usermail.lst"

report "--------------------------------------------------------------------------"
report ">>>>>> Travel Link reconciliation to Stella Report"
report "--------------------------------------------------------------------------"

logfile=$appdir/run_tlink_reconcilication.lst

report "Run the stored proc that runs the report"
sqlplus -L ${dbuser}\${dbpass}  @$appdir/run_tlink_reconciliation.sql > $logfile

report "display logfile contents:"
cat $logfile

report "Test for errors"
echo "Logfile:" $logfile
rm $appdir/tlinkrecon_error.err
grep "Fail" $logfile > $appdir/tlinkrecon_error.err
grep "Error" $logfile >> $appdir/tlinkrecon_error.err
grep "ERROR" $logfile >> $appdir/tlinkrecon_error.err
grep "error " $logfile >> $appdir/tlinkrecon_error.err
grep "SEVERE" $logfile >> $appdir/tlinkrecon_error.err
grep "WARNING" $logfile >> $appdir/tlinkrecon_error.err
grep "CRITICAL" $logfile >> $appdir/tlinkrecon_error.err
grep "PLS-" $logfile >> $appdir/tlinkrecon_error.err
grep "ORA-" $logfile >> $appdir/tlinkrecon_error.err
# -s tests that size > 0 bytes
report "Need error email?"
if [ -s $appdir/tlinkrecon_error.err ]
 then
   echo "error found"
   echo >>  $appdir/tlinkrecon_error.err
   cat ${mail_list}|while read users
   do
     mailx -s "ERRORS `hostname` Details of Travel Link  to Stella Reconciliation on "$today_d" " ${users}  <  $appdir/tlinkrecon_error.err
   done


fi

report "email summary"
echo "Logfile:" $logfile
grep "Rows " $logfile > $appdir/tlinkrecon_summ.lst
# -s tests that size > 0 bytes
report "Need summ email?"
if [ -s $appdir/tlinkrecon_summ.lst ]
 then
   echo "summ found"
   echo >>  $appdir/tlinkrecon_summ.lst
   cat ${user_mail_lst} ${mail_list}|while read users
   do
     mailx -s "STELLA `hostname` Summary of Travel Link to Stella Reconciliation on "$today_d" " ${users}  <  $appdir/tlinkrecon_summ.lst
   done


fi


report "END OF PROGRAM"
exit 0
