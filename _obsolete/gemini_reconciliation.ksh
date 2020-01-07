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
# 30/4/2003    Leigh     Created script
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

report "--------------------------------------------------------------------------"
report ">>>>>> DWHSE Gemini reconciliation to Stella Report"
report "--------------------------------------------------------------------------"

logfile=$appdir/run_gemini_reconcilication.lst

report "Run the stored proc that runs the report"
sqlplus  ${dbuser}/${dbpass} @$appdir/run_gemini_reconciliation.sql > $logfile

report "display logfile contents:"
cat $logfile

report "Test for errors"
echo "Logfile:" $logfile
grep "Fail" $logfile > $appdir/gemrecon_error.err 
grep "Error" $logfile >> $appdir/gemrecon_error.err
grep "ERROR" $logfile >> $appdir/gemrecon_error.err
grep "error " $logfile >> $appdir/gemrecon_error.err
grep "SEVERE" $logfile >> $appdir/gemrecon_error.err
grep "WARNING" $logfile >> $appdir/gemrecon_error.err
grep "CRITICAL" $logfile >> $appdir/gemrecon_error.err
# -s tests that size > 0 bytes
report "Need error email?"
if [ -s $appdir/gemrecon_error.err ]
 then
   echo "error found"
   echo >>  $appdir/gemrecon_error.err
   cat ${mail_list}|while read users
   do
     mailx -s "ERRORS `hostname` Details of dwhse/Gemini to Stella Reconciliation on "$today_d" " ${users}  <  $appdir/gemrecon_error.err
   done


fi

report "email summary"
echo "Logfile:" $logfile
grep "Rows " $logfile > $appdir/gemrecon_summ.lst
# -s tests that size > 0 bytes
report "Need summ email?"
if [ -s $appdir/gemrecon_summ.lst ]
 then
   echo "summ found"
   echo >>  $appdir/gemrecon_summ.lst
   cat ${mail_list}|while read users
   do
     mailx -s "STELLA `hostname` Summary of dwhse/Gemini to Stella Reconciliation on "$today_d" " ${users}  <  $appdir/gemrecon_summ.lst
   done


fi


report "END OF PROGRAM" 
exit 0
