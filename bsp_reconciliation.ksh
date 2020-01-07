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
# 30/8/2003    Leigh     Created script
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
report ">>>>>> BSP reconciliation to Stella Report"
report "--------------------------------------------------------------------------"

logfile=$appdir/run_bsp_reconcilication.lst

report "Run the stored proc that runs the report"
sqlplus -s -L ${dbuser}/${dbpass} @$appdir/run_bsp_reconciliation.sql > $logfile

report "display logfile contents:"
cat $logfile

report "Test for errors"
rm $appdir/bsprecon_error.err
echo "Logfile:" $logfile
grep "Error" $logfile >> $appdir/bsprecon_error.err
grep "ERROR" $logfile >> $appdir/bsprecon_error.err
grep "error " $logfile >> $appdir/bsprecon_error.err
grep "SEVERE" $logfile >> $appdir/bsprecon_error.err
grep "WARNING" $logfile >> $appdir/bsprecon_error.err
grep "CRITICAL" $logfile >> $appdir/bsprecon_error.err
grep "PLS-" $logfile >> $appdir/bsprecon_error.err
grep "ORA-" $logfile >> $appdir/bsprecon_error.err
# -s tests that size > 0 bytes
report "Need error email?"
if [ -s $appdir/bsprecon_error.err ]
 then
   echo "error found"
   echo >>  $appdir/bsprecon_error.err
   cat ${mail_list}|while read users
   do
     mailx -s "ERRORS `hostname` Details of BSP to Stella Reconciliation on "$today_d" " ${users}  <  $appdir/bsprecon_error.err
   done


fi

report "email summary"
echo "Logfile:" $logfile
grep "Rows " $logfile > $appdir/bsprecon_summ.lst
# -s tests that size > 0 bytes
report "Need summ email?"
if [ -s $appdir/bsprecon_summ.lst ]
 then
   echo "summ found"
   echo >>  $appdir/bsprecon_summ.lst
   cat ${mail_list}|while read users
   do
     mailx -s "STELLA `hostname` Summary of BSP to Stella Reconciliation on "$today_d" " ${users}  <  $appdir/bsprecon_summ.lst
   done


fi


report "END OF PROGRAM" 
exit 0
