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
# 14/02/2018   SMartin   Migrate to P-GS2-ORA-01
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
user_mail_list="${appdir}/usermail.lst"
error_mail_list="${appdir}/appsupportmail.lst"

report "--------------------------------------------------------------------------"
report ">>>>>> DWHSE bookings missing from Stella Report"
report "--------------------------------------------------------------------------"

# Setting variables
report "Setting variables"

logfile=$appdir/run_bkg_missing_stella_check.lst


report "Run the stored proc that runs the report"
sqlplus  -s ${dbuser}/${dbpass} @$appdir/run_bkg_missing_stella_check.sql > $logfile

report "display logfile contents:"
cat $logfile


report "Test for errors"
echo "Logfile:" $logfile
grep "Fail" $logfile > $appdir/miss_error.err
grep "Error " $logfile >> $appdir/miss_error.err
grep "ERROR" $logfile >> $appdir/miss_error.err
grep "error " $logfile >> $appdir/miss_error.err
grep "SEVERE" $logfile >> $appdir/miss_error.err
grep "WARNING" $logfile >> $appdir/miss_error.err
grep "CRITICAL" $logfile >> $appdir/miss_error.err
# -s tests that size > 0 bytes
report "Need error email?"
if [ -s $appdir/miss_error.err ]
 then
   echo "error found"
   echo >>  $appdir/miss_error.err
   cat ${error_mail_list}|while read users
   do
     echo ${users}
     mailx -s "Details of `hostname` STELLA missing bookings errors encountered  on "$today_d" " ${users} <  $appdir/miss_error.err
   done


fi



report "output summary"
logfile=${logfile}/stlbkgms.log
echo "Logfile:" $logfile
cat $logfile > $appdir/miss_error.err
# -s tests that size > 0 bytes
report "Need summ email?"
if [ -s $appdir/miss_error.err ]
 then
   echo "summ found"
   echo >>  $appdir/miss_error.err
   cat ${user_mail_list}|while read users
   do
     echo ${users}
     mailx -s "Summary of `hostname` Gemini bookings about to depart and missing from Stella  on "$today_d" " ${users} <  $appdir/miss_error.err
   done
fi


report "END OF PROGRAM"
exit 0

