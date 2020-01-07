#!/bin/ksh
#set -x

####################################################################################################
#                                                                                                  #
# LOAD_AIR.KSH  Load and integrate script for loading and integrating late prices                  #
#                                                                                                  #
####################################################################################################
# Change                                                                                           #
# ------                                                                                           #
#                                                                                                  #
# Date          Who               Description                                                      #
# ----------    --------------    -----------                                                      #
# 04/01/2003    Jyoti             Created script                                                   #
#                                                                                                  #
####################################################################################################
# Input Parameters                                                                                 #
# ----------------                                                                                 #
# Option Flags                                                                                     #
# ------------                                                                                     #
#  r   Step to restart job from if required. If no -r flag supplied then it defaults to 00 which   #
#      means run the whole job.                                                                    # 
#                                                                                                  #
####################################################################################################
# NOTES                                                                                            #
# The xargs command is used throughout this script to prevent parameter list too long errors       #
# occuring when issue commands such as ls and rm                                                   #
####################################################################################################

function report
{
   echo `date +%Y/%m/%d---%H:%M:%S` $*
}

appdir="/home/dw/DWLIVE/stella"
recycledir="/data/stella/airrecycle"
datadir="/data/stella/airfiles"
datasourcedir="/data/stella"
today_d=`date +%Y%b%d`
today_d=`echo $today_d | sed /.*/y/ADFJMNOS/adfjmnos/`
PASSWD=`cat /home/dw/DWLIVE/passwords/stella.txt`
checkdir="/data/stella/filecheck"
error_log="${checkdir}/log.lst"
error_log2="${checkdir}/emptylog.lst"
error_mail_list="${appdir}/appsupportmail.lst"
user_mail_list="${appdir}/usermail.lst"
backupdir="${datasourcedir}/backup"
mailmessage="${appdir}/mail_message.txt"

#city_listing=${appdir}/citylist.lst

f_log_path="/home/dw/DWLIVE/logs/stella"
archivedir="/data/stella_archive"
stellabakdir="/data/stella/backup"
date1=`date +%Y%m`
date2=`date +%Y%b%d`
archive_directory=${archivedir}/${date1}

echo $PASSWD 
report ": Parameters passed: ${*}.\n"

report "--------------------------------------------------------------------------"
report ">>>>>> Stella AIR Load load_air.ksh"
report "--------------------------------------------------------------------------"

# Setting variables
report "Setting variables"
. /home/dw/bin/set_oracle_variables.ksh

report "Checking input params"
START_STEP_NO=${START_STEP_NO:=00}

while getopts d:r: value
do

  case $value in
    r) START_STEP_NO=${OPTARG}
            ;;
    \?) echo "${0}: unknown option ${OPTARG}"
           exit 2
           ;;
  esac

done
echo "start at step " $START_STEP_NO

export START_STEP_NO

report "FTPing files from source server"
step_no=10
report "Step ${step_no}\n"
if [[ "${step_no}" -ge "${START_STEP_NO}" ]] then
  echo "not written yet"
else
  report "Step ${step_no} bypassed.\n"
fi

report "Deleting error files which was created by prior run "
step_no=20
report "Step ${step_no}\n"
if [[ "${step_no}" -ge "${START_STEP_NO}" ]] then
  if [ -f ${appdir}/air_error.err ]
  then
    rm ${appdir}/air_error.err
  fi

   report "files are already in airfiles folder , now trying to change ownership/rights on files"
   echo ${datadir}/[0-9]*.AIR xargs chmod 777

 else
    report "Step ${step_no} bypassed.\n"
 fi


step_no=30
report "Step ${step_no}\n"
if [[ "${step_no}" -ge "${START_STEP_NO}" ]] then
  report "Moving recycle files  (${recycledir}) into current data directory (${datadir}) so they can be processed again"

  cd ${recycledir}
  ls [0-9]*.AIR | xargs -n 1 echo > ${checkdir}/checkit.lst

  cat ${checkdir}/checkit.lst|while read files
  do
    mv ${files} ${datadir}
    
   # mv ${recycledir}/${files} ${datadir}/.
  done

else
  report "Step ${step_no} bypassed.\n"
fi



step_no=35
report "Step ${step_no}\n"
if [[ "${step_no}" -ge "${START_STEP_NO}" ]] then
  report "Checking for dup files"
  if [ -f ${error_log} ]
  then
    rm ${error_log}
  fi

  cd ${datadir}
  ls [0-9]*.AIR | xargs -n 1 echo > ${appdir}/filelist.txt
  cat ${appdir}/filelist.txt|while read file_list 
  do

    echo "Checking file ${file_list}"

    ls -1 ${backupdir}/${file_list} > ${checkdir}/checkit.lst
    cat ${checkdir}/checkit.lst|while read checkfile
    do

      echo "Checking ${checkfile} is equal to ${backupdir}/${file_list}"

      if [ ${checkfile} = ${backupdir}/${file_list} ]
      then
        echo "${file_list}" >> ${error_log} 
      fi
    done

  done

  #if [ -f ${error_log} ]
  if [ -s ${error_log} ]
  then
    echo "The following files already exist in the back up directory" > ${mailmessage}
    echo " it appears we are trying to process same file twice " >> ${mailmessage}
    echo " The air load was cancelled" >> ${mailmessage}
    cat ${error_log} >> ${mailmessage}
  
    cat ${error_mail_list}|while read users
    do
      echo ${users}
      mailx -v -s "STELLA AIR (Flexi) duplicate files exist `hostname` on "$today_d" " ${users} < ${mailmessage}
      #mailx -v -s "STELLA - `hostname` Duplicate Files Exist" ${users} < ${mailmessage}
    done
    report "Exiting Shell"
    exit
  fi

  report "No files already exist - continuing with process"
else
  report "Step ${step_no} bypassed.\n"
fi


step_no=40
report "Step ${step_no}\n"
if [[ "${step_no}" -ge "${START_STEP_NO}" ]] then
  report "List of contents of data file directory before actual processing starts"

  cd ${datadir}
  ls [0-9]*.AIR | xargs -n 1 echo 

  report "Run the stored proc that runs the java program to load the AIR files into database"
  sqlplus -s stella/$PASSWD  @$appdir/run_air_load.sql > $appdir/run_air_load.lst


  report "Show output from java program:"
  cat ${appdir}/run_air_load.lst

  report "Completed load of data"

  report "Logfile used for run was "   
  logfile=`grep "Lf#" ${appdir}/run_air_load.lst | cut -f2 -d#` 
  echo ${logfile}

  grep "^Fail" ${logfile} >> ${appdir}/air_error.err 
  grep "^Error " ${logfile} >> ${appdir}/air_error.err
  grep "^SEVERE" ${logfile} >> ${appdir}/air_error.err
  grep "^WARNING" ${logfile} >> ${appdir}/air_error.err
  grep "^CRITICAL"  ${appdir}/run_air_load.lst  >> ${appdir}/air_error.err
else
  report "Step ${step_no} bypassed.\n"
fi

step_no=50
report "Step ${step_no}\n"
if [[ "${step_no}" -ge "${START_STEP_NO}" ]] then
  # -s tests that size > 0 bytes
  if [ -s ${appdir}/air_error.err ]
  then
    echo "error found"
    echo >>  ${appdir}/air_error.err
    echo "Logfile:${logfile}" >> ${appdir}/air_error.err
    cat ${error_mail_list} ${user_mail_list}|while read users
    do
      echo ${users}
      mailx -v -s "Details of STELLA AIR (Flexi) load errors encountered `hostname`  on "$today_d" " ${users} < ${appdir}/air_error.err
    done
 
  fi
else
  report "Step ${step_no} bypassed.\n"
fi


step_no=60
report "Step ${step_no}\n"
if [[ "${step_no}" -ge "${START_STEP_NO}" ]] then
  rm ${appdir}/air_mail.log
  echo "Todays run log:"  >> ${appdir}/air_mail.log 
  echo "Logfile:${logfile}" >> ${appdir}/air_mail.log
  grep "^INFO :START" ${logfile} >> ${appdir}/air_mail.log
  grep "^INFO :Files" ${logfile} >> ${appdir}/air_mail.log
  grep "^INFO :Num tkts" ${logfile} >> ${appdir}/air_mail.log
  grep "^INFO :COMPLETE" ${logfile} >> ${appdir}/air_mail.log
  cat ${user_mail_list} ${error_mail_list}|while read users
  do
    echo ${users}
    mailx -v -s "STELLA AIR (Flexi) load log `hostname` on "$today_d" " ${users} < ${appdir}/air_mail.log
  done
else
  report "Step ${step_no} bypassed.\n"
fi


step_no=70
report "Step ${step_no}\n"
if [[ "${step_no}" -ge "${START_STEP_NO}" ]] then
  echo "about to delete old files"
  find ${f_log_path} ${f_log_path}/*.*  -mtime +30 -exec  ls  -ltr  {}  \;
  find ${f_log_path} ${f_log_path}/*.*  -mtime +30 -exec  rm  -f    {}  \;
else
  report "Step ${step_no} bypassed.\n"
fi



step_no=80
if [[ "${step_no}" -ge "${START_STEP_NO}" ]] then
  report "Step ${step_no}\n"

  echo "about to remove files from ${stellabakdir} more than 30 days old"
  for arcfiles in `find ${stellabakdir} -name "[0-9]*.AIR" -mtime +30`
  do
    #echo "deleting ${arcfiles} from backup area (more than 30 days old)" 
    rm ${arcfiles}
  done

  echo ""
  echo "about to archive todays air files"

  for arcfiles in `find ${stellabakdir} -name "[0-9]*.AIR" -mtime 0`
  do 
    #echo "copying ${arcfiles} into archiving directory"
    cp ${arcfiles} ${archivedir}/
  done

  echo "gonna tar/compress old air files from archive area"

  if [ ! -d ${archive_directory} ]
  then
    mkdir ${archive_directory}
    echo Made ${archive_directory}
  fi

  if [ -d ${archive_directory} ]
  then

    if [ ! -f ${archive_directory}/air_${date2}.tar.Z ]
    then
      # daily archive file does not exist, so create one and then compress it
      echo "about to tar up individual files"
          
      ls ${archivedir}/[0-9]*.AIR | xargs -n 1 echo > ${archivedir}/tarlist.txt
      tar -cvf ${archive_directory}/air_${date2}.tar -T ${archivedir}/tarlist.txt

      echo "about to compress the file" ${archive_directory}/air_${date2}.tar
      compress ${archive_directory}/air_${date2}.tar

      echo "about to remove individual files"
      echo ${archivedir}/[0-9]*.AIR |xargs rm 
      rm ${archivedir}/tarlist.txt

    else
      echo "You are running for a duplicate day. Aborting......"
    fi
  
    echo "step finished tar/compress"
  fi

else
  report "Step ${step_no} bypassed.\n"
fi




report "END OF PROGRAM" 
exit 0
