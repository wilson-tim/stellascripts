#!/bin/ksh
#set -x

####################################################################################################
#                                                                                                  #
# LOAD_SPECIALIST_AIR_FILES.KSH  Load script for loading specialist air files                      #
#                                                                                                  #
####################################################################################################
# Change                                                                                           #
# ------                                                                                           #
#                                                                                                  #
# Date          Who               Description                                                      #
# ----------    --------------    -----------                                                      #
# 12/05/2006    Jyoti             Created to load  H&J airs from spec_airfiles folder              #
# 16/12/2019    TWilson           Adapted for usage with 11g                                       #
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
# occurring when issue commands such as ls and rm                                                  #
####################################################################################################

. /Oracle/SECTOR/bin/set_ora_env.ksh
#
#
PATH=$PATH:$ORACLE_HOME/bin:/opt/bin:/usr/ccs/bin
export PATH
LD_LIBRARY_PATH=$ORACLE_HOME/lib:/usr/java/lib
export LD_LIBRARY_PATH
#
# set up directory variables
#
. /Oracle/SECTOR/scripts/stella/set_paths.ksh
#

function report
{
   echo `date +%Y/%m/%d---%H:%M:%S` $*
}

today_d=$(date +%d%b%Y)
# today_d=`echo $today_d | sed /.*/y/ADFJMNOS/adfjmnos/`
error_log="${checkdir}/log.lst"
error_log2="${checkdir}/emptylog.lst"
error_mail_list="${appdir}/appsupportmail.lst"
user_mail_list="${appdir}/spec_usermail.lst"
mailmessage="${appdir}/mail_message.txt"

starttime=""
filesread=0
filessuccess=0
filesrecycled=0
fileserror=0
numtickets=0
endtime=""

#city_listing=${appdir}/citylist.lst

date1=`date +%Y%m`
date2=`date +%Y%b%d`
archive_directory=${archivedir}/${date1}

report ": Parameters passed: ${*}.\n"

report "--------------------------------------------------------------------------"
report ">>>>>> Stella Specialist AIR Load load_specialist_air_files.ksh"
report "--------------------------------------------------------------------------"

report "Checking input params"
START_STEP_NO=${START_STEP_NO:=00}
starttime=$(date)

while getopts :r: value
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

#step_no=10
#if [[ "${step_no}" -ge "${START_STEP_NO}" ]] then
#  report "Step ${step_no}\n"
#  report "FTPing files from source server"
#  echo "not written yet"
#else
#  report "Step ${step_no} bypassed.\n"
#fi


step_no=20
if [[ "${step_no}" -ge "${START_STEP_NO}" ]] then
  report "Step ${step_no}\n"
  report "Deleting error files which was created by prior run"
  if [ -f ${appdir}/specair_error.err ]
  then
    rm ${appdir}/specair_error.err
  fi

   report "Change ownership/rights on data files"
   echo ${data_path}/airfiles/[0-9]*.AIR | xargs chmod 644 2> /dev/null

 else
    report "Step ${step_no} bypassed.\n"
 fi


#step_no=30
#if [[ "${step_no}" -ge "${START_STEP_NO}" ]] then
#  report "Step ${step_no}\n"
#  report "Moving recycled files  (${recycledir}) into current data directory (${data_path}/spec_airfiles) so they can be processed again"

#  cd ${recycledir}
#  for filename in spec*.AIR
#  do
#    [ -f ${filename} ] || continue
#    mv ${filename} ${data_path}/spec_airfiles/
#  done

#else
#  report "Step ${step_no} bypassed.\n"
#fi

step_no=35
if [[ "${step_no}" -ge "${START_STEP_NO}" ]] then
  report "Step ${step_no}\n"
  report "Checking for duplicate files"
  if [ -f ${error_log} ]
  then
    rm ${error_log}
  fi

  cd ${data_path}/spec_airfiles
  for filename in spec*.AIR
  do
    [ -f ${filename} ] || continue
    echo "Checking file ${filename}"
    if [ -f ${backupdir}/${filename} ]
    then 
      echo "${filename}" >> ${error_log}
    fi
  done
  
  if [ -s ${error_log} ]
  then
    echo "The following files already exist in the backup directory" > ${mailmessage}
    echo " it appears we are trying to process same file twice" >> ${mailmessage}
    echo " The specialist air load was cancelled" >> ${mailmessage}
    cat ${error_log} >> ${mailmessage}
  
    cat ${error_mail_list}|while read users
    do
      echo ${users}
      mailx -s "STELLA Specialist AIR load `hostname` on ${today_d} - Duplicate files found" ${users} < ${mailmessage}
    done
    report "Exiting Shell"
    exit
  fi

  report "No files already exist - continuing with process"
else
  report "Step ${step_no} bypassed.\n"
fi


step_no=40
if [[ "${step_no}" -ge "${START_STEP_NO}" ]] then
  report "Step ${step_no}\n"
  report "List of contents of data file directory before actual processing starts"

  if [ -f ${appdir}/load_specialist_air_files.lst ]
  then
    rm ${appdir}/load_specialist_air_files.lst
  fi

  cd ${data_path}/spec_airfiles

  if [ $(ls -C1 spec*.AIR 2> /dev/null | wc -l) -gt 0 ] 
  then
    ls -C1 spec*.AIR 2> /dev/null | xargs -n 1 echo > ${appdir}/load_specialist_air_files.lst
  else
    echo "WARNING No files were found to process" >> ${appdir}/specair_error.err 
  fi

  report "Run the stored proc that loads the specialist AIR files into the database"
  
  # Clear the INTEGRATION_ERRORS table
sqlplus -S -L ${dbuser}/${dbpass} << eof_marker
  set serveroutput on size 1000000

  set linesize 200

  BEGIN  
    DELETE FROM INTEGRATION_ERRORS
    WHERE ERROR_DATE < TRUNC(SYSDATE);

    COMMIT;

  END;

/

EXIT
eof_marker

  for filename in spec*.AIR
  do
    [ -f ${filename} ] || continue
    echo "Loading "${filename}

    ((filesread+=1))

    logfile="load_specair_${filename}.log"
    echo "Logfile "${logfile}

    ${appdir}/load_specialist_air_file.ksh ${filename} > ${load_log_path}/${logfile} 2>&1

    if [[ -s ${load_log_path}/${logfile} ]]
    then
      # Check whether any error(s) have been logged
      grep -q -i -E '^FAIL|^ERROR |^ERROR,|^SEVERE|^WARNING|^CRITICAL' ${load_log_path}/${logfile}
      grep_status=$?
      if [[ $grep_status -eq 0 ]]
      then
        # Error(s) have been logged, move AIR file to ${errordir}
        ((fileserror+=1))
        mv ${filename} ${errordir}/
      else
        ((filessuccess+=1))
        mv ${filename} ${stellabakdir}/
      fi

      # Append error details to batch error log
      grep -i "^FAIL" ${load_log_path}/${logfile} >> ${appdir}/specair_error.err 
      grep -i "^ERROR " ${load_log_path}/${logfile} >> ${appdir}/specair_error.err
      grep -i "^ERROR," ${load_log_path}/${logfile} >> ${appdir}/specair_error.err
      grep -i "^SEVERE" ${load_log_path}/${logfile} >> ${appdir}/specair_error.err
      grep -i "^WARNING" ${load_log_path}/${logfile} >> ${appdir}/specair_error.err
      grep -i "^CRITICAL" ${load_log_path}/${logfile} >> ${appdir}/specair_error.err
    fi
  done
else
  report "Step ${step_no} bypassed.\n"
fi


step_no=50
if [[ "${step_no}" -ge "${START_STEP_NO}" ]] then
  report "Step ${step_no}\n"
  report "Email error report to users"
  if [ -s ${appdir}/specair_error.err ]
  then
    echo "error found"
    echo >>  ${appdir}/specair_error.err
    echo "Logfile:${logfile}" >> ${appdir}/specair_error.err
    cat ${error_mail_list} ${user_mail_list}|while read users
    do
      echo ${users}
      mailx -s "STELLA Specialist AIR load `hostname` on ${today_d} - Error report" ${users} < ${appdir}/specair_error.err
    done
 
  fi
else
  report "Step ${step_no} bypassed.\n"
fi

endtime=$(date)

step_no=60
if [[ "${step_no}" -ge "${START_STEP_NO}" ]] then
  report "Step ${step_no}\n"
  report "Email processing report to users"
  rm -f ${appdir}/specair_mail.log
  if [[ ! -z "${logfile}" ]] && [[ -s ${load_log_path}/${logfile} ]]
  then
    echo "Today's run log:"  >> ${appdir}/specair_mail.log 
#    echo "Logfile: ${logfile}" >> ${appdir}/specair_mail.log
#    grep "INFO :START" ${logfile} >> ${appdir}/specair_mail.log
#    grep "INFO :Files" ${logfile} >> ${appdir}/specair_mail.log
#    grep "INFO :Num tkts" ${logfile} >> ${appdir}/specair_mail.log
#    grep "INFO :COMPLETE" ${logfile} >> ${appdir}/specair_mail.log
    echo "INFO: START              "${starttime} >> ${appdir}/specair_mail.log
    echo "INFO: Files read         "${filesread} >> ${appdir}/specair_mail.log
    echo "INFO: Files success      "${filessuccess} >> ${appdir}/specair_mail.log
#    echo "INFO: Files recycled     "${filesrecycled} >> ${appdir}/specair_mail.log
    echo "INFO: Files error        "${fileserror} >> ${appdir}/specair_mail.log
#    echo "INFO: Num tkts inserted  "${numtickets} >> ${appdir}/specair_mail.log
    echo "INFO: COMPLETE           "${endtime} >> ${appdir}/specair_mail.log
    cat ${user_mail_list} ${error_mail_list}|while read users
    do
      echo ${users}
      mailx -s "STELLA Specialist AIR load `hostname` on ${today_d} - Summary report" ${users} < ${appdir}/specair_mail.log
    done
  fi
else
  report "Step ${step_no} bypassed.\n"
fi


step_no=70
if [[ "${step_no}" -ge "${START_STEP_NO}" ]] then
  report "Step ${step_no}\n"
  report "Delete old log files"
  echo "about to delete old log files"
  find ${load_log_path} ${load_log_path}/*.* -mtime +8 -exec  ls -ltr  {}  \;
  find ${load_log_path} ${load_log_path}/*.* -mtime +8 -exec  rm  -f  {}  \;
else
  report "Step ${step_no} bypassed.\n"
fi


step_no=80
if [[ "${step_no}" -ge "${START_STEP_NO}" ]] then
  report "Step ${step_no}\n"
  report "Delete old AIR files then archive new AIR files"

  echo "about to remove files from ${stellabakdir} more than 7 days old"
  for arcfiles in `find ${stellabakdir} -name "spec*.AIR" -mtime +7`
  do
    [ -f ${arcfiles} ] || continue
    #echo "deleting ${arcfiles} from backup area (more than 7 days old)" 
    rm ${arcfiles}
  done

  echo ""
  echo "About to archive today's specialist AIR files"

  for arcfiles in `find ${stellabakdir} -name "spec*.AIR" -mtime -1`
  do 
    [ -f ${arcfiles} ] || continue
    #echo "Copying ${arcfiles} into archive directory"
    cp ${arcfiles} ${archivedir}/
  done

  echo "About to tar/compress old AIR files from archive area"

  if [ ! -d ${archive_directory} ]
  then
    mkdir ${archive_directory}
    echo Made ${archive_directory}
  fi

  if [ -d ${archive_directory} ]
  then

    if [[ ! -f ${archive_directory}/spec_air_${date2}.tar.gz ]]
    then
      if [ $(ls -C1 ${archivedir}/spec*.AIR 2> /dev/null | wc -l) -gt 0 ] 
      then
        # daily archive file does not exist, so create one and then compress it
        echo "About to tar up individual files"
          
        ls ${archivedir}/spec*.AIR 2> /dev/null | xargs -n 1 echo > ${archivedir}/tarlist.txt
        tar -cvf ${archive_directory}/spec_air_${date2}.tar -T ${archivedir}/tarlist.txt

        echo "About to compress the file " ${archive_directory}/spec_air_${date2}.tar
        gzip ${archive_directory}/spec_air_${date2}.tar

        echo "about to remove individual files"
        echo ${archivedir}/spec*.AIR | xargs rm
        rm ${archivedir}/tarlist.txt
      else
        echo "No files to tar up"
      fi

    else
      echo "You are running for a duplicate day. Aborting......"
    fi
  
    echo "Finished tar/compress"
  fi

else
  report "Step ${step_no} bypassed.\n"
fi




report "END OF PROGRAM" 
exit 0
