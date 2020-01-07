#!/bin/ksh
#set -x

####################################################################################################
#                                                                                                  #
# LOAD_MIR.KSH  Load and integrate script for loading and integrating late prices                  #
#                                                                                                  #
####################################################################################################
# Change                                                                                           #
# ------                                                                                           #
#                                                                                                  #
# Date          Who               Description                                                      #
# ----------    --------------    -----------                                                      #
# 04/01/2003    Leigh             Created script                                                   #
# 23/07/2003    John Durnford     Amended Step 20 - Check relevant directories for files and alert #
#                                 in log file if none present                                      #
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

function report
{
   echo `date +%Y/%m/%d---%H:%M:%S` $*
}

today_d=`date +%Y%b%d`
today_d=`echo $today_d | sed /.*/y/ADFJMNOS/adfjmnos/`
error_log="${checkdir}/log.lst"
error_log2="${checkdir}/emptylog.lst"
error_mail_list="${appdir}/appsupportmail.lst"
user_mail_list="${appdir}/usermail.lst"
mailmessage="${appdir}/mail_message.txt"
city_listing=${appdir}/citylist.lst
date1=`date +%Y%m`
date2=`date +%Y%b%d`

report ": Parameters passed: ${*}.\n"

report "--------------------------------------------------------------------------"
report ">>>>>> Stella MIR Load load_mir.ksh"
report "--------------------------------------------------------------------------"

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


report "Copying files to main run area from each pseudo area"
# also renames each files with pseudocity prefix
step_no=20
report "Step ${step_no}\n"
if [[ "${step_no}" -ge "${START_STEP_NO}" ]] then
  if [ -f ${appdir}/mir_error.err ]
  then
    rm ${appdir}/mir_error.err
  fi

  cat ${city_listing}|while read citycode
  do
    echo
    echo ${citycode}

    cd ${datasourcedir}/mir${citycode}
    ls *.* | xargs -n 1 echo > ${checkdir}/checkit.lst

    wc -l ${checkdir}/checkit.lst > ${checkdir}/filecount.lst
    cat ${checkdir}/filecount.lst|while read file_num file_name
    do
      if [ ${file_num} -eq 0 ]
      then
        echo "WARNING WARNING WARNING WARNING WARNING"
        echo "No files found in directory mir"${citycode}
        echo ""

        echo "WARNING WARNING WARNING WARNING WARNING" >> ${appdir}/mir_error.err
        echo "No files found in directory mir"${citycode} >> ${appdir}/mir_error.err
        echo "" >> ${appdir}/mir_error.err
      fi
    done

    echo "Moving files from ${datasourcedir}/mir${citycode} to ${datadir}"
    cat ${checkdir}/checkit.lst|while read files
    do
      mv -f ${datasourcedir}/mir${citycode}/${files} ${datasourcedir}/mir${citycode}/${citycode}${files}
      mv ${datasourcedir}/mir${citycode}/${citycode}${files} ${datadir}/.
    done

  done

  report "finished moving files, now trying to change ownership/rights on files"
  echo ${datadir}/*.*|xargs chmod 777

else
   report "Step ${step_no} bypassed.\n"
fi

step_no=30
report "Step ${step_no}\n"
if [[ "${step_no}" -ge "${START_STEP_NO}" ]] then
  report "Moving recycle files  (${recycledir}) into current data directory (${datadir}) so they can be processed again"

  cd ${recycledir}
  ls *.* | xargs -n 1 echo > ${checkdir}/checkit.lst

  cat ${checkdir}/checkit.lst|while read files
  do
    mv ${recycledir}/${files} ${datadir}/.
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
  ls *.* | xargs -n 1 echo > ${appdir}/filelist.txt
  cat ${appdir}/filelist.txt|while read file_list 
  do

    #echo "Checking file ${file_list}"

    ls -1 ${backupdir}/${file_list} > ${checkdir}/checkit.lst
    cat ${checkdir}/checkit.lst|while read checkfile
    do

      #echo "Checking ${checkfile} is equal to ${backupdir}/${file_list}"

      if [ ${checkfile} = ${backupdir}/${file_list} ]
      then
        echo "${file_list}" >> ${error_log} 
      fi
    done

  done

  if [ -f ${error_log} ]
  then
    echo "The following files already exist in the back up directory" > ${mailmessage}
    echo " it appears we are trying to process same file  twice " >> ${mailmessage}
    echo " The mir load was cancelled" >> ${mailmessage}
    cat ${error_log} >> ${mailmessage}
  
    cat ${error_mail_list}|while read users
    do
      echo ${users}
      mailx -s "STELLA - `hostname` Duplicate Files Exist" ${users} < ${mailmessage}
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
  ls *.* | xargs -n 1 echo 

  report "Run the stored proc that runs the java program to load the files into database"
  sqlplus -s ${dbuser}/${dbpass}  @$appdir/run_mir_load.sql > $appdir/run_mir_load.lst


  report "Show output from java program:"
  cat ${appdir}/run_mir_load.lst

  report "Completed load of data"

  report "Logfile used for run was "   
  logfile=`grep "Lf#" ${appdir}/run_mir_load.lst | cut -f2 -d#` 
  echo ${logfile}

  grep "Fail" ${logfile} >> ${appdir}/mir_error.err 
  grep "Error " ${logfile} >> ${appdir}/mir_error.err
  grep "SEVERE" ${logfile} >> ${appdir}/mir_error.err
  grep "WARNING" ${logfile} >> ${appdir}/mir_error.err
  grep "CRITICAL"  ${appdir}/run_mir_load.lst  >> ${appdir}/mir_error.err
else
  report "Step ${step_no} bypassed.\n"
fi

step_no=50
report "Step ${step_no}\n"
if [[ "${step_no}" -ge "${START_STEP_NO}" ]] then
  # -s tests that size > 0 bytes
  if [ -s ${appdir}/mir_error.err ]
  then
    echo "error found"
    echo >>  ${appdir}/mir_error.err
    echo "Logfile:${logfile}" >> ${appdir}/mir_error.err
    cat ${user_mail_list} ${error_mail_list}|while read users
    do
      echo ${users}
      mailx -s "Details of STELLA MIR  errors encountered `hostname`  on "$today_d" " ${users} < ${appdir}/mir_error.err
    done
 
  fi
else
  report "Step ${step_no} bypassed.\n"
fi


step_no=60
report "Step ${step_no}\n"
if [[ "${step_no}" -ge "${START_STEP_NO}" ]] then
  rm ${appdir}/mir_mail.log
  echo "Todays run log:"  >> ${appdir}/mir_mail.log 
  echo "Logfile:${logfile}" >> ${appdir}/mir_mail.log
  grep "INFO :START" ${logfile} >> ${appdir}/mir_mail.log
  grep "INFO :Files" ${logfile} >> ${appdir}/mir_mail.log
  grep "INFO :Num tkts" ${logfile} >> ${appdir}/mir_mail.log
  grep "INFO :COMPLETE" ${logfile} >> ${appdir}/mir_mail.log
  cat ${user_mail_list} ${error_mail_list}|while read users
  do
    echo ${users}
    mailx -s "STELLA MIR load `hostname` on "$today_d" " ${users} < ${appdir}/mir_mail.log
  done
else
  report "Step ${step_no} bypassed.\n"
fi


step_no=70
report "Step ${step_no}\n"
if [[ "${step_no}" -ge "${START_STEP_NO}" ]] then
  echo "about to delete old files"
  find ${f_log_path} ${f_log_path}/*.*  -mtime +8 -exec   ls -ltr  {}  \;
  find ${f_log_path} ${f_log_path}/*.*  -mtime +8 -exec  rm  -f  {}  \;
else
  report "Step ${step_no} bypassed.\n"
fi



step_no=80
if [[ "${step_no}" -ge "${START_STEP_NO}" ]] then
  report "Step ${step_no}\n"

  echo "about to remove files from ${stellabakdir} more than 7 days old"
  for arcfiles in `find ${stellabakdir} -name "*" -mtime +7`
  do
    #echo "deleting ${arcfiles} from backup area (more than 7 days old)" 
    rm ${arcfiles}
  done

  echo ""
  echo "about to archive todays mir files"

  for arcfiles in `find ${stellabakdir} -name "*.MIR" -mtime +0 -mtime -1`
  do 
    #echo "copying ${arcfiles} into archiving directory"
    cp ${arcfiles} ${archivedir}/
  done

  echo "gonna tar/compress old mir files from archive area"

  if [ ! -d ${archive_directory} ]
  then
    mkdir ${archive_directory}
    echo Made ${archive_directory}
  fi

  if [ -d ${archive_directory} ]
  then

    if [ ! -f ${archive_directory}/mir_${date2}.tar.Z ]
    then
      # daily archive file does not exist, so create one and then compress it
      echo "about to tar up individual files"
          
      ls ${archivedir}/*.MIR | xargs -n 1 echo > ${archivedir}/tarlist.txt
      tar -cvf ${archive_directory}/mir_${date2}.tar -L ${archivedir}/tarlist.txt

      echo "about to compress the file" ${archive_directory}/mir_${date2}.tar
      compress ${archive_directory}/mir_${date2}.tar

      echo "about to remove individual files"
      echo ${archivedir}/*.MIR |xargs rm 
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
