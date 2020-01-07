#!/bin/ksh
#set -x

####################################################################################################
#                                                                                                  #
# LOAD_SPECIALIST_AIR.KSH  Load and integrate script for loading specialist air files              #
#                                                                                                  #
####################################################################################################
# Change                                                                                           #
# ------                                                                                           #
#                                                                                                  #
# Date          Who               Description                                                      #
# ----------    --------------    -----------                                                      #
# 12/5/06       Jyoti             created to load  H&J airs from spec_airfiles folder 
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

spec_airfiles="${datasourcedir}/spec_airfiles"
today_d=`date +%Y%b%d`
today_d=`echo $today_d | sed /.*/y/ADFJMNOS/adfjmnos/`
error_log="${checkdir}/log.lst"
error_log2="${checkdir}/emptylog.lst"
error_mail_list="${appdir}/appsupportmail.lst"
user_mail_list="${appdir}/spec_usermail.lst"
mailmessage="${appdir}/mail_message.txt"
specair_prefix="spec"

date1=`date +%Y%m`
date2=`date +%Y%b%d`

report ": Parameters passed: ${*}.\n"

report "--------------------------------------------------------------------------"
report ">>>>>> Stella Specialist AIR Load load__specialist_air.ksh"
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



report "Copying files to airfiles folder from  spec_airfiles "
# also renames each files with  prefix (spec_)
step_no=20
report "Step ${step_no}\n"
if [[ "${step_no}" -ge "${START_STEP_NO}" ]] then
  if [ -f ${appdir}/specair_error.err ]
  then
    rm ${appdir}/specair_error.err
  fi

    cd ${spec_airfiles}
    ls *.* | xargs -n 1 echo > ${checkdir}/checkit.lst

    wc -l ${checkdir}/checkit.lst > ${checkdir}/specair_filecount.lst
    cat ${checkdir}/specair_filecount.lst|while read file_num file_name
    do
      if [ ${file_num} -eq 0 ]
      then
        echo "WARNING WARNING WARNING WARNING WARNING"
        echo "No files found in directory ${spec_airfiles}
        echo ""

        echo "WARNING WARNING WARNING WARNING WARNING" >> ${appdir}/specair_error.err
        echo "No files found in directory  ${spec_airfiles}>> ${appdir}/specair_error.err
        echo "" >> ${appdir}/specair_error.err
      fi
    done

    echo "Moving files from  ${spec_airfiles} to ${datadir}"
    cat ${checkdir}/checkit.lst|while read files
    do
      mv -f  ${spec_airfiles}/${files} ${spec_airfiles}/${specair_prefix}${files}
      
      echo " moving files to ${datadir}" 
      
      mv  ${spec_airfiles}/${specair_prefix}${files} ${datadir}/${specair_prefix}${files}
    done

  #done

  report "finished moving files, now trying to change ownership/rights on files"
  echo ${datadir}/*.*|xargs chmod 777

else
   report "Step ${step_no} bypassed.\n"
fi

############


step_no=30
report "Step ${step_no}\n"
if [[ "${step_no}" -ge "${START_STEP_NO}" ]] then
  report "Moving recycle files  (${recycledir}) into current data directory (${datadir}) so they can be processed again"

  cd ${recycledir}
  ls ${specair_prefix}*.AIR | xargs -n 1 echo > ${checkdir}/checkit.lst
  cat ${checkdir}/checkit.lst|while read rfiles
  do
    cp ${recycledir}/${rfiles} ${datadir}/${rfiles}
   # mv ${recycledir}/${rfiles} ${datadir}/.
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
  ls ${specair_prefix}*.AIR | xargs -n 1 echo > ${appdir}/filelist.txt
  cat ${appdir}/filelist.txt|while read file_list 
  do

    #echo "Checking file ${file_list}"

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
    echo " it appears we are trying to process same file  twice " >> ${mailmessage}
    echo " The specialist air load was cancelled" >> ${mailmessage}
    cat ${error_log} >> ${mailmessage}
  
    cat ${error_mail_list}|while read users
    do
      echo ${users}
      mailx  -s "STELLA - `hostname` Duplicate Files Exist" ${users} < ${mailmessage}
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
  ls ${specair_prefix}*.AIR | xargs -n 1 echo 

  report "Run the stored proc that runs the java program to load the AIR files into database"
  sqlplus -s stella/$PASSWD  @$appdir/run_air_load.sql > $appdir/run_specair_load.lst


  report "Show output from java program:"
  cat ${appdir}/run_specair_load.lst

  report "Completed load of data"

  report "Logfile used for run was "   
  logfile=`grep "Lf#" ${appdir}/run_specair_load.lst | cut -f2 -d#` 
  echo ${logfile}

# removed -i otherwise repeats same line twice in e mail 
  grep  "Fail" ${logfile} >> ${appdir}/specair_error.err 
  grep   "Error " ${logfile} >> ${appdir}/specair_error.err
  grep   "SEVERE" ${logfile} >> ${appdir}/specair_error.err
  grep  "WARNING" ${logfile} >> ${appdir}/specair_error.err
  grep  "CRITICAL"  ${appdir}/run_specair_load.lst  >> ${appdir}/specair_error.err
else
  report "Step ${step_no} bypassed.\n"
fi

step_no=50
report "Step ${step_no}\n"
if [[ "${step_no}" -ge "${START_STEP_NO}" ]] then
  # -s tests that size > 0 bytes
  if [ -s ${appdir}/specair_error.err ]
  then
    echo "error found"
    echo >>  ${appdir}/specair_error.err
    echo "Logfile:${logfile}" >> ${appdir}/specair_error.err
    cat ${error_mail_list} ${user_mail_list}|while read users
    do
      echo ${users}
      mailx  -s "Details of STELLA Specialist AIR  errors encountered `hostname`  on "$today_d" " ${users} < ${appdir}/specair_error.err
    done
 
  fi
else
  report "Step ${step_no} bypassed.\n"
fi


step_no=60
report "Step ${step_no}\n"
if [[ "${step_no}" -ge "${START_STEP_NO}" ]] then
  rm ${appdir}/specair_mail.log
  echo "Todays run log:"  >> ${appdir}/specair_mail.log 
  echo "Logfile:${logfile}" >> ${appdir}/specair_mail.log
  grep "INFO :START" ${logfile} >> ${appdir}/specair_mail.log
  grep "INFO :Files" ${logfile} >> ${appdir}/specair_mail.log
  grep "INFO :Num tkts" ${logfile} >> ${appdir}/specair_mail.log
  grep "INFO :COMPLETE" ${logfile} >> ${appdir}/specair_mail.log
  cat ${user_mail_list} ${error_mail_list}|while read users
  do
    echo ${users}
    mailx  -s "STELLA Specialist AIR load `hostname` on "$today_d" " ${users} < ${appdir}/specair_mail.log
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
  for arcfiles in `find ${stellabakdir} -name "${specair_prefix}*.AIR" -mtime +7`
  do
    #echo "deleting ${arcfiles} from backup area (more than 7 days old)" 
    rm ${arcfiles}
  done

  echo ""
  echo "about to archive todays specialist air files"

  for arcfiles in `find ${stellabakdir} -name "${specair_prefix}*.AIR" -mtime +0 -mtime -1`
  do 
    #echo "copying ${arcfiles} into archiving directory"
    cp ${arcfiles} ${archivedir}/
  done

  echo "gonna tar/compress old specialist air files from archive area"

  if [ ! -d ${archive_directory} ]
  then
    mkdir ${archive_directory}
    echo Made ${archive_directory}
  fi

  if [ -d ${archive_directory} ]
  then

    if [ ! -f ${archive_directory}/specair_${date2}.tar.Z ]
    then
      # daily archive file does not exist, so create one and then compress it
      echo "about to tar up individual files"
          
      ls ${archivedir}/${specair_prefix}*.AIR | xargs -n 1 echo > ${archivedir}/tarlist.txt
      tar -cvf ${archive_directory}/Air_${date2}.tar -L ${archivedir}/tarlist.txt

      echo "about to compress the file" ${archive_directory}/air_${date2}.tar
      compress ${archive_directory}/air_${date2}.tar

      echo "about to remove individual files"
      echo ${archivedir}/${specair_prefix}*.AIR |xargs rm 
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
