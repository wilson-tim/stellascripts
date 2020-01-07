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

today_d=`date +%Y%b%d`
today_d=`echo $today_d | sed /.*/y/ADFJMNOS/adfjmnos/`
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
starttime=$(data)

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

step_no=80
if [[ "${step_no}" -ge "${START_STEP_NO}" ]] then
  report "Step ${step_no}\n"
  report "Delete old AIR files then archive new AIR files"
  echo "About to remove files from ${stellabakdir} more than 7 days old"
  for arcfiles in `find ${stellabakdir} -name "[0-9]*.AIR" -mtime +7`
  do
    [ -f ${arcfiles} ] || continue
    #echo "Deleting ${arcfiles} from backup area (more than 7 days old)" 
    rm ${arcfiles}
  done

  echo ""
  echo "About to archive today's AIR files"

  for arcfiles in `find ${stellabakdir} -name "[0-9]*.AIR" -mtime -1`
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

    if [[ ! -f ${archive_directory}/air_${date2}.tar.gz ]] && [[ -s ${appdir}/load_air_files.lst ]]
    then
      # daily archive file does not exist, so create one and then compress it
      echo "About to tar up individual files"
          
      ls ${archivedir}/[0-9]*.AIR 2> /dev/null | xargs -n 1 echo > ${archivedir}/tarlist.txt
      tar -cvf ${archive_directory}/air_${date2}.tar -T ${archivedir}/tarlist.txt

      echo "About to compress the file" ${archive_directory}/air_${date2}.tar
      gzip ${archive_directory}/air_${date2}.tar

      echo "About to remove individual files"
      echo ${archivedir}/[0-9]*.AIR | xargs rm 
      rm ${archivedir}/tarlist.txt

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
