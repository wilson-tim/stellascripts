#!/bin/ksh
# set -x
######################################################################################################
#                                                                                                    #
# LOAD_SPECIALIST_AIR_CONTROL.KSH  Control script for STELLA (Specialist) AIR file load              #
#                                                                                                    #
# PARAMETERS            $1 set to "TEST" or "LIVE"                                                   #
#                                                                                                    #
######################################################################################################
# CHANGE                                                                                             #
#-------                                                                                             #
#                                                                                                    #
# Date          Who               Description                                                        #
# ----------    --------------    -----------                                                        #
# 16/12/2019    TWilson                Initial Creation                                              #
#                                                                                                    #
# NOTES                                                                                              #
#                                                                                                    #
# 30 07 * * 1-5   /Oracle/SECTOR/scripts/stella/load_specialist_air_control.ksh LIVE 1>>/Oracle/SECTOR/scripts/stella/log/dw_cron.log1 2>>/Oracle/SECTOR/scripts/stella/log/dw_cron.log2
#######################################################################################################
#
. /Oracle/SECTOR/bin/set_ora_env.ksh
#
#
PATH=$PATH:$ORACLE_HOME/bin:/opt/bin:/usr/ccs/bin
export PATH
LD_LIBRARY_PATH=$ORACLE_HOME/lib:/usr/java/lib
export LD_LIBRARY_PATH
#
# Start Process
#
version=$1
#
# Set up directory variables
#
. /Oracle/SECTOR/scripts/stella/set_paths.ksh
#
# Set up local variables
#
file_date=$(date +%Y%m%d)
log_file=${log_path}/dw_load.log
#
echo "-------------------------------------------------------------------------------------------------"
echo "Load processing for STELLA (Specialist) AIR files started ("${version}") "$(date)
echo "-------------------------------------------------------------------------------------------------"
#
echo "Load processing for STELLA (Specialist) AIR files started ("${version}") "$(date) >> ${log_file}
#
if [ -f ${log_path}/load_specialist_air${file_date}.log ]
then
  rm ${log_path}/load_specialist_air${file_date}.log
fi
#
if [ -f ${log_path}/load_specialist_air${file_date}.err ]
then
  rm ${log_path}/load_specialist_air${file_date}.err
fi
#
if [[ $(ps -ef | grep "load_specialist_air.ksh" | grep -v grep | wc -l) -ne 0 ]] 
then
       echo "***********************************************************************"
       echo "*** PROCESS load_specialist_air.ksh ALREADY RUNNING, CANNOT RESTART ***"
       echo "***********************************************************************"
       exit
fi
#
#
# Move and rename (renumber) files to ensure unique filenames
sourceDir="/Oracle/SECTOR/data/reports/stella/.spec_airfiles-ftp-tmp"
destDir="/Oracle/SECTOR/data/reports/stella/spec_airfiles"

prepString="spec"
digiLen=5
fileExt=".AIR"

i=$(cat "/home/oraclestr/.count-air-spec")
ls -a $sourceDir | tail -n +3 | while read fileName; do
	newFileName=$(printf "$prepString%$(($digiLen-${#i}))s$i$fileExt" | sed 's/ /0/g')
	((i++))
	if [ "$fileName" != "$newFileName" ]; then
		echo "$fileName > $newFileName "
	else
		echo "$fileName "
	fi
	mv "$sourceDir/$fileName" "$destDir/$newFileName"
	if [ "$?" -eq "0" ]; then
		echo $i > "/home/oraclestr/.count-air-spec"
	fi
done
#
#
# Load files
${appdir}/load_specialist_air_files.ksh >> ${log_path}/load_specialist_air${file_date}.log 2>&1
#
echo "-------------------------------------------------------------------------------------------------"
echo "Load processing for STELLA (Specialist) AIR files ended ("${version}") "$(date)
echo "-------------------------------------------------------------------------------------------------"
#
echo "Load processing for STELLA (Specialist) AIR files ended ("${version}") "$(date) >> ${log_file}
#
#
#

