#!/usr/bin/ksh
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
# move and rename files...
#${dir_path}/bin/move-ftp-tmp.sh
# run main mapping script and redirect output to date-stamped log file
echo "####################################################" >> ${log_path}/stella/testDWSES384`date +%Y%m%d`.log

${dir_path}/testDWSES384.ksh >> ${log_path}/stella/testDWSES384`date +%Y%m%d`.log 2>&1

