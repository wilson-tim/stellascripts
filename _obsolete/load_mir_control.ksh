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

$dir_path/load_mir.ksh  >${log_path}/load_mir`date +%Y%m%d`.log 2 >>${log_path}/load_mir`date +%Y%m%d`.log
