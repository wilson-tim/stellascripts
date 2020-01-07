#!/bin/ksh
# set -x       
######################################################################################################
#
# SET_PATHS.KSH        Script to set the paths for all scripts to ensure consistency
#
#-----------------------------------------------------------------------------------------------------
# CHANGE
#-------
#
# Date       Who  Description
# --------   ---  ---------------------------------------------------------------------------------------
# 01/10/08   ML   Initial creation
# 18/11/2016 SM   Added Oracle Paths
#
# -------- ---  ---------------------------------------------------------------------------------------
# 
# Set variables
. /Oracle/SECTOR/bin/set_ora_env.ksh
PATH=$PATH:usr/kerberos/bin:/usr/local/bin:/bin:/usr/bin:/Oracle/SECTOR/bin:/Oracle/SECTOR/app/oraclestr/product/11.2.0/dbhome_1/bin
export PATH
LD_LIBRARY_PATH=$ORACLE_HOME/lib:/usr/java/lib
export LD_LIBRARY_PATH


db_name='Stella'
dw_name='STELLA'
dbuser='STELLA'
dbpass='lager65'
coreuser='core_dataw'
corepass='appl35'
dir_path='/Oracle/SECTOR/scripts/stella'
log_path=${dir_path}'/log'
ctl_path=${dir_path}'/ctl'
bad_path=${dir_path}'/load/bad'
dis_path=${dir_path}'/load/dis'
load_log_path=${dir_path}'/load/log'
data_path='/Oracle/SECTOR/data/reports/stella'
backup_path='{data_path}/backup'
area_id=99
env='Prod'
appdir=${dir_path}
datadir=${data_path}
recycledir="${data_path}/airrecycle"
datasourcedir=${data_path}
checkdir="${data_path}/filecheck"
backupdir="${data_path}/backup"
f_log_path=${log_path}
archivedir="${data_path}/archive"
stellabakdir="${data_path}/backup"
errordir="${data_path}/error"
jutiluser='JUTIL'
jutilpass='procs'
#
export db_name
export dw_name
export dbuser
export dbpass
export coreuser
export corepass
export dir_path
export log_path
export ctl_path
export bad_path
export dis_path
export load_log_path
export data_path
export backup_path
export area_id
export env
#
export appdir
export datadir
export recycledir
export datasourcedir
export checkdir
export backupdir
export f_log_path
export archivedir
export stellabackdir
export errordir
export jutiluser
export jutilpass
#
