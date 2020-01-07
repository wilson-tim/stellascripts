#!/bin/ksh
#set -x
####################################################################################################
# analyse_schema.ksh  script to analyse schema                                                     #
####################################################################################################
# Change                                                                                           #
# ------                                                                                           #
#                                                                                                  #
# Date          Who               Description                                                      #
# ----------    --------------    -----------                                                      #
# 01/09/2002    LAshton           Original                                                         #
# 04/01/2003    Jyoti             Created script                                                   #
# 14/02/2018    SMartin           Migrate to P-GS2-ORA-01                                          #
#                                                                                                  #
####################################################################################################
. /Oracle/SECTOR/bin/set_ora_env.ksh
#
PATH=$PATH:$ORACLE_HOME/bin:/opt/bin:/usr/ccs/bin
export PATH
LD_LIBRARY_PATH=$ORACLE_HOME/lib:/usr/java/lib ; export LD_LIBRARY_PATH
#
# set up directory variables
#
. /Oracle/SECTOR/scripts/stella/set_paths.ksh
# 
echo "About to analyse entire stella schema"
echo "Execute dbms_utility.analyze_schema('STELLA_DATAW','COMPUTE');"|sqlplus -s $dbuser/$dbpass


~
~

