#!/usr/bin/ksh

# set -x

# ------------------------------------------------------------------------------------------------ #
#                                                                                                  #
# Shell Name: fch_ftp.ksh                                                                          #
#                                                                                                  #
# Purpose                                                                                          #
# -------                                                                                          #
# FTP's a file from a remote host to a local host.                                                 #
#                                                                                                  #
# ------------------------------------------------------------------------------------------------ #
#  History                                                                                         #
#  -------                                                                                         #
#                                                                                                  #
# Date      By         Description                                                                 #
# --------  ---------- --------------------------------------------------------------------------- #
# 09/12/02 A.James     Initial version.                                                            #
#                      N.B.This shell is based on the shell used for FTP in the Retail Profit      #
#                          load (shell 'retailprofit_ftp.ksh').                                    #
#                                                                                                  #
# ------------------------------------------------------------------------------------------------ #

# ------------------------------------------------------------------------------------------------ #
#                                                                                                  #
# Input Parameters                                                                                 #
# ----------------                                                                                 #
#                                                                                                  #
# $1 Host (either name or ip address) to connect to in order to ftp the file.                      #
# $2 User name for logging on to the remote Host                                                   #
# $3 Password for remote user.                                                                     #
# $4 Directory containing file to be transferred on the remote host.                               #
# $5 Name of file to be transferred on the remote host.                                            #
# $6 Directory containing destination file on the local host.                                      #
# $7 Name of destination file on the local host.                                                   #
#                                                                                                  #
#                                                                                                  #
# Option Flags                                                                                     #
# ------------                                                                                     #
#  i   Retry interval in seconds, i.e. the time in seconds to wait before retrying the FTP.        #
#      The default is 15 minutes (900 seconds).                                                    #
#  m   Maximum number of tries before job times-out with a failure. The default is 1 which means   #
#      try the FTP only once.                                                                      #
#                                                                                                  #
# Example Runs                                                                                     #
# ------------                                                                                     #
#  fch_ftp.ksh -i 300 -m 5 .......                                                                 #
#                   Try the FTP up to 5 times waiting 5 minutes between each retry attempt.        #
#                                                                                                  #
# ------------------------------------------------------------------------------------------------ #

# ------------------------------------------------------------------------------------------------ #
#                                                                                                  #
# To Do                                                                                            #
# -----                                                                                            #
#                                                                                                  #
# ------------------------------------------------------------------------------------------------ #

# ------------------------------------------------------------------------------------------------ #
#                                                                                                  #
# The following function echoes the date and time in a particular format and any parameters passed #
# to this function. It can be used to output log messages with a consistent date and time format.  #
#                                                                                                  #
# ------------------------------------------------------------------------------------------------ #

function report
{
  echo `date +%Y/%m/%d---%H:%M:%S` $*     # Format is YYYY/MM/DD---HH24:MI:SS
}

# ------------------------------------------------------------------------------------------------ #
#                                                                                                  #
#                                    Body of shell follows.                                        #
#                                                                                                  #
# ------------------------------------------------------------------------------------------------ #

# Standard log message at start of script.
report ": Starting script ${0} to use FTP to transfer a file from a remote host.\n"
report ": Parameters passed: ${*}.\n"

#
# Handle retry interval flag and maximum retries flag if passed.
#

#
# Set defaults first.
#
retry_interval_in_secs=${retry_interval_in_secs:=900}
max_tries=${max_tries:=1}

while getopts i:m: value
do

    case $value in
         i) retry_interval_in_secs=${OPTARG}
            ;;
         m) max_tries=${OPTARG}
            ;;
       \?) report "${0}: unknown option ${OPTARG}"
           exit 2
           ;;
    esac

done

shift `expr ${OPTIND} - 1`

remote_host="${1}"
remote_user="${2}"
remote_passwd="${3}"

source_file_loc="${4}"
source_filename="${5}"
target_file_loc="${6}"
target_filename="${7}"

report ": Attempting to transfer the ${source_file} file from the remote host ${remote_host}.\n"

target_file="${target_file_loc}/${target_filename}"

#
if [[ -f "${target_file}" ]]
then
    report "Target file ${target_file} already exists"
    exit 1
fi

(( retry_count = 1 ))
while (( retry_count <= ${max_tries} ))
do

   if (( retry_count > 1 )) then
      report "Sleeping for ${retry_interval_in_secs} seconds..."
      sleep ${retry_interval_in_secs}
   fi

   report
   report "Trying FTP, attempt ${retry_count}"

    ##################################################
    # ftp command with output passed to variable
    FTP_OUTPUT=`ftp -vni << EOF
    open ${remote_host}
    user ${remote_user} ${remote_passwd}
    cd ${source_file_loc}
    lcd ${target_file_loc}
    get ${source_filename} ${target_filename}
    quit
    EOF`
###################################################
#
    echo $FTP_OUTPUT | grep -i "Transfer complete"
    exit_code=${?}

    report "Exit code from grep: ${exit_code}."

    report "Output from FTP: \n${FTP_OUTPUT}\n"

    if [[ "${exit_code}" -ne 0 ]] then
        report "Error during GET of file ${source_file}"
        report "Possible causes:"
        report "1.File to be retrieved may not exist on remote host."
        report "2.FTP failed for some reason. Check output from FTP for errors.\n"

        if [[ -f "${target_file}" ]] then             # If target file exists (maybe partially created), tidy up by removing it.
           rm ${target_file}
        fi

   else

        # FTP succeeded, so break out of retry loop and indicate success so following steps continue.
        report "FTP of file ${source_file} succeeded."

        exit_code=0                                       # File ok and ready to be processed.

        break
   fi

   # FTP failed, so retry until it does succeed or the maximum number of retries has been exceeded.

   (( retry_count = retry_count + 1 ))

done

# ------------------------------------------------------------------------------------------------ #
#                                                                                                  #
#                                          End of shell                                            #
#                                                                                                  #
# ------------------------------------------------------------------------------------------------ #

if [[ "${exit_code}" -eq 0 ]] then
   # Final standard log messages if script completed successfully.
   report ": 'ftp' completed successfully.\n"
   report ": Script '${0}' finished....\n"

else
   # Final standard log messages if script failed.
   report ": 'ftp' failed or timed-out.\n"
   report ": Script '${0}' aborted....\n"
fi

exit ${exit_code}

