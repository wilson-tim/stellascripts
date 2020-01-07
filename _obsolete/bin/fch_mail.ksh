#!/usr/bin/ksh

  set -x

# ------------------------------------------------------------------------------------------------ #
#                                                                                                  #
# Shell Name: fch_mail.ksh                                                                         #
#                                                                                                  #
# Purpose                                                                                          #
# -------                                                                                          #
# 'java' program that sends mail messages with optional file attachments.                          #
#                                                                                                  #
# Notes:                                                                                           #
# 1.Make sure that at least one plain text line appears in each file to be attached to the mail.   #
#   Otherwise, if an attached file is empty the mail software (e.g. Lotus Notes) may not be able   #
#   to work out the file type when opening the file, which can cause a problem for the mail        #
#   software.                                                                                      #
# 2.If a parameter contains embedded spaces (e.g. in the mail subject) then it must be passed with #
#   double quotes else each word will be treated as a separate parameter.                          #
#   An example of how to pass a mail subject with embedded spaces on the command line is:          #
#     fch_mail.ksh "\"Mail subject with spaces\"" - Escape character before each double quote.     #
#                                                                                                  #
# ------------------------------------------------------------------------------------------------ #
#  Problems And Workarounds                                                                        #
#  ------------------------                                                                        #
#                                                                                                  #
# Date      By         Description Of Problem                Workaround                            #
# --------  ---------- -----------------------------------   ------------------------------------- #
# 10/06/03  A.James    First line of mail message is being   Add a blank line to top of mail file. #
#                      removed when mail is sent.                                                  #
# ------------------------------------------------------------------------------------------------ #
#  History                                                                                         #
#  -------                                                                                         #
#                                                                                                  #
# Date      By         Description                                                                 #
# --------  ---------- --------------------------------------------------------------------------- #
# 06/05/03  A.James    Initial version.                                                            #
# 29/01/04  A.James    Mail host 'cra_nta2' being de-commissioned hence changed to 'cra_nta1'.     #
# 27/10/05  L.Ashton   Mailhost changed to use DNS entry of smtp                                   #
# 30/04/07  A.James    From address changed to include hostname so the host will appear in the     #
#                      'who' when the e-mail is received. This enables people to see where the job #
#                      has been run immediately.                                                   #
# ------------------------------------------------------------------------------------------------ #

# ------------------------------------------------------------------------------------------------ #
#                                                                                                  #
# Input Parameters                                                                                 #
# ----------------                                                                                 #
#                                                                                                  #
# $1   Mail subject.                                                                               #
# $2   Full name (including path) of file containing the text of the mail message.                 #
# $3   Separator character(s) (usually a comma) used to separate entries in a list.                #
# $4   Full name (including path) of file containing the list of recipients of the mail message.   #
# $5   List of files to be attached to the mail message (including path). Each entry is separated  #
#      by the separator character(s) passed in parameter 2. [Optional parameter]                   #
#                                                                                                  #
# Option Flags                                                                                     #
# ------------                                                                                     #
#  d   Turn java debugging on.                                                                     #
#                                                                                                  #
# Example Runs                                                                                     #
# ------------                                                                                     #
# 1.To send a mail with two attached files (note all parameters enclosed in double quotes):        #
#   fch_mail.ksh "\"Test Mail subject\"" "/data/testmail.msg" "," "/data/mailuser.list" "/data/attach.file1,/data/attach.file2"
#                                                                                                  #
# 2.To send a mail without any file attachments (note all parameters enclosed in double quotes):   #
#   fch_mail.ksh "\"Test Mail subject\"" "/data/testmail.msg" "," "/data/mailuser.list"
#                                                                                                  #
# ------------------------------------------------------------------------------------------------ #

. /home/dw/bin/set_oracle_variables.ksh
. /home/dw/bin/set_java_variables.ksh

# ------------------------------------------------------------------------------------------------ #
#                                                                                                  #
# The following function echoes the date and time in a particular format and any parameters passed #
# to this function. It can be used to output log messages with a consistent date and time format.  #
#                                                                                                  #
# ------------------------------------------------------------------------------------------------ #

function report
{
  echo "`date +%Y/%m/%d---%H:%M:%S` $*"                 # Format is YYYY/MM/DD---HH24:MI:SS
}

function getJavaParameter
{
#
# Function takes the input parameters and wraps double quotes around them to form a
# standard java parameter.

  echo "\"${*}\""

}

#
# Handle java debug flag if passed.
#

while getopts d value
do

    case $value in
         d) java_debug_mode=true
            ;;
       \?) echo "${0}: unknown option ${OPTARG}"
           exit 2
           ;;
    esac

done

shift `expr ${OPTIND} - 1`

java_debug_mode=${java_debug_mode:=false}                 # Set up default.


#
# Check correct parameters supplied.
#
if [[ "${#}" -lt 4 || "${#}" -gt 5 ]] then
   report "\nUsage: fch_mail 'mail subject' 'mail message file' 'list separator' 'mail recipients file' 'file attachments list'\n"
   report "Usage: ${#} parameters (${*}) supplied.\n"
   report "Aborting java mail (script ${0})...\n"
   exit 2
fi

#
# Check files iused in the mail program are valid.
#
if [[ ! -s "${2}" ]] then
   report "\nFile Error: Mail message file (${2}) does not exist or is empty...\n"
   report "Aborting java mail (script ${0})...\n"
   exit 1
fi

if [[ ! -s "${4}" ]] then
   report "\nFile Error: Mail recipients file (${4}) does not exist or is empty...\n"
   report "Aborting java mail (script ${0})...\n"
   exit 1
fi

# ------------------------------------------------------------------------------------------------ #
#                                                                                                  #
# Set up java parameters to be used in the mail program.                                           #
#                                                                                                  #
# ------------------------------------------------------------------------------------------------ #

#mail_host='cra_nta1'                       # Mail host for sending e-mails.
mail_host='localhost'                       # Mail host for sending e-mails.
mail_host=`getJavaParameter ${mail_host}`

list_separator=$3                       # Separator used in in lists for mail.

java_list_separator=${list_separator}                       # java parameter passed as list separator in mail.
java_list_separator=`getJavaParameter ${java_list_separator}`

# from_mail_address=${LOGNAME}                                # Mail 'from' address.
from_mail_address="`hostname`.${LOGNAME}@`hostname`"
from_mail_address=`getJavaParameter ${from_mail_address}`

#
# !!!! Do not set mail subject to a java parameter as it will add further double quotes which may have
# already been set up in call to module.
#
mail_subject=$1

mail_msg_file=$2
mail_msg_file=`getJavaParameter ${mail_msg_file}`

#
# Following variable contains the name of the file that has the mail addresses of the people who
# should receive the log and error files from this run.
#
to_mail_list_file=$4

field_separator=""
for mailuser in `cat ${to_mail_list_file}`
do
    to_mail_list="${to_mail_list}${field_separator}${mailuser}"
    field_separator="${list_separator}"
done

#
# Set mailist to standard java parameter.
#
to_mail_list=`getJavaParameter ${to_mail_list}`

#
# Set up attachment file list.
#

# attach_file_list="\"${LOGS}/${0}${RUNDATE}.log${list_separator}${LOGS}/${0}${RUNDATE}.err\""
attach_file_list=$5

#
# Set attachment list to standard java parameter.
#

attach_file_list=`getJavaParameter ${attach_file_list}`

# ------------------------------------------------------------------------------------------------ #
#                                                                                                  #
# End of set up of java parameters.                                                                #
#                                                                                                  #
# ------------------------------------------------------------------------------------------------ #


#
# Send mail (with attachments if any).
#
# N.B. Use eval to resolve parameters that contain spaces.
#

echo "PATH: ${PATH}"
echo "CLASSPATH: ${CLASSPATH}"

eval nohup java uk.co.firstchoice.util.mail.SendMailUsingLists ${mail_host} ${java_list_separator} ${to_mail_list} ${from_mail_address} ${mail_subject} ${mail_msg_file} ${attach_file_list} ${java_debug_mode} &

exit_code=${?}
report "Exit Status of ${exit_code} returned...\n"
# Check it ran successfully.
if [[ "${exit_code}" -ne 0 ]] then

   report "Failure running java mail program.\n"
   report "Exit Status of ${exit_code} returned...\n"
   report "Aborting mail program (script ${0})...\n"

else
   report "Mail sent successfully.\n"
fi

#
# Exit with exit code returned.
#

exit ${exit_code}

