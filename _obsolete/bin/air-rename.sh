#!/bin/bash
#
# set up directory variables
#
. /Oracle/SECTOR/scripts/stella/set_paths.ksh

workDir="/data/stella/airfiles"
tempDir="/data/stella/renamed"

if [ ! -d "$tempDir" ]; then
        mkdir $tempDir
fi

prepString="spec"
digiLen=5
fileExt=".AIR"

i=1
if [ -n "$1" ]; then
i=$1
fi
ls -a $workDir | tail -n +3 | while read fileName; do
        newFileName=$(printf "$prepString%$(($digiLen-${#i}))s$i$fileExt" | sed 's/ /0/g')
        cp "$workDir/$fileName" "$tempDir/$newFileName"
        ((i++))
done

#rmdir "$workDir"
#mv "$tempDir" "$workDir"

