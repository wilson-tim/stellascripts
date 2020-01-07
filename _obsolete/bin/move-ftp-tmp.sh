!/bin/bash

## move the airfilles with the correct number from the temp ftp folder and print descrep to stdout

sourceDir="/data/stella/.airfiles-ftp-tmp"
destDir="/data/stella/airfiles"

prepString=""
digiLen=5
fileExt=".AIR"

i=$(cat "/home/dw/.count-air")
ls -a $sourceDir | tail -n +3 | while read fileName; do
        newFileName=$(printf "$prepString%$(($digiLen-${#i}))s$i$fileExt" | sed 's/ /0/g')
        ((i++))
        if [ "$fileName" != "$newFileName" ]; then
                echo "$fileName > $newFileName [Bad sequence]..."
        else
                echo "$fileName [Good sequence]..."
        fi
        mv "$sourceDir/$fileName" "$destDir/$newFileName"
        if [ "$?" -eq "0" ]; then
                echo $i > "/home/dw/.count-air"
        fi
done

## move the specialist airfiles with the correct number from the temp ftp folder and print descrep to stdout

sourceDir="/data/stella/.spec_airfiles-ftp-tmp"
destDir="/data/stella/spec_airfiles"

prepString=""
digiLen=5
fileExt=".AIR"

i=$(cat "/home/dw/.count-air-spec")
ls -a $sourceDir | tail -n +3 | while read fileName; do
        newFileName=$(printf "$prepString%$(($digiLen-${#i}))s$i$fileExt" | sed 's/ /0/g')
        ((i++))
        if [ "$fileName" != "$newFileName" ]; then
                echo "$fileName > $newFileName [Bad sequence]..."
        else
                echo "$fileName [Good sequence]..."
        fi
        mv "$sourceDir/$fileName" "$destDir/$newFileName"
        if [ "$?" -eq "0" ]; then
                echo $i > "/home/dw/.count-air-spec"
        fi
done

