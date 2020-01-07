# Move and rename (renumber) files...
sourceDir="/Oracle/SECTOR/data/reports/stella/backup"
destDir="/Oracle/SECTOR/data/reports/stella/airfiles"

prepString="spec"
digiLen=5
fileExt=".AIR"

i=$(cat "/home/oraclestr/.count-air-spec")
ls -a $sourceDir | tail -n +3 | while read fileName; do
	newFileName=$(printf "$prepString%$(($digiLen-${#i}))s$i$fileExt" | sed 's/ /0/g')
	((i++))
	if [ "$fileName" != "$newFileName" ]; then
		echo "$fileName > $newFileName [Bad sequence]..."
	else
		echo "$fileName [Good sequence]..."
	fi
echo ${fileName}" "${newFileName}
#	mv "$sourceDir/$fileName" "$destDir/$newFileName"
#	if [ "$?" -eq "0" ]; then
#		echo $i > "/home/dw/.count-air"
#	fi
done

