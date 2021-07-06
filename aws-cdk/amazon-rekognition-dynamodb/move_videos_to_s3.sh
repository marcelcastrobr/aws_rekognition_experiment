#!/bin/bash
#yourfilenames=`ls ./|grep PORT`
IFS=' '
for eachfile in *.mp4
do 
   aws s3 cp $eachfile s3://amazonrekogntiondynamodb-inboundimages/
   #read -a strarr <<< "$eachfile"
   #if [[ ${#strarr[*]} -eq 4 ]]
   #then 
	   #echo "There are ${#strarr[*]} strings"
#	   echo "rename $eachfile to ${strarr[3]}"
#	   mv "$eachfile" "${strarr[3]}"
#   fi
done
