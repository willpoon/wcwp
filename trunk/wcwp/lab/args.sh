#purpose :  processing multi-arguments
cnt=$#
while [ $cnt -gt 0 ]
do
      arg[$cnt]=$cnt
      cnt=`expr $cnt - 1`
      echo ${arg[$cnt]}
done 
