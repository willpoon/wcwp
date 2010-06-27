#purpose :  processing multi-arguments
cnt=$#
while [ $cnt -gt 0 ]
do
      #arg[$cnt]=$($cnt)
	args=("$@")
      cnt=`expr $cnt - 1`
      #echo $cnt ${arg[$cnt]}
echo ${args[$cnt]}  
done 
