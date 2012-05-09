#!/bin/ksh
#*************************************************************************************************************
#函数名：write_file.sh
#功  能：生成report文件
#参  数：无
#      
#编写人: 
#编写时间 2010-06-13
#    
#输  出: 
#注意事项：
#*************************************************************************************************************

#fReportFileName + dataFileName + " " + strDoneTime + " " + strResultCode

fReportFileName=$1
dataFileName=$2
strDoneTime=$3
strResultCode=$4

if [ $strDoneTime -eq -1 ] ; then
	
	strDoneTime=" "
	
fi


echo "${dataFileName}�${strDoneTime}�${strResultCode}\r" > $fReportFileName
