#!/bin/ksh
#*************************************************************************************************************
#��������write_file.sh
#��  �ܣ�����report�ļ�
#��  ������
#      
#��д��: 
#��дʱ�� 2010-06-13
#    
#��  ��: 
#ע�����
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
