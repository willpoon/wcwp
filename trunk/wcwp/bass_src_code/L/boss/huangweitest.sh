#!/bin/sh


#*******************************************************
#��������Mon_Max_Day 
#��  �ܣ����ÿ���µ��������
#��  ���� 
#      1�����YYYY
#      2������(1-12)
#��  ��: ÿ���µ��������
#*******************************************************
Mon_Max_Day()
{
	day=0
	if [ $2 -gt 0 -a $2 -lt 13 ]
	then
	case $2 in
    1|01|3|03|5|05|7|07|8|08|10|12) day=31;;
    4|04|6|06|9|09|11) day=30;;
    2|02)
    if [ `expr $1 % 4` -eq 0 ]; then
      if [ `expr $1 % 400` -eq 0 ]; then
        day=29
      elif [ `expr $1 % 100` -eq 0 ]; then
        day=28
      else
        day=29
      fi
    else
     day=28
   fi;;
  esac
  printf $day
fi
}

#*******************************************************
#��������Get_Any_Date
#��  �ܣ�ȡ��ĳ���ڵ�������ǰ��������
#��  ����
#     1������ģʽ��-b ��ʾ��ǰ������(������) ��-a ��ʾ���������(������),ȱʡΪ-a 1,������һ��
#     2����ʾ���������,ȱʡΪ1
#     3����ʾ��������,ȱʡΪ��ǰ����
#��  ����������ǰ/�������
#*******************************************************
Get_Any_Date()
{

	Pre=1
	if [ "$2" != "" ]
	then
    Pre=$2
	fi
	if [ "$3" != "" ]
	then 
		sLen=`echo $3 |wc -c|more`
		Len=`expr $sLen - 1`
    #Len=echo "$3"|awk '{printf length($10)}'
    
    if [ $Len != 8 ]
    then   
    	
      echo "�Ƿ���������[$Len]!!"
      exit
    fi  
    
    year=`echo $3|awk '{print substr($1,1,4)}'`
    month=`echo $3|awk '{print substr($1,5,2)}'`
    day=`echo $3|awk '{print substr($1,7,2)}'`
    DateP=$3
        
	else
    month=`date +%m`
    day=`date +%d`
    year=`date +%Y`
    DateP=`date +'%Y%m%d'`
  fi
  if [ "$1" = "-a" ]
	then
    day=`expr $day + $Pre`
    Ss="��"
	else
    day=`expr $day - $Pre`
    Ss="ǰ"
	fi


#****������ʼ****
Max=`Mon_Max_Day $month`

#****�����ǰ�����****
	while [ $day -le 0 ]
	do
    month=`expr $month - 1`
    if [ $month -eq 0 ]
    then
        month=12
        year=`expr $year - 1`
    fi
    Max=`Mon_Max_Day $month`
    day=`expr $day + $Max`
	done

#****����������****
	while [ $day -gt $Max ]
	do
    day=`expr $day - $Max`
    month=`expr $month + 1`
    if [ $month -eq 13 ]
    then
        month=1
        year=`expr $year + 1`
    fi
    Max=`Mon_Max_Day $month`
	done
  if [ `echo "$month" | wc -m` -ne 3 ] ; then
    month=0$month
  fi
  if [ `echo "$day" | wc -m` -ne 3 ] ; then
    day=0$day
  fi
  DateA=`printf "%02s%2s%2s" ${year} ${month} ${day}`  
  printf $DateA                
}

#sDates=`Get_Any_Date -a 1 20090228`

#echo $sDates  

#smon=`Mon_Max_Day 2012 2`
#echo $smon

	
#��shell
#1�����ݿ�������Ϣ
DB2_OSS_DB="bassdb48"
DB2_OSS_USER="bass2"
DB2_OSS_PASSWD="0312004131"

#2��ѭ��ÿ��*.AVL.Z
PROGRAM_NAME="load_boss.sh"
WORK_PATH=/bassdb2/etl/L/boss
TOLOAD_PATH=${WORK_PATH}/toload
ERROR_PATH=${WORK_PATH}/error
BAK_PATH=${WORK_PATH}/backup
STOP_FILE="stop_load_boss"
RUNNING_FILE="running_load_boss"

DB2_OSS_PASSWD=`/bassdb2/etl/L/boss/decode ${DB2_OSS_PASSWD}`
echo $DB2_OSS_PASSWD
	
	#��DB2���ݿ���ִ��SQL
DB2_SQL_EXEC()
{
db2 connect to $DB2_OSS_DB user $DB2_OSS_USER using $DB2_OSS_PASSWD
eval $DB2_SQLCOMM
db2 commit
db2 terminate
}

	#li.you add by 2008-06-22,�����±�־��Ϊ1�Һ�������ʧ�ܽӿ����µ���
	file_num=`ls -l *.AVL.Z |wc -l`
	echo file_num=$file_num
	
	if [ ${file_num} -eq 0 ] ; then
	     DB2_SQLCOMM="db2 \"Select 'xxxxxx',substr(control_code,7,5)  from APP.SCH_CONTROL_RUNLOG where flag=1 and control_code like 'TR1_L_0%'\""
             DB2_SQL_EXEC | grep xxxxxx|awk '{print $2}' > interface_code_huangwei.list
             
             echo "before while\n"
	     while read interface_code_huangwei
	     do
	           echo "${interface_code_huangwei}${sday}�ӿ����¼���!"
	           interface_code_huangwei=`echo ${interface_code_huangwei}`
	           ##mv  ${TOLOAD_PATH}/[P,A,M,I]${interface_code}${sday}000000.AVL.Z   ${WORK_PATH}
	           #mv  ${TOLOAD_PATH}/[P,A,M,I]${interface_code}${sday}000000.CHK     ${WORK_PATH}
	           #mv  ${ERROR_PATH}/[P,A,M,I]${interface_code}${sday}000000.AVL.Z    ${WORK_PATH}
	           #mv  ${ERROR_PATH}/[P,A,M,I]${interface_code}${sday}000000.CHK      ${WORK_PATH}
	           #mv  ${BAK_PATH}/[P,A,M,I]${interface_code}${sday}000000.AVL.Z      ${WORK_PATH}
	           #mv  ${BAK_PATH}/[P,A,M,I]${interface_code}${sday}000000.CHK        ${WORK_PATH}
	     done < ./interface_code_huangwei.list
	     rm ./interface_code_huangwei.list
	     echo "file rm successful!"
	fi	