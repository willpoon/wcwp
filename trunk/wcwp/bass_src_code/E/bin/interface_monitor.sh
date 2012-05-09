#!/bin/ksh
#*******************************************************
#��������interface_monitor.sh
#��  �ܣ����ɽӿڼ��ظ澯��Ϣ,��������ŷ��ͱ�
#��  ���� 
#author: zhaolp2      
#��  ��: 2010-03-22	      
#��  ��: ����ŷ��ͱ����澯��Ϣ
#*******************************************************

 send_message2=""

send_sms2()
{
DB2_SQLCOMM="db2 \"insert into APP.SMS_SEND_INFO(MESSAGE_CONTENT,mobile_num) select '$send_message', phone_id from BASS2.ETL_SEND_MESSAGE where MODULE='LOAD' and phone_id not in ('13628916714')\""
DB2_SQL_EXEC
}

send_sms()
{
DB2_SQLCOMM="db2 \"insert into APP.SMS_SEND_INFO(MESSAGE_CONTENT,mobile_num) select '$send_message2', phone_id from BASS2.ETL_SEND_MESSAGE where MODULE='LOAD' and phone_id not in ('13628916714')\""
DB2_SQL_EXEC
}


#��DB2���ݿ���ִ��SQL
DB2_SQL_EXEC()
{
db2 connect to $DB2_OSS_DB user $DB2_OSS_USER using $DB2_OSS_PASSWD
eval $DB2_SQLCOMM
db2 commit
db2 terminate
}



#���ݿ�������Ϣ
DB2_OSS_DB="bassdb"
DB2_OSS_USER="bass2"
DB2_OSS_PASSWD="0312004131"

DB2_OSS_PASSWD=`/bassdb1/etl/L/boss/decode ${DB2_OSS_PASSWD}`
echo ${DB2_OSS_PASSWD}



while [ true ] 
do

error_flag="0"
#���Ӷ������嵥�Ͷ����嵥��ִ�м�أ�load_boss_cdr.sh ÿ���賿1��ִ�У���3���ʱ��������load �Ƿ�ɹ������û�гɹ�������Ϊ�˳���������⡣
echo `date`
time=`date +%H%M`
echo ${time}

if [ ${time} -gt "0300" ] ; then
   DB2_SQLCOMM="db2 \"select 'xxxxxx',count(cycle_id) task_num from BASS2.ETL_TASK_LOG where task_id='A05001' and cycle_id=replace(char(date(current timestamp- 1 days),ISO),'-','')\""
   task_num=`DB2_SQL_EXEC | grep xxxxxx|awk '{print $2}'`
   echo task_num=${task_num}
   if [ ${task_num} = "0" ] ; then
        time_str=`date`
        echo "$time_str: send message about load_boss_cdr.sh!" 
        send_message="A05001�����嵥δ�������أ�����ԭ�򣬲���ʱ����"
        error_flag="1"
        send_sms2
   fi     
fi


#���Ӷ������嵥�Ͷ����嵥��ִ�м�أ�load_boss_cdr.sh ÿ���賿1��ִ�У���3���ʱ��������load �Ƿ�ɹ������û�гɹ�������Ϊ�˳���������⡣
echo `date`
time=`date +%H%M`
echo ${time}

if [ ${time} -gt "0300" ] ; then
   DB2_SQLCOMM="db2 \"select 'xxxxxx',count(cycle_id) task_num from BASS2.ETL_TASK_LOG where task_id='A05002' and cycle_id=replace(char(date(current timestamp- 1 days),ISO),'-','')\""
   task_num=`DB2_SQL_EXEC | grep xxxxxx|awk '{print $2}'`
   echo task_num=${task_num}
   if [ ${task_num} = "0" ] ; then
        time_str=`date`
        echo "$time_str: send message about load_boss_cdr.sh!" 
        send_message="A05002�����嵥δ�������أ�����ԭ�򣬲���ʱ����"
        error_flag="1"
        send_sms2
   fi     
fi


#added end

#�û��ӿڼ��������澯
echo `date`
time=`date +%H%M`
echo ${time}

if [ ${time} -gt "0310" ] ; then
   DB2_SQLCOMM="db2 \"select 'xxxxxx',count(cycle_id) task_num from BASS2.ETL_TASK_LOG where task_id='I11001' and cycle_id=replace(char(date(current timestamp- 1 days),ISO),'-','')\""
   task_num=`DB2_SQL_EXEC | grep xxxxxx|awk '{print $2}'`
   echo task_num=${task_num}
   if [ ${task_num} = "0" ] ; then
        time_str=`date`
        send_message="�ӿ�I11001����Ʒ���ʵ��(�û���)�ӿ�δ��������,����ԭ��"
        error_flag="1"
        send_sms2
   fi     
fi


#�ʻ��ӿڼ��������澯
echo `date`
time=`date +%H%M`
echo ${time}

if [ ${time} -gt "0330" ] ; then
   DB2_SQLCOMM="db2 \"select 'xxxxxx',count(cycle_id) task_num from BASS2.ETL_TASK_LOG where task_id='I13001' and cycle_id=replace(char(date(current timestamp- 1 days),ISO),'-','')\""
   task_num=`DB2_SQL_EXEC | grep xxxxxx|awk '{print $2}'`
   echo task_num=${task_num}
   if [ ${task_num} = "0" ] ; then
        time_str=`date`
        send_message="�ӿ�I13001���ʻ���δ��������,����ԭ��"
        error_flag="1"
        send_sms2
   fi     
fi



#�ͻ��ӿڼ��������澯
echo `date`
time=`date +%H%M`
echo ${time}

if [ ${time} = "0420" ] ; then
   DB2_SQLCOMM="db2 \"select 'xxxxxx',count(cycle_id) task_num from BASS2.ETL_TASK_LOG where task_id='I11101' and cycle_id=replace(char(date(current timestamp- 1 days),ISO),'-','')\""
   task_num=`DB2_SQL_EXEC | grep xxxxxx|awk '{print $2}'`
   echo task_num=${task_num}
   if [ ${task_num} = "0" ] ; then
        time_str=`date`
        send_message="�ӿ�I11101�����˿ͻ���δ��������,����ԭ��"
        error_flag="1"
        send_sms2
   fi     
fi



#ͳ���������ϱ������ʱ��,���ؼ�¼����
echo `date`
time=`date +%H%M`
echo ${time}

if [ ${time} -gt "0430" ] ; then
   DB2_SQLCOMM="db2 \"select 'xxxxxx',count(cycle_id) task_num from BASS2.ETL_TASK_LOG where task_id in ('I11001','I13001','I11101') and cycle_id=replace(char(date(current timestamp- 1 days),ISO),'-','')\""
   task_num=`DB2_SQL_EXEC | grep xxxxxx|awk '{print $2}'`
   echo task_num=${task_num}
   if [ ${task_num} != "3" ] ; then
        time_str=`date`
        send_message="�������ϱ�δ��ʱ����,����ԭ��"
        error_flag="1"
        send_sms2
   else 
   	DB2_SQLCOMM="db2 \"select 'xxxxxx',task_id,stime,load_sheet_cnt,script_name||'�ӿ�' from BASS2.ETL_TASK_LOG where task_id in ('I11001','I13001','I11101') and cycle_id=replace(char(date(current timestamp- 1 days),ISO),'-','')\""
   	
	
   	
   	send_message2=""
while read task_id stime load_sheet_cnt script_name
do   	
	echo "${script_name}${task_id} ����ʱ��:$stime,���ؼ�¼����:$load_sheet_cnt"
   	send_message2="$send_message2 ${script_name}${task_id} ����ʱ��:$stime,���ؼ�¼����:$load_sheet_cnt"
   	
done<<!
 	`DB2_SQL_EXEC | grep xxxxxx|awk '{printf("%s\t%s\t%s\t%s\n",$2,$3,$4,$5)}'`
!
   	
   
   	
   fi  
      
fi

echo $send_message2

#�����û����ϱ����ɸ澯
echo `date`
time=`date +%H%M`
echo ${time}

if [ ${time} -gt "0530" ] ; then
   DB2_SQLCOMM="db2 \"select 'xxxxxx',count(*) from APP.sch_control_runlog where control_code in ('BASS2_Dw_product_ds.tcl') and ( char(date(begintime),ISO)<>char(date(current timestamp),ISO) or flag<>0 )\""
   task_num=`DB2_SQL_EXEC | grep xxxxxx|awk '{print $2}'`
   echo task_num=${task_num}
   if [ ${task_num} = "1" ] ; then
        time_str=`date`
        send_message="�����û����ϱ�δ��ʱ����,����ԭ��"
        error_flag="1"
        send_sms2
   fi     
fi


sleep 600
	echo `date`
	time=`date +%H%M`
	echo ${time}
	
	if [ ${time} -gt "0700" -a $error_flag -eq "0" ] ; then
		break
	fi	
	
	
done




##BOSS�ӿڼ������澯
#echo `date`
#time=`date +%H%M`
#echo ${time}
#if [ ${time} -gt "0600" ] ; then
#   DB2_SQLCOMM="db2 \"insert into APP.SMS_SEND_INFO(MESSAGE_CONTENT,mobile_num)  select  '����6��ʱBOSS�սӿڼ������(����:' || rtrim( char(a.task_num) ) ||',�Ѽ�����:'|| rtrim( char(b.task_num)) ||'),�뼰ʱ����!', c.phone_id from  ( select count(*) task_num from APP.SCH_CONTROL_TASK where control_code like 'TR1_L_0%' and deal_time=1 and control_code not in ('TR1_L_02017','TR1_L_01227')) a, ( select count(distinct task_id) task_num from BASS2.ETL_TASK_LOG where task_id like '_0%' and cycle_id=replace(char(date(current timestamp- 1 days),ISO),'-','') and task_id not in ('I01227') ) b, BASS2.ETL_SEND_MESSAGE c where c.module in ('LOAD','OTHER')\""
#   DB2_SQL_EXEC     
#fi


message_len=`echo $send_message2 | awk -F '{print length($1)}'`

echo `date`
time=`date +%H%M`
echo ${time}
if [ ${time} -gt "0700" -a $message_len -ne 0 ] ; then
	send_message2="�������ϱ��Ѱ�ʱ����"$send_message2
	send_sms
fi


echo "��������������"
