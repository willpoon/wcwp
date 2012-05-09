#!/bin/ksh
#*******************************************************
#函数名：interface_monitor.sh
#功  能：生成接口加载告警信息,并插入短信发送表
#参  数： 
#author: zhaolp2      
#日  期: 2010-03-22	      
#输  出: 向短信发送表插入告警信息
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


#在DB2数据库中执行SQL
DB2_SQL_EXEC()
{
db2 connect to $DB2_OSS_DB user $DB2_OSS_USER using $DB2_OSS_PASSWD
eval $DB2_SQLCOMM
db2 commit
db2 terminate
}



#数据库连接信息
DB2_OSS_DB="bassdb"
DB2_OSS_USER="bass2"
DB2_OSS_PASSWD="0312004131"

DB2_OSS_PASSWD=`/bassdb1/etl/L/boss/decode ${DB2_OSS_PASSWD}`
echo ${DB2_OSS_PASSWD}



while [ true ] 
do

error_flag="0"
#增加对语音清单和短信清单的执行监控：load_boss_cdr.sh 每天凌晨1点执行，在3点的时候检查数据load 是否成功，如果没有成功，则认为此程序存在问题。
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
        send_message="A05001语音清单未正常加载，请检查原因，并及时处理！"
        error_flag="1"
        send_sms2
   fi     
fi


#增加对语音清单和短信清单的执行监控：load_boss_cdr.sh 每天凌晨1点执行，在3点的时候检查数据load 是否成功，如果没有成功，则认为此程序存在问题。
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
        send_message="A05002短信清单未正常加载，请检查原因，并及时处理！"
        error_flag="1"
        send_sms2
   fi     
fi


#added end

#用户接口加载正常告警
echo `date`
time=`date +%H%M`
echo ${time}

if [ ${time} -gt "0310" ] ; then
   DB2_SQLCOMM="db2 \"select 'xxxxxx',count(cycle_id) task_num from BASS2.ETL_TASK_LOG where task_id='I11001' and cycle_id=replace(char(date(current timestamp- 1 days),ISO),'-','')\""
   task_num=`DB2_SQL_EXEC | grep xxxxxx|awk '{print $2}'`
   echo task_num=${task_num}
   if [ ${task_num} = "0" ] ; then
        time_str=`date`
        send_message="接口I11001：产品规格实例(用户表)接口未正常加载,请检查原因！"
        error_flag="1"
        send_sms2
   fi     
fi


#帐户接口加载正常告警
echo `date`
time=`date +%H%M`
echo ${time}

if [ ${time} -gt "0330" ] ; then
   DB2_SQLCOMM="db2 \"select 'xxxxxx',count(cycle_id) task_num from BASS2.ETL_TASK_LOG where task_id='I13001' and cycle_id=replace(char(date(current timestamp- 1 days),ISO),'-','')\""
   task_num=`DB2_SQL_EXEC | grep xxxxxx|awk '{print $2}'`
   echo task_num=${task_num}
   if [ ${task_num} = "0" ] ; then
        time_str=`date`
        send_message="接口I13001：帐户表未正常加载,请检查原因！"
        error_flag="1"
        send_sms2
   fi     
fi



#客户接口加载正常告警
echo `date`
time=`date +%H%M`
echo ${time}

if [ ${time} = "0420" ] ; then
   DB2_SQLCOMM="db2 \"select 'xxxxxx',count(cycle_id) task_num from BASS2.ETL_TASK_LOG where task_id='I11101' and cycle_id=replace(char(date(current timestamp- 1 days),ISO),'-','')\""
   task_num=`DB2_SQL_EXEC | grep xxxxxx|awk '{print $2}'`
   echo task_num=${task_num}
   if [ ${task_num} = "0" ] ; then
        time_str=`date`
        send_message="接口I11101：个人客户表未正常加载,请检查原因！"
        error_flag="1"
        send_sms2
   fi     
fi



#统计三户资料表的生成时间,加载记录条数
echo `date`
time=`date +%H%M`
echo ${time}

if [ ${time} -gt "0430" ] ; then
   DB2_SQLCOMM="db2 \"select 'xxxxxx',count(cycle_id) task_num from BASS2.ETL_TASK_LOG where task_id in ('I11001','I13001','I11101') and cycle_id=replace(char(date(current timestamp- 1 days),ISO),'-','')\""
   task_num=`DB2_SQL_EXEC | grep xxxxxx|awk '{print $2}'`
   echo task_num=${task_num}
   if [ ${task_num} != "3" ] ; then
        time_str=`date`
        send_message="三户资料表未按时生成,请检查原因！"
        error_flag="1"
        send_sms2
   else 
   	DB2_SQLCOMM="db2 \"select 'xxxxxx',task_id,stime,load_sheet_cnt,script_name||'接口' from BASS2.ETL_TASK_LOG where task_id in ('I11001','I13001','I11101') and cycle_id=replace(char(date(current timestamp- 1 days),ISO),'-','')\""
   	
	
   	
   	send_message2=""
while read task_id stime load_sheet_cnt script_name
do   	
	echo "${script_name}${task_id} 加载时间:$stime,加载记录条数:$load_sheet_cnt"
   	send_message2="$send_message2 ${script_name}${task_id} 加载时间:$stime,加载记录条数:$load_sheet_cnt"
   	
done<<!
 	`DB2_SQL_EXEC | grep xxxxxx|awk '{printf("%s\t%s\t%s\t%s\n",$2,$3,$4,$5)}'`
!
   	
   
   	
   fi  
      
fi

echo $send_message2

#二经用户资料表生成告警
echo `date`
time=`date +%H%M`
echo ${time}

if [ ${time} -gt "0530" ] ; then
   DB2_SQLCOMM="db2 \"select 'xxxxxx',count(*) from APP.sch_control_runlog where control_code in ('BASS2_Dw_product_ds.tcl') and ( char(date(begintime),ISO)<>char(date(current timestamp),ISO) or flag<>0 )\""
   task_num=`DB2_SQL_EXEC | grep xxxxxx|awk '{print $2}'`
   echo task_num=${task_num}
   if [ ${task_num} = "1" ] ; then
        time_str=`date`
        send_message="二经用户资料表未按时生成,请检查原因！"
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




##BOSS接口加载数告警
#echo `date`
#time=`date +%H%M`
#echo ${time}
#if [ ${time} -gt "0600" ] ; then
#   DB2_SQLCOMM="db2 \"insert into APP.SMS_SEND_INFO(MESSAGE_CONTENT,mobile_num)  select  '早上6点时BOSS日接口加载情况(总数:' || rtrim( char(a.task_num) ) ||',已加载数:'|| rtrim( char(b.task_num)) ||'),请及时处理!', c.phone_id from  ( select count(*) task_num from APP.SCH_CONTROL_TASK where control_code like 'TR1_L_0%' and deal_time=1 and control_code not in ('TR1_L_02017','TR1_L_01227')) a, ( select count(distinct task_id) task_num from BASS2.ETL_TASK_LOG where task_id like '_0%' and cycle_id=replace(char(date(current timestamp- 1 days),ISO),'-','') and task_id not in ('I01227') ) b, BASS2.ETL_SEND_MESSAGE c where c.module in ('LOAD','OTHER')\""
#   DB2_SQL_EXEC     
#fi


message_len=`echo $send_message2 | awk -F '{print length($1)}'`

echo `date`
time=`date +%H%M`
echo ${time}
if [ ${time} -gt "0700" -a $message_len -ne 0 ] ; then
	send_message2="三户资料表已按时生成"$send_message2
	send_sms
fi


echo "程序正常结束！"
