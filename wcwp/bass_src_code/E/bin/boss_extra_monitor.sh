#!/bin/ksh

#create by asiainfo LiZhanyong 2009-01-01

#程序功能：监控日接口抽取完成情况，每天凌晨1点、4点、6点进行执行

#在DB2数据库中执行SQL
DB2_SQL_EXEC()
{
db2 connect to $DB2_OSS_DB user $DB2_OSS_USER using $DB2_OSS_PASSWD
eval $DB2_SQLCOMM
db2 commit
db2 terminate
}

#在ORACLE数据库执行SQL
count_extra()
{
sqlplus zk/zk1234@xztestdb <<SQLEND
select count(distinct task_id) extra_num from mb_if_jyfx_task_log where cycle_id=TO_CHAR(SYSDATE,'YYYYMMDD') and task_id not like 'M%';
commit;
SQLEND
}


#主shell
#1、数据库连接信息
DB2_OSS_DB="bassdb48"
DB2_OSS_USER="bass2"
DB2_OSS_PASSWD="0312004131"

DB2_OSS_PASSWD=`/bassdb2/etl/L/boss/decode ${DB2_OSS_PASSWD}`
echo ${DB2_OSS_PASSWD}

#Mon Sep 22 21:37:45 CST 2008

echo `date`
time=`date +%H`
echo ${time}


#BOSS接口抽取告警，每天凌晨1点统计接口抽取数

if [ ${time} = "02" ] ; then	 
   extra_num=count_extra
   DB2_SQLCOMM="db2 \"insert into app.sms_send_info (message_content,mobile_num) select '截至凌晨${time}点，BOSS接口抽取完成总数为$extra_num',phone_id from  BASS2.ETL_SEND_MESSAGE where MODULE='LOAD' and phone_id in ('13618999501','13989094821')\""
   echo DB2_SQLCOMM
   DB2_SQL_EXEC
fi


#BOSS接口抽取告警，每天凌晨4点统计接口抽取数
if [ ${time} = "04" ] ; then
   extra_num=count_extra
   DB2_SQLCOMM="db2 \"insert into app.sms_send_info (message_content,mobile_num) select '截至凌晨${time}点，BOSS接口抽取完成总数为$extra_num',phone_id from  BASS2.ETL_SEND_MESSAGE where MODULE='LOAD' and phone_id in ('13618999501','13989094821')\""
   echo DB2_SQLCOMM
   DB2_SQL_EXEC
fi


#BOSS接口抽取告警，每天凌晨6点统计接口抽取数

if [ ${time} = "06" ] ; then
   extra_num=count_extra
   DB2_SQLCOMM="db2 \"insert into app.sms_send_info (message_content,mobile_num) select '截至凌晨${time}点，BOSS接口抽取完成总数为$extra_num',phone_id from  BASS2.ETL_SEND_MESSAGE where MODULE='LOAD' and phone_id in ('13618999501','13989094821')\""
   echo DB2_SQLCOMM
   DB2_SQL_EXEC
fi


echo "程序正常结束！"








