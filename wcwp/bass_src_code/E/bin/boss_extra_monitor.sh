#!/bin/ksh

#create by asiainfo LiZhanyong 2009-01-01

#�����ܣ�����սӿڳ�ȡ��������ÿ���賿1�㡢4�㡢6�����ִ��

#��DB2���ݿ���ִ��SQL
DB2_SQL_EXEC()
{
db2 connect to $DB2_OSS_DB user $DB2_OSS_USER using $DB2_OSS_PASSWD
eval $DB2_SQLCOMM
db2 commit
db2 terminate
}

#��ORACLE���ݿ�ִ��SQL
count_extra()
{
sqlplus zk/zk1234@xztestdb <<SQLEND
select count(distinct task_id) extra_num from mb_if_jyfx_task_log where cycle_id=TO_CHAR(SYSDATE,'YYYYMMDD') and task_id not like 'M%';
commit;
SQLEND
}


#��shell
#1�����ݿ�������Ϣ
DB2_OSS_DB="bassdb48"
DB2_OSS_USER="bass2"
DB2_OSS_PASSWD="0312004131"

DB2_OSS_PASSWD=`/bassdb2/etl/L/boss/decode ${DB2_OSS_PASSWD}`
echo ${DB2_OSS_PASSWD}

#Mon Sep 22 21:37:45 CST 2008

echo `date`
time=`date +%H`
echo ${time}


#BOSS�ӿڳ�ȡ�澯��ÿ���賿1��ͳ�ƽӿڳ�ȡ��

if [ ${time} = "02" ] ; then	 
   extra_num=count_extra
   DB2_SQLCOMM="db2 \"insert into app.sms_send_info (message_content,mobile_num) select '�����賿${time}�㣬BOSS�ӿڳ�ȡ�������Ϊ$extra_num',phone_id from  BASS2.ETL_SEND_MESSAGE where MODULE='LOAD' and phone_id in ('13618999501','13989094821')\""
   echo DB2_SQLCOMM
   DB2_SQL_EXEC
fi


#BOSS�ӿڳ�ȡ�澯��ÿ���賿4��ͳ�ƽӿڳ�ȡ��
if [ ${time} = "04" ] ; then
   extra_num=count_extra
   DB2_SQLCOMM="db2 \"insert into app.sms_send_info (message_content,mobile_num) select '�����賿${time}�㣬BOSS�ӿڳ�ȡ�������Ϊ$extra_num',phone_id from  BASS2.ETL_SEND_MESSAGE where MODULE='LOAD' and phone_id in ('13618999501','13989094821')\""
   echo DB2_SQLCOMM
   DB2_SQL_EXEC
fi


#BOSS�ӿڳ�ȡ�澯��ÿ���賿6��ͳ�ƽӿڳ�ȡ��

if [ ${time} = "06" ] ; then
   extra_num=count_extra
   DB2_SQLCOMM="db2 \"insert into app.sms_send_info (message_content,mobile_num) select '�����賿${time}�㣬BOSS�ӿڳ�ȡ�������Ϊ$extra_num',phone_id from  BASS2.ETL_SEND_MESSAGE where MODULE='LOAD' and phone_id in ('13618999501','13989094821')\""
   echo DB2_SQLCOMM
   DB2_SQL_EXEC
fi


echo "��������������"








