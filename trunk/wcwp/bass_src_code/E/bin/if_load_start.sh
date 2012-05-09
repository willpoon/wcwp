#!/bin/sh

#配置信息
DB2_OSS_DB="bassdb48"
DB2_OSS_USER="bass2"
DB2_OSS_PASSWD="0312004131"

DB2_OSS_PASSWD=`/bassdb2/etl/L/boss/decode ${DB2_OSS_PASSWD}`
echo ${DB2_OSS_PASSWD}

#在DB2数据库中执行SQL
DB2_SQL_EXEC()
{
db2 terminate;db2 connect to $DB2_OSS_DB user $DB2_OSS_USER using $DB2_OSS_PASSWD
eval $DB2_SQLCOMM
db2 commit
db2 connect reset
}


if_load_boss=`ps -ef|grep -v grep|grep load_boss.sh|wc -l`
if_load_dsmp=`ps -ef|grep -v grep|grep load_dsmp.sh|wc -l`
if_load_nm=`ps -ef|grep -v grep|grep load_nm.sh|wc -l`
if_load_mr=`ps -ef|grep -v grep|grep load_mr.sh|wc -l`
if_load_ca=`ps -ef|grep -v grep|grep load_ca.sh|wc -l`
if_load_ota=`ps -ef|grep -v grep|grep load_ota.sh|wc -l`

if [ $if_load_boss -eq 0 ] ; then
      DB2_SQLCOMM="db2 \"insert into APP.SMS_SEND_INFO(MESSAGE_CONTENT,mobile_num) select 'BOSS数据加载程序未启动,请及时处理!',phone_id from BASS2.ETL_SEND_MESSAGE where MODULE='LOAD'\""
      DB2_SQL_EXEC
fi

if [ $if_load_dsmp -eq  0 ] ; then
      DB2_SQLCOMM="db2 \"insert into APP.SMS_SEND_INFO(MESSAGE_CONTENT,mobile_num) select 'DSMP数据加载程序未启动,请及时处理!',phone_id from BASS2.ETL_SEND_MESSAGE where MODULE='LOAD'\""
      DB2_SQL_EXEC
fi

if [ $if_load_nm -eq  0 ] ; then
      DB2_SQLCOMM="db2 \"insert into APP.SMS_SEND_INFO(MESSAGE_CONTENT,mobile_num) select '网管数据加载程序未启动,请及时处理!',phone_id from BASS2.ETL_SEND_MESSAGE where MODULE='LOAD'\""
      DB2_SQL_EXEC
fi

if [ $if_load_mr -eq  0 ] ; then
      DB2_SQLCOMM="db2 \"insert into APP.SMS_SEND_INFO(MESSAGE_CONTENT,mobile_num) select '彩铃数据加载程序未启动,请及时处理!',phone_id from BASS2.ETL_SEND_MESSAGE where MODULE='LOAD'\""
      DB2_SQL_EXEC
fi

if [ $if_load_ca -eq  0 ] ; then
      DB2_SQLCOMM="db2 \"insert into APP.SMS_SEND_INFO(MESSAGE_CONTENT,mobile_num) select '飞信数据加载程序未启动,请及时处理!',phone_id from BASS2.ETL_SEND_MESSAGE where MODULE='LOAD'\""
      DB2_SQL_EXEC 
fi

if [ $if_load_ota -eq  0 ] ; then
      DB2_SQLCOMM="db2 \"insert into APP.SMS_SEND_INFO(MESSAGE_CONTENT,mobile_num) select 'OTA加载程序未启动,请及时处理!',phone_id from BASS2.ETL_SEND_MESSAGE where MODULE='LOAD'\""
      DB2_SQL_EXEC 
fi

