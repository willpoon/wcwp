#!/bin/sh

#配置信息
DB2_OSS_DB="bassdb"
DB2_OSS_USER="bass2"
DB2_OSS_PASSWD="0312004131"
cdr_dir=/bassdb1/etl/E/xfer/data

DB2_OSS_PASSWD=`/bassdb1/etl/L/boss/decode ${DB2_OSS_PASSWD}`
echo ${DB2_OSS_PASSWD}

#在DB2数据库中执行SQL
DB2_SQL_EXEC()
{
db2 terminate;db2 connect to $DB2_OSS_DB user $DB2_OSS_USER using $DB2_OSS_PASSWD
eval $DB2_SQLCOMM
db2 commit
db2 connect reset
}

today=`date '+%Y%m%d'`
#today=20100121


cdr_sms=DR_SMS_L_${today}
cdr_call=DR_GSM_[GP,S]_${today}
#ls -C1 | awk '$0 ~ /DR_GSM_[A-Z][A-Z]*_${today}/'

cd ${cdr_dir}

cdr_sms_num=`ls -l ${cdr_sms}*|wc -l`
cdr_call_num=`ls -C1 | grep "^DR_GSM_[A-Z]\{1,2\}_${today}" | wc -l`
#cdr_call_num=`ls -l ${cdr_call}|wc -l`
cdr_sms_record_num=`cat ${cdr_sms}* | wc -l`
#cdr_call_record_num=`cat ${cdr_call}* | wc -l`
cdr_call_record_num=`ls -C1 | grep "^DR_GSM_[A-Z]\{1,2\}_${today}" | xargs cat | wc -l`

cdr_sms_record_num=`expr ${cdr_sms_record_num} / 10000`
cdr_call_record_num=`expr ${cdr_call_record_num} / 10000`

echo ${cdr_sms_num}
echo ${cdr_call_num}
echo ${cdr_sms_record_num}
echo ${cdr_call_record_num}



DB2_SQLCOMM="db2 \"insert into APP.SMS_SEND_INFO(MESSAGE_CONTENT,mobile_num) select '${today}短信清单文件总数:${cdr_sms_num} 记录条数:${cdr_sms_record_num}万,语音清单总数:${cdr_call_num} 记录条数:${cdr_call_record_num}万!',phone_id from BASS2.ETL_SEND_MESSAGE where MODULE='LOAD'\""
DB2_SQL_EXEC > /dev/null 
	