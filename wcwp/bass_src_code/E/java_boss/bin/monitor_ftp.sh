#!/bin/sh

#配置信息
DB2_OSS_DB="bassdb48"
DB2_OSS_USER="bass2"
DB2_OSS_PASSWD="0312004131"

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

ftp_running_cnts=`ps -ef|grep -v grep|grep ftp_java_interface.sh|wc -l`


if [ $ftp_running_cnts -eq 0 ] ; then      
      cd /bassdb1/etl/E/boss/java/crm_interface/bin
      nohup ./ftp_java_interface.sh > nohup.out &
      DB2_SQLCOMM="db2 \"insert into APP.SMS_SEND_INFO(MESSAGE_CONTENT,mobile_num) select '43主机接口FTP程序已停止,系统将自动启动!!',phone_id from BASS2.ETL_SEND_MESSAGE where MODULE='LOAD'\""
      echo $DB2_SQLCOMM
      #DB2_SQL_EXEC
      
fi


