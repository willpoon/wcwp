#!/bin/sh
#*************************************************************************************************************
#��������load_boss_monitor.sh
#��  �ܣ����load_boss.sh�����Ƿ����ִ���쳣��������쳣����load_boss.sh����ɱ��
#        �жϵ�������/bassdb2/etl/L/boss/nohup.out �ļ���С�Ƿ��б仯���������ִ��ǰ�������ļ���С�ޱ仯������Ϊ�������
#��  ������
#      
#��д��: lizhanyong 
#��дʱ�� 2008-12-23
#    
#��  ��: ��־�ļ�load_boss_monitor.log
#ע�������shell��crontab����ִ�У�ÿ��20����ִ��һ��
#�޸ı�ע��2009-08-17 by lizhanyong ����shellͷע����Ϣ
#*************************************************************************************************************



#1�����ݿ�������Ϣ
DB2_OSS_DB="bassdb48"
DB2_OSS_USER="bass2"
DB2_OSS_PASSWD="0312004131"


DB2_OSS_PASSWD=`/bassdb2/etl/L/boss/decode ${DB2_OSS_PASSWD}`
#echo $DB2_OSS_PASSWD

#��DB2���ݿ���ִ��SQL
DB2_SQL_EXEC()
{
db2 connect to $DB2_OSS_DB user $DB2_OSS_USER using $DB2_OSS_PASSWD
eval $DB2_SQLCOMM
db2 commit
db2 terminate
}

WORK_PATH=/bassdb2/etl/L/boss
monitor_log_file=${WORK_PATH}/load_boss_monitor.log
cd $WORK_PATH
log_filename="nohup.out"
sql_temp="sql_temp.file"



#����ļ�log_byte.file�����ڣ�������
if [ ! -f log_byte.file ] ; then
				echo "file not exists!" >>$monitor_log_file
	    	touch log_byte.file
fi	

#ȡ��־�ļ���С
log_byte=`ls -trl $log_filename |awk '{print $5}'` 
echo "------------------------------------------------------" >> $monitor_log_file
echo `date` >> $monitor_log_file 
time=`date '+%Y%m%d%H%M%S'`              
echo time=$time >> $monitor_log_file
echo work_path=${WORK_PATH} >> $monitor_log_file
echo log_filename=$log_filename >> $monitor_log_file 

echo "������־�ļ���С�ǣ�" >> $monitor_log_file
echo $log_byte >> $monitor_log_file

echo "�ϴ���־�ļ���С�ǣ�" >> $monitor_log_file
echo `head -1 log_byte.file | awk '{printf $1}'` >> $monitor_log_file
log_byte2=`head -1 log_byte.file | awk '{printf $1}'` 
echo log_byte=$log_byte >> $monitor_log_file
echo log_byte2=$log_byte2 >> $monitor_log_file
#log_byte2=$log_byte

##���ǰ�����ε���־�ļ���С��ȣ���load_boss.sh�������ɱ�����Ժ��س���if_load_start.sh�Ὣ������

	    if [ "${log_byte}" = "${log_byte2}" ] ; then 
	    	
        ##������־�ļ�	    	
	    	cp $log_filename $log_filename$time
	    	cp $sql_temp $sql_temp$time
	    	
        ##��ȡload_boss.sh�Ľ��̺ţ���ɱ������	
        pid=`ps -ef|grep -iw load | grep -iw "load_boss_nfjd" | grep -v grep | awk '{print $2}'`
        for i in $pid
        do
                echo "kill -9 $i"
                kill -9 $i
        done    	
    	   	
 	    	echo "`date`��load_boss.sh����ִ���쳣��ϵͳɱ�����̣��Ժ�ϵͳ���Զ�������" >> $monitor_log_file 
	    	
        ##���Ÿ澯	    	
	    	DB2_SQLCOMM="db2 \"insert into APP.SMS_SEND_INFO(MESSAGE_CONTENT,mobile_num) select 'load_boss_nfjd.sh����ִ���쳣��ϵͳɱ�����̣��Ժ�ϵͳ���Զ���������ע��۲�!', phone_id from BASS2.ETL_SEND_MESSAGE where MODULE='LOAD' and phone_id in ('13618999501','13908918072','13989094821','13989991420')\""
	      echo $DB2_SQLCOMM >> $monitor_log_file
        DB2_SQL_EXEC >> $monitor_log_file
        
        ##��¼��־�ļ���С	    	
	      echo "`date`������־�ļ���С��¼���ļ�log_byte.file�У�" >> $monitor_log_file
	      echo ${log_byte} > $WORK_PATH/log_byte.file

##���ǰ�����ε���־�ļ���С����ȣ��򽫺�һ�ε���־�ļ���С��¼���ļ�log_byte.file��	    	
	    else 
	       echo "`date`��load_boss.sh����ִ��������" >> $monitor_log_file
	       echo "`date`����������־�ļ���С��¼���ļ�log_byte.file�У�" >> $monitor_log_file
	       echo ${log_byte} > $WORK_PATH/log_byte.file
	    fi  
	    
echo "-------------------------------------------------------" >> $monitor_log_file
exit

