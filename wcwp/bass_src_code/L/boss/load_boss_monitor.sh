#!/bin/sh
#*************************************************************************************************************
#函数名：load_boss_monitor.sh
#功  能：监控load_boss.sh程序是否出现执行异常，如果有异常，则将load_boss.sh进程杀掉
#        判断的依据是/bassdb2/etl/L/boss/nohup.out 文件大小是否有变化，如果程序执行前后两次文件大小无变化，则认为程序假死
#参  数：无
#      
#编写人: lizhanyong 
#编写时间 2008-12-23
#    
#输  出: 日志文件load_boss_monitor.log
#注意事项：此shell由crontab调用执行，每隔20分钟执行一次
#修改备注：2009-08-17 by lizhanyong 增加shell头注释信息
#*************************************************************************************************************



#1、数据库连接信息
DB2_OSS_DB="bassdb48"
DB2_OSS_USER="bass2"
DB2_OSS_PASSWD="0312004131"


DB2_OSS_PASSWD=`/bassdb2/etl/L/boss/decode ${DB2_OSS_PASSWD}`
#echo $DB2_OSS_PASSWD

#在DB2数据库中执行SQL
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



#如果文件log_byte.file不存在，则生成
if [ ! -f log_byte.file ] ; then
				echo "file not exists!" >>$monitor_log_file
	    	touch log_byte.file
fi	

#取日志文件大小
log_byte=`ls -trl $log_filename |awk '{print $5}'` 
echo "------------------------------------------------------" >> $monitor_log_file
echo `date` >> $monitor_log_file 
time=`date '+%Y%m%d%H%M%S'`              
echo time=$time >> $monitor_log_file
echo work_path=${WORK_PATH} >> $monitor_log_file
echo log_filename=$log_filename >> $monitor_log_file 

echo "本次日志文件大小是：" >> $monitor_log_file
echo $log_byte >> $monitor_log_file

echo "上次日志文件大小是：" >> $monitor_log_file
echo `head -1 log_byte.file | awk '{printf $1}'` >> $monitor_log_file
log_byte2=`head -1 log_byte.file | awk '{printf $1}'` 
echo log_byte=$log_byte >> $monitor_log_file
echo log_byte2=$log_byte2 >> $monitor_log_file
#log_byte2=$log_byte

##如果前后两次的日志文件大小相等，则将load_boss.sh程序进程杀掉，稍后监控程序if_load_start.sh会将其启动

	    if [ "${log_byte}" = "${log_byte2}" ] ; then 
	    	
        ##备份日志文件	    	
	    	cp $log_filename $log_filename$time
	    	cp $sql_temp $sql_temp$time
	    	
        ##获取load_boss.sh的进程号，并杀掉进程	
        pid=`ps -ef|grep -iw load | grep -iw "load_boss_nfjd" | grep -v grep | awk '{print $2}'`
        for i in $pid
        do
                echo "kill -9 $i"
                kill -9 $i
        done    	
    	   	
 	    	echo "`date`，load_boss.sh程序执行异常，系统杀掉进程，稍后系统会自动启动。" >> $monitor_log_file 
	    	
        ##短信告警	    	
	    	DB2_SQLCOMM="db2 \"insert into APP.SMS_SEND_INFO(MESSAGE_CONTENT,mobile_num) select 'load_boss_nfjd.sh程序执行异常，系统杀掉进程，稍后系统会自动启动，请注意观察!', phone_id from BASS2.ETL_SEND_MESSAGE where MODULE='LOAD' and phone_id in ('13618999501','13908918072','13989094821','13989991420')\""
	      echo $DB2_SQLCOMM >> $monitor_log_file
        DB2_SQL_EXEC >> $monitor_log_file
        
        ##记录日志文件大小	    	
	      echo "`date`，将日志文件大小记录到文件log_byte.file中！" >> $monitor_log_file
	      echo ${log_byte} > $WORK_PATH/log_byte.file

##如果前后两次的日志文件大小不相等，则将后一次的日志文件大小记录到文件log_byte.file中	    	
	    else 
	       echo "`date`，load_boss.sh程序执行正常！" >> $monitor_log_file
	       echo "`date`，将本次日志文件大小记录到文件log_byte.file中！" >> $monitor_log_file
	       echo ${log_byte} > $WORK_PATH/log_byte.file
	    fi  
	    
echo "-------------------------------------------------------" >> $monitor_log_file
exit

