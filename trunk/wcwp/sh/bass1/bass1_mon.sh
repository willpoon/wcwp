#
#1.表空间
#2.日接口
#3.月接口
#4.数据质量
#5.数据导出与库表一致
#
#
#在DB2数据库中执行SQL
DB2_SQL_EXEC()
{
DB2_OSS_DB="bassdb"
DB2_OSS_USER="bass2"
DB2_OSS_PASSWD="bass2"
#echo ${DB2_OSS_PASSWD}	
    db2 terminate;db2 connect to $DB2_OSS_DB user $DB2_OSS_USER using $DB2_OSS_PASSWD
    eval $DB2_SQLCOMM 
		if [ $? -ne 0 ];then 
		echo "error occurs when running DB2_SQLCOMM!"
		return 1
		fi
    db2 commit 
    db2 connect reset 
    return 0
}

sendalarmsms(){
	if [ $# -ne 1 ];then 
	echo sendalarmsms msg
	exit
	fi
	MESSAGE_CONTENT=$1
	sql_str="insert into APP.SMS_SEND_INFO(MESSAGE_CONTENT,MOBILE_NUM) \
	select  ('${MESSAGE_CONTENT}'),MOBILE_NUM from bass1.mon_user_mobile
	"
	DB2_SQLCOMM="db2 \"${sql_str}\""
	#echo ${DB2_SQLCOMM}
	DB2_SQL_EXEC > /dev/null
	
  if [ $? -ne 0 ];then 
	echo "error occurs while generating alert msg!"
	return 1
	fi	
	return 0
}

# sendalarmsms "今日记录级返回完全!"


#求当日文件级返回数
fn_d_file_lvl_ret_cnt(){
	sql_str=" select  'xxxxx',count(0)\
						from \
						( \
						select  a.* ,row_number()over(partition by  substr(filename,18,5) order by deal_time desc ) rn \
						from APP.G_FILE_REPORT a \
						where substr(filename,9,8) = replace(char(current date - 1 days),'-','') and err_code='00' \
						) t where rn = 1
					 "
	DB2_SQLCOMM="db2 \"${sql_str}\""
	#echo ${DB2_SQLCOMM}
	filelvl_ret_cnt=`DB2_SQL_EXEC|grep 'xxxxx'|awk '{print $2}'`	
  if [ $? -ne 0 ];then 
	echo "error occurs while getting record_ret_cnt ! "
	return 1
	fi	
	echo "d_file_lvl_ret_cnt:${filelvl_ret_cnt}"
	return ${filelvl_ret_cnt}
	}


#求当日文件级返回数 (9点接口)
fn_d9_file_lvl_ret_cnt(){
	sql_str=" select  'xxxxx',count(0)\
						from \
						( \
						select  a.* ,row_number()over(partition by  substr(filename,18,5) order by deal_time desc ) rn \
						from APP.G_FILE_REPORT a \
						where substr(filename,9,8) = replace(char(current date - 1 days),'-','') \
						and err_code='00' \
						and substr(filename,18,5) \
						in (  select INTERFACE_CODE from   BASS1.MON_ALL_INTERFACE \
								   where upload_time = '每日9点前' \
								) \
						) t where rn = 1
					 "
	DB2_SQLCOMM="db2 \"${sql_str}\""
	#echo ${DB2_SQLCOMM}
	filelvl_ret_cnt=`DB2_SQL_EXEC|grep 'xxxxx'|awk '{print $2}'`	
  if [ $? -ne 0 ];then 
	echo "fn_d9_file_lvl_ret_cnt:error occurs while getting record_ret_cnt ! "
	return 1
	fi	
	echo "d_file_lvl_ret_cnt:${filelvl_ret_cnt}"
	return ${filelvl_ret_cnt}
	}


#求当日文件级返回数 (11点接口)
fn_d11_file_lvl_ret_cnt(){
	sql_str=" select  'xxxxx',count(0)\
						from \
						( \
						select  a.* ,row_number()over(partition by  substr(filename,18,5) order by deal_time desc ) rn \
						from APP.G_FILE_REPORT a \
						where substr(filename,9,8) = replace(char(current date - 1 days),'-','') \
						and err_code='00' \
						and substr(filename,18,5) \
						in (  select INTERFACE_CODE from   BASS1.MON_ALL_INTERFACE \
								   where upload_time = '每日11点前' \
								) \
						) t where rn = 1
					 "
	DB2_SQLCOMM="db2 \"${sql_str}\""
	#echo ${DB2_SQLCOMM}
	filelvl_ret_cnt=`DB2_SQL_EXEC|grep 'xxxxx'|awk '{print $2}'`	
  if [ $? -ne 0 ];then 
	echo "fn_d11_file_lvl_ret_cnt:error occurs while getting record_ret_cnt ! "
	return 1
	fi	
	echo "d_file_lvl_ret_cnt:${filelvl_ret_cnt}"
	return ${filelvl_ret_cnt}
	}



#求当日文件级返回数 (13点接口)
fn_d13_file_lvl_ret_cnt(){
	sql_str=" select  'xxxxx',count(0)\
						from \
						( \
						select  a.* ,row_number()over(partition by  substr(filename,18,5) order by deal_time desc ) rn \
						from APP.G_FILE_REPORT a \
						where substr(filename,9,8) = replace(char(current date - 1 days),'-','') \
						and err_code='00' \
						and substr(filename,18,5) \
						in (  select INTERFACE_CODE from   BASS1.MON_ALL_INTERFACE \
								   where upload_time = '每日13点前' \
								) \
						) t where rn = 1
					 "
	DB2_SQLCOMM="db2 \"${sql_str}\""
	#echo ${DB2_SQLCOMM}
	filelvl_ret_cnt=`DB2_SQL_EXEC|grep 'xxxxx'|awk '{print $2}'`	
  if [ $? -ne 0 ];then 
	echo "fn_d13_file_lvl_ret_cnt:error occurs while getting record_ret_cnt ! "
	return 1
	fi	
	echo "d_file_lvl_ret_cnt:${filelvl_ret_cnt}"
	return ${filelvl_ret_cnt}
	}




#求当日文件级返回数 (15点接口)
fn_d15_file_lvl_ret_cnt(){
	sql_str=" select  'xxxxx',count(0)\
						from \
						( \
						select  a.* ,row_number()over(partition by  substr(filename,18,5) order by deal_time desc ) rn \
						from APP.G_FILE_REPORT a \
						where substr(filename,9,8) = replace(char(current date - 1 days),'-','') \
						and err_code='00' \
						and substr(filename,18,5) \
						in (  select INTERFACE_CODE from   BASS1.MON_ALL_INTERFACE \
								   where upload_time = '每日15点前' \
								) \
						) t where rn = 1
					 "
	DB2_SQLCOMM="db2 \"${sql_str}\""
	#echo ${DB2_SQLCOMM}
	filelvl_ret_cnt=`DB2_SQL_EXEC|grep 'xxxxx'|awk '{print $2}'`	
  if [ $? -ne 0 ];then 
	echo "fn_d15_file_lvl_ret_cnt:error occurs while getting record_ret_cnt ! "
	return 1
	fi	
	echo "d_file_lvl_ret_cnt:${filelvl_ret_cnt}"
	return ${filelvl_ret_cnt}
	}




#求当日记录级返回数
fn_d_recordlvl_ret_cnt(){
	sql_str="select 'xxxxx',count(0) from app.g_runlog \
					 where time_id=int(replace(char(current date - 1 days),'-','')) \
					 and return_flag=1
					 "
	DB2_SQLCOMM="db2 \"${sql_str}\""
	#echo ${DB2_SQLCOMM}
	reclvl_ret_cnt=`DB2_SQL_EXEC|grep 'xxxxx'|awk '{print $2}'`	
  if [ $? -ne 0 ];then 
	echo "error occurs while getting record_ret_cnt ! "
	return 1
	fi	
	return ${reclvl_ret_cnt}
	}


################################################################
##main
##数据监控：提醒类
################################################################
echo $$
while [ true ]
do
#设置退出条件
if [ -f ./stop_bass1_mon ];then 
echo "$0 normal exit"
echo $$
exit
fi
#对部分提醒类短信，一天只发一次，故要发送一次后，要设置标志位，标记短信已发送1。在每天06：00重置为未发送0.
			#初始化为1，已发送，以防不停地发!
			g_d_recordlvl_sent_flag=1
			alert_time=`date +%H`
			if [ ${alert_time} = "07" ];then 			
				g_d_recordlvl_sent_flag=0
			fi

#1.日常通知类：每天一条，根据记录级返回时间，不定时触发。(但应在传日数据之后，即08：00后)
			#1.1日数据：监控记录级返回，当全部返回时触发。
			echo 1.1
			alert_time=`date +%H`
			if [ ${alert_time} -ge "08" ];then 			
				fn_d_recordlvl_ret_cnt
				v_d_recordlvl_ret_cnt=$?
				echo ${v_d_recordlvl_ret_cnt}
				if [ ${v_d_recordlvl_ret_cnt} -eq 56 -a ${g_d_recordlvl_sent_flag} -eq 0 ]	; then 
					MESSAGE_CONTENT="日接口(day)记录级校验已全部正常返回!!"
					sendalarmsms ${MESSAGE_CONTENT}
					g_d_recordlvl_sent_flag=1
				fi
			fi

#2.日常提醒类：当日常传数计划被打乱，没有按时进行时触发。定时触发，在特定的一小时内触发，该时间内最多5条。
			
			#2.1 提示上传日数据：为了防止遗忘，如果还没传数，发短信提醒！
			#每天8：00发
			echo 2.1
			alert_time=`date +%H`
			if [ ${alert_time} = "08" ];then 
			
			fn_d_file_lvl_ret_cnt      
			v_d_filelvl_ret_cnt=$?     
			echo ${v_d_filelvl_ret_cnt}
			
				if [ ${v_d_filelvl_ret_cnt} -eq 0 ]	;then 
					MESSAGE_CONTENT="今日接口尚未上传！请抓紧上传！"
					sendalarmsms ${MESSAGE_CONTENT}
				fi
			fi
			
			
			#2.2提示处理9点前接口：监控9点接口上传情况,如果还没传数，发短信提醒！
			#每天08：00发
			echo 2.2		
			alert_time=`date +%H`
			if [ ${alert_time} = "08" ];then 
			fn_d9_file_lvl_ret_cnt
			v_d_filelvl_ret_cnt=$?
			echo ${v_d_filelvl_ret_cnt}
				if [ ${v_d_filelvl_ret_cnt} -ne 8 ]	;then 
					MESSAGE_CONTENT="9点前接口不等于8个，请在9点前处理!"
					sendalarmsms ${MESSAGE_CONTENT}
				fi
			fi
			
			#2.3提示处理11点前接口：监控11点接口上传情况,如果还没传数，发短信提醒！
			#每天10：00发
			echo 2.3			
			alert_time=`date +%H`
			if [ ${alert_time} = "10" ];then 
			fn_d11_file_lvl_ret_cnt
			v_d_filelvl_ret_cnt=$?
			echo ${v_d_filelvl_ret_cnt}
				if [ ${v_d_filelvl_ret_cnt} -ne 30 ]	;then 
					MESSAGE_CONTENT="9点前接口不等于30个，请在11点前处理!"
					sendalarmsms ${MESSAGE_CONTENT}
				fi
			fi
			
			
			#2.4提示处理13点前接口:监控13点接口上传情况,如果还没传数，发短信提醒！
			#每天12：00发
			echo 2.4			
			alert_time=`date +%H`
			if [ ${alert_time} = "12" ];then 
			fn_d13_file_lvl_ret_cnt
			v_d_filelvl_ret_cnt=$?
			echo ${v_d_filelvl_ret_cnt}
				if [ ${v_d_filelvl_ret_cnt} -ne 12 ]	;then 
					MESSAGE_CONTENT="13点前接口不等于12个，请在13点前处理!"
					sendalarmsms ${MESSAGE_CONTENT}
				fi
			fi
			
			
			#2.5提示处理15点前接口:监控15点接口上传情况,如果还没传数，发短信提醒！
			#每天14：00发
			echo 2.5			
			alert_time=`date +%H`
			if [ ${alert_time} = "14" ];then 
			fn_d15_file_lvl_ret_cnt
			v_d_filelvl_ret_cnt=$?
			echo ${v_d_filelvl_ret_cnt}
				if [ ${v_d_filelvl_ret_cnt} -ne 6 ]	;then 
					MESSAGE_CONTENT="15点前接口不等于6个，请在15点前处理!"
					sendalarmsms ${MESSAGE_CONTENT}
				fi
			fi
			


################################################################
##月数据监控
################################################################



################################################################
##表空间监控
################################################################
sleep 600
done

exit