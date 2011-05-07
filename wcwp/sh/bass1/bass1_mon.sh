##############################################################
#author : panzhiwei
#功能：一经日常监控
#r1 : 记录级返回时间监控（只提示一次）
#r2 : 各个时间点接口上传监控、上传提醒。如未处理指定小时内10分钟提醒一次。
#r3 : 接口文件导出监控（3-6点每10分钟扫描一次，有异常即触发告警）
#r4 : 增加监控：凌晨4：00若接口导出为0，告警。
#r5 :	2011-05-06 19:13:37调整统计顺序：先wc -l 统计文件数,再查数据库exp程序完成数。 
#r6 :	2011-05-06 19:13:40只检查verf file,不检查dat file
##############################################################

yesterday()
{
	#usage:yesterday yyyymmdd
        year=`echo "$1"|cut -c1-4`
        month=`echo "$1"|cut -c5-6`
        day=`echo "$1"|cut -c7-8`

        month=`expr $month + 0`
        day=`expr $day - 1`

        if [ $day -eq 0 ]; then
                month=`expr $month - 1`
                if [ $month -eq 0 ]; then
                        month=12
                        day=31
                        year=`expr $year - 1`
                else
                        case $month in
                                1|3|5|7|8|10|12) day=31;;
                                4|6|9|11) day=30;;
                                2)
                                        if [ `expr $year % 4` -eq 0 ]; then
                                                if [ `expr $year % 400` -eq 0 ]; then
                                                        day=29
                                                elif [ `expr $year % 100` -eq 0 ]; then
                                                        day=28
                                                else
                                                        day=29
                                                fi
                                        else
                                                day=28
                                        fi ;;
                        esac
                fi
        fi

        if [ $month -lt 10 ] ; then
                month=`echo "0$month"`
        fi

        if [ $day -lt 10 ] ; then
                day=`echo "0$day"`
        fi
        echo $year$month$day
        return 1
}

#在DB2数据库中执行SQL
DB2_SQL_EXEC()
{
DB2_OSS_DB="bassdb"
DB2_OSS_USER="bass2"
DB2_OSS_PASSWD="bass2"
    db2 terminate;db2 connect to $DB2_OSS_DB user $DB2_OSS_USER using $DB2_OSS_PASSWD
    eval $DB2_SQLCOMM 
		if [ $? -ne 0 ];then 
		echo "error occurs when running DB2_SQLCOMM!"
		db2 connect reset 
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
	DB2_SQL_EXEC > /dev/null
	
  if [ $? -ne 0 ];then 
	echo "error occurs while generating alert msg!"
	return 1
	fi	
	return 0
}



#求当日文件级返回数
fn_d_file_lvl_ret_cnt(){
	sql_str=" select  'xxxxx',count(0)\
						from \
						( \
						select  a.* ,row_number()over(partition by  substr(filename,18,5) order by deal_time desc ) rn \
						from APP.G_FILE_REPORT a \
						where substr(filename,9,8) = replace(char(current date - 1 days),'-','') and err_code='00' \
						) t where rn = 1 with ur
					 "
	DB2_SQLCOMM="db2 \"${sql_str}\""
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
						) t where rn = 1 with ur 
					 "
	DB2_SQLCOMM="db2 \"${sql_str}\""
	filelvl_ret_cnt=`DB2_SQL_EXEC|grep 'xxxxx'|awk '{print $2}'`	
  if [ $? -ne 0 ];then 
	echo "fn_d9_file_lvl_ret_cnt:error occurs while getting record_ret_cnt ! "
	return 1
	fi	
	echo "d9_file_lvl_ret_cnt:${filelvl_ret_cnt}"
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
						) t where rn = 1 with ur 
					 "
	DB2_SQLCOMM="db2 \"${sql_str}\""
	#echo ${DB2_SQLCOMM}
	filelvl_ret_cnt=`DB2_SQL_EXEC|grep 'xxxxx'|awk '{print $2}'`	
  if [ $? -ne 0 ];then 
	echo "fn_d11_file_lvl_ret_cnt:error occurs while getting record_ret_cnt ! "
	return 1
	fi	
	echo "d11_file_lvl_ret_cnt:${filelvl_ret_cnt}"
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
						) t where rn = 1 with ur 
					 "
	DB2_SQLCOMM="db2 \"${sql_str}\""
	filelvl_ret_cnt=`DB2_SQL_EXEC|grep 'xxxxx'|awk '{print $2}'`	
  if [ $? -ne 0 ];then 
	echo "fn_d13_file_lvl_ret_cnt:error occurs while getting record_ret_cnt ! "
	return 1
	fi	
	echo "d13_file_lvl_ret_cnt:${filelvl_ret_cnt}"
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
						) t where rn = 1 with ur 
					 "
	DB2_SQLCOMM="db2 \"${sql_str}\""
	filelvl_ret_cnt=`DB2_SQL_EXEC|grep 'xxxxx'|awk '{print $2}'`	
  if [ $? -ne 0 ];then 
	echo "fn_d15_file_lvl_ret_cnt:error occurs while getting record_ret_cnt ! "
	return 1
	fi	
	echo "d15_file_lvl_ret_cnt:${filelvl_ret_cnt}"
	return ${filelvl_ret_cnt}
	}




#求当日记录级返回数
fn_d_recordlvl_ret_cnt(){
	sql_str="select 'xxxxx',count(0) from app.g_runlog \
					 where time_id=int(replace(char(current date - 1 days),'-','')) \
					 and return_flag=1 with ur 
					 "
	DB2_SQLCOMM="db2 \"${sql_str}\""
	reclvl_ret_cnt=`DB2_SQL_EXEC|grep 'xxxxx'|awk '{print $2}'`	
  if [ $? -ne 0 ];then 
	echo "error occurs while getting record_ret_cnt ! "
	return 1
	fi	
	return ${reclvl_ret_cnt}
	}


getunixtime2(){
	#sun os date 不能求 unixtimestamp , 唯有这样来取了！有一定的绝对误差。
		if [ $# -ne 0 ];then
		echo "getmtime"
		exit
		fi
		touchfile="."
		touch $touchfile
		in_file=$touchfile
		if [ ! -d ${in_file} ];then 
		echo "getmtime 所操作的文件 ${in_file} 不存在!!"
		return 1
		fi
		tcltimestamp=`echo "puts [file mtime ${in_file}]"|tclsh|awk -F'\r' '{print $1}'`
		echo  $tcltimestamp
		return 0
}

################################################################
##main
##数据监控：提醒类
################################################################
echo $$
##define global var
d_at09_cnt=8
d_at11_cnt=30
d_at13_cnt=17
d_at15_cnt=6
d_all_cnt=61
#
m_on03_cnt=12
m_on05_cnt=15
m_on08_cnt=30
m_on10_cnt=17
m_on15_cnt=10
m_all_cnt=84





##

start_run_time=`getunixtime2`
while [ true ]
do
	#设置退出条件
	if [ -f ./stop_bass1_mon ];then 
	echo "$0 normal exit"
	echo $$
	rm ./stop_bass1_mon
	exit
	fi
	#日志时间
	echo `date +%Y%m%d%H%M%S`	
	#对部分提醒类短信，一天只发一次，故要发送一次后，要设置标志位，标记短信已发送1。在每天06：00重置为未发送0.
			#初始化为1，已发送，以防不停地发!

			now_alert_time=`getunixtime2`
			diffs=`expr $now_alert_time - $start_run_time`
			#一次性初始化g_d_recordlvl_sent_flag,防止重复置0,将后面修改为1的状态覆盖了。
			if [ $diffs -lt 60 ];then 
			g_d_recordlvl_sent_flag=0
			fi
			#g_d_recordlvl_sent_flag：
			#如果记录级全部返回，应为1
			#如果未全部返回，应为0
			#次日在未全部返回时，应为0，全部返回，应为1
			#根据惯例，每天7点才刚开始传数据，不能马上全部返回，故可以在07点将g_d_recordlvl_sent_flag清零
			#假设在09点全部返回，那么在当日09-次日07这段时间，g_d_recordlvl_sent_flag=1.
			#在07-09期间，g_d_recordlvl_sent_flag = 0，处于等待发送状态。
			#07点g_d_recordlvl_sent_flag清零
			alert_time=`date +%H`
			if [ ${alert_time} = "07" -a ${g_d_recordlvl_sent_flag} -eq 1 ];then 			
				g_d_recordlvl_sent_flag=0
			fi
			echo ${g_d_recordlvl_sent_flag}
#1.日常通知类：每天一条，根据记录级返回时间，不定时触发。(但应在传日数据之后，即08：00后)
			#1.1日数据：监控记录级返回，当全部返回时触发。
			#echo 1.1
			alert_time=`date +%H`
			#加入${g_d_recordlvl_sent_flag} -eq 0 ，双重判断，减少扫表次数(从16*5次减少到1*5次)
			if [ ${alert_time} -ge "08" -a ${g_d_recordlvl_sent_flag} -eq 0 ];then 			
				fn_d_recordlvl_ret_cnt
				v_d_recordlvl_ret_cnt=$?
				echo ${v_d_recordlvl_ret_cnt}
				if [ ${v_d_recordlvl_ret_cnt} -eq ${d_all_cnt} -a ${g_d_recordlvl_sent_flag} -eq 0 ]	; then 
					MESSAGE_CONTENT="日接口(day)记录级校验已全部正常返回."
					sendalarmsms "${MESSAGE_CONTENT}"
					#将发送标记置为已发送！
					g_d_recordlvl_sent_flag=1
				fi
			fi

#2.日常提醒类：当日常传数计划被打乱，没有按时进行时触发。定时触发，在特定的一小时内触发，该时间内最多5条。
			
			#2.1 提示上传日数据：为了防止遗忘，如果还没传数，发短信提醒！
			#每天8：00发
			#echo 2.1
			alert_time=`date +%H`
			if [ ${alert_time} = "08" ];then 
			
				fn_d_file_lvl_ret_cnt      
				v_d_filelvl_ret_cnt=$?     
				echo ${v_d_filelvl_ret_cnt}
			
				if [ ${v_d_filelvl_ret_cnt} -eq 0 ]	;then 
					MESSAGE_CONTENT="今日接口尚未上传！请抓紧上传！"
					sendalarmsms "${MESSAGE_CONTENT}"
				fi
			fi
			
			
			#2.2提示处理9点前接口：监控9点接口上传情况,如果还没传数，发短信提醒！
			#每天08：00发
			#echo 2.2		
			alert_time=`date +%H`
			if [ ${alert_time} = "08" ];then 
				fn_d9_file_lvl_ret_cnt
				v_d_filelvl_ret_cnt=$?
				echo ${v_d_filelvl_ret_cnt}
				if [ ${v_d_filelvl_ret_cnt} -ne ${d_at09_cnt} ]	;then 
					MESSAGE_CONTENT="9点前接口文件级返回不等于${d_at09_cnt}个，请在9点前处理，确认是否已上传!"
					sendalarmsms "${MESSAGE_CONTENT}"
				fi
			fi
			
			#2.3提示处理11点前接口：监控11点接口上传情况,如果还没传数，发短信提醒！
			#每天10：00发
			#echo 2.3			
			alert_time=`date +%H`
			if [ ${alert_time} = "10" ];then 
				fn_d11_file_lvl_ret_cnt
				v_d_filelvl_ret_cnt=$?
				echo ${v_d_filelvl_ret_cnt}
				if [ ${v_d_filelvl_ret_cnt} -ne ${d_at11_cnt} ]	;then 
					MESSAGE_CONTENT="9点前接口文件级返回不等于${d_at11_cnt}个，请在11点前处理，确认是否已上传!"
					sendalarmsms "${MESSAGE_CONTENT}"
				fi
			fi
			
			
			#2.4提示处理13点前接口:监控13点接口上传情况,如果还没传数，发短信提醒！
			#每天12：00发
			#echo 2.4			
			alert_time=`date +%H`
			if [ ${alert_time} = "12" ];then 
				fn_d13_file_lvl_ret_cnt
				v_d_filelvl_ret_cnt=$?
				echo ${v_d_filelvl_ret_cnt}
				if [ ${v_d_filelvl_ret_cnt} -ne ${d_at13_cnt} ]	;then 
					MESSAGE_CONTENT="13点前接口文件级返回不等于${d_at13_cnt}个，请在13点前处理，确认是否已上传!"
					sendalarmsms "${MESSAGE_CONTENT}"
				fi
			fi
			
			
			#2.5提示处理15点前接口:监控15点接口上传情况,如果还没传数，发短信提醒！
			#每天14：00发
			#echo 2.5			
			alert_time=`date +%H`
			if [ ${alert_time} = "14" ];then 
			fn_d15_file_lvl_ret_cnt
			v_d_filelvl_ret_cnt=$?
			echo ${v_d_filelvl_ret_cnt}
				if [ ${v_d_filelvl_ret_cnt} -ne ${d_at15_cnt} ]	;then 
					MESSAGE_CONTENT="15点前接口文件级返回不等于${d_at15_cnt}个，请在15点前处理，确认是否已上传!"
					sendalarmsms "${MESSAGE_CONTENT}"
				fi
			fi
			


################################################################
##导出监控
################################################################
			alert_time=`date +%H`
			if [ ${alert_time} = "03" -o ${alert_time} = "04" -o ${alert_time} = "05" -o ${alert_time} = "06" -o ${alert_time} = "07" ];then 
					sql_str="select 'xxxxx', count(0) from   app.sch_control_runlog  a \
									where a.control_code like 'BASS1%EXP%DAY%' \
									and date(a.begintime) =  date(current date) \
									and flag = 0 with ur 
					"
					DB2_SQLCOMM="db2 \"${sql_str}\""	
					today=`date '+%Y%m%d'`
					deal_date=`yesterday ${today}`
					exp_dir="/bassapp/backapp/data/bass1/export/export_${deal_date}"
					#dat_file_cnt=`ls -lrt ${exp_dir}/*.dat | wc -l|awk '{print $1}'`
					#echo "dat_file_cnt数据文件数 :${dat_file_cnt}"									
				  verf_file_cnt=`ls -lrt ${exp_dir}/*.verf | wc -l|awk '{print $1}'`					
				  echo "verf_file_cnt 校验文件数 :${verf_file_cnt}"													
					exp_cnt=`DB2_SQL_EXEC|grep 'xxxxx'|awk '{print $2}'`			
				#4：00检查导出非0	
				echo s1
				if [ ${alert_time} = "04" ];then
						#if [ ${dat_file_cnt} -eq  0 ];then
						#		MESSAGE_CONTENT="数据文件数：${dat_file_cnt},导出延迟,请核查！"
						#		echo ${MESSAGE_CONTENT}
						#		sendalarmsms "${MESSAGE_CONTENT}"
						#fi								
						if [ ${verf_file_cnt} -eq  0 ];then
								MESSAGE_CONTENT="数据文件数：${verf_file_cnt},导出延迟,请核查！"
								echo ${MESSAGE_CONTENT}
								sendalarmsms "${MESSAGE_CONTENT}"
						fi
				fi
				#echo s2
				#if [ ${dat_file_cnt} -ne  ${exp_cnt} ];then
				#	MESSAGE_CONTENT="数据文件数：${dat_file_cnt}不等于 ${exp_cnt},请先处理！"
				#	echo ${MESSAGE_CONTENT}
				#	sendalarmsms "${MESSAGE_CONTENT}"
				#fi
				echo s3							
				if [ ${verf_file_cnt} -ne  ${exp_cnt}  ];then 
					MESSAGE_CONTENT="校验文件数:${verf_file_cnt}不等于${exp_cnt},请先处理！"
					echo ${MESSAGE_CONTENT}
					sendalarmsms "${MESSAGE_CONTENT}"
				fi
		 fi


################################################################
##表空间监控
################################################################

                                                                
################################################################
##月数据监控                                                    
################################################################
sleep 600
done

exit