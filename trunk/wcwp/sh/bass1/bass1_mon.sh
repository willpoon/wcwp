#���ܣ���ʵ�����¼�أ�
#1.��ռ�
#2.�սӿ�
#3.�½ӿ�
#4.��������
#5.���ݵ�������һ��
#6.�������ѣ�������
#
#ԭ�򣺲�������С��

#��DB2���ݿ���ִ��SQL
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



#�����ļ���������
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


#�����ļ��������� (9��ӿ�)
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
								   where upload_time = 'ÿ��9��ǰ' \
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


#�����ļ��������� (11��ӿ�)
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
								   where upload_time = 'ÿ��11��ǰ' \
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



#�����ļ��������� (13��ӿ�)
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
								   where upload_time = 'ÿ��13��ǰ' \
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




#�����ļ��������� (15��ӿ�)
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
								   where upload_time = 'ÿ��15��ǰ' \
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




#���ռ�¼��������
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
	#sun os date ������ unixtimestamp , Ψ��������ȡ�ˣ���һ���ľ�����
		if [ $# -ne 0 ];then
		echo "getmtime"
		exit
		fi
		touchfile="."
		touch $touchfile
		in_file=$touchfile
		if [ ! -d ${in_file} ];then 
		echo "getmtime ���������ļ� ${in_file} ������!!"
		return 1
		fi
		tcltimestamp=`echo "puts [file mtime ${in_file}]"|tclsh|awk -F'\r' '{print $1}'`
		echo  $tcltimestamp
		return 0
}

################################################################
##main
##���ݼ�أ�������
################################################################
echo $$
echo `date +%Y%m%d%H%M%S`
start_run_time=`getunixtime2`
while [ true ]
do
	#�����˳�����
	if [ -f ./stop_bass1_mon ];then 
	echo "$0 normal exit"
	echo $$
	rm ./stop_bass1_mon
	exit
	fi
#�Բ�����������ţ�һ��ֻ��һ�Σ���Ҫ����һ�κ�Ҫ���ñ�־λ����Ƕ����ѷ���1����ÿ��06��00����Ϊδ����0.
			#��ʼ��Ϊ1���ѷ��ͣ��Է���ͣ�ط�!

			now_alert_time=`getunixtime2`
			diffs=`expr $now_alert_time - $start_run_time`
			#һ���Գ�ʼ��g_d_recordlvl_sent_flag,��ֹ�ظ���0,�������޸�Ϊ1��״̬�����ˡ�
			if [ $diffs -lt 60 ];then 
			g_d_recordlvl_sent_flag=0
			fi
			#g_d_recordlvl_sent_flag��
			#�����¼��ȫ�����أ�ӦΪ1
			#���δȫ�����أ�ӦΪ0
			#������δȫ������ʱ��ӦΪ0��ȫ�����أ�ӦΪ1
			#���ݹ�����ÿ��7��Ÿտ�ʼ�����ݣ���������ȫ�����أ��ʿ�����07�㽫g_d_recordlvl_sent_flag����
			#������09��ȫ�����أ���ô�ڵ���09-����07���ʱ�䣬g_d_recordlvl_sent_flag=1.
			#��07-09�ڼ䣬g_d_recordlvl_sent_flag = 0�����ڵȴ�����״̬��
			#07��g_d_recordlvl_sent_flag����
			alert_time=`date +%H`
			if [ ${alert_time} = "07" -a ${g_d_recordlvl_sent_flag} -eq 1 ];then 			
				g_d_recordlvl_sent_flag=0
			fi
			echo ${g_d_recordlvl_sent_flag}
#1.�ճ�֪ͨ�ࣺÿ��һ�������ݼ�¼������ʱ�䣬����ʱ������(��Ӧ�ڴ�������֮�󣬼�08��00��)
			#1.1�����ݣ���ؼ�¼�����أ���ȫ������ʱ������
			#echo 1.1
			alert_time=`date +%H`
			if [ ${alert_time} -ge "08" ];then 			
				fn_d_recordlvl_ret_cnt
				v_d_recordlvl_ret_cnt=$?
				echo ${v_d_recordlvl_ret_cnt}
				if [ ${v_d_recordlvl_ret_cnt} -eq 56 -a ${g_d_recordlvl_sent_flag} -eq 0 ]	; then 
					MESSAGE_CONTENT="�սӿ�(day)��¼��У����ȫ����������!!"
					sendalarmsms ${MESSAGE_CONTENT}
					#�����ͱ����Ϊ�ѷ��ͣ�
					g_d_recordlvl_sent_flag=1
				fi
			fi

#2.�ճ������ࣺ���ճ������ƻ������ң�û�а�ʱ����ʱ��������ʱ���������ض���һСʱ�ڴ�������ʱ�������5����
			
			#2.1 ��ʾ�ϴ������ݣ�Ϊ�˷�ֹ�����������û���������������ѣ�
			#ÿ��8��00��
			#echo 2.1
			alert_time=`date +%H`
			if [ ${alert_time} = "08" ];then 
			
				fn_d_file_lvl_ret_cnt      
				v_d_filelvl_ret_cnt=$?     
				echo ${v_d_filelvl_ret_cnt}
			
				if [ ${v_d_filelvl_ret_cnt} -eq 0 ]	;then 
					MESSAGE_CONTENT="���սӿ���δ�ϴ�����ץ���ϴ���"
					sendalarmsms ${MESSAGE_CONTENT}
				fi
			fi
			
			
			#2.2��ʾ����9��ǰ�ӿڣ����9��ӿ��ϴ����,�����û���������������ѣ�
			#ÿ��08��00��
			#echo 2.2		
			alert_time=`date +%H`
			if [ ${alert_time} = "08" ];then 
				fn_d9_file_lvl_ret_cnt
				v_d_filelvl_ret_cnt=$?
				echo ${v_d_filelvl_ret_cnt}
				if [ ${v_d_filelvl_ret_cnt} -ne 8 ]	;then 
					MESSAGE_CONTENT="9��ǰ�ӿڲ�����8��������9��ǰ����!"
					sendalarmsms ${MESSAGE_CONTENT}
				fi
			fi
			
			#2.3��ʾ����11��ǰ�ӿڣ����11��ӿ��ϴ����,�����û���������������ѣ�
			#ÿ��10��00��
			#echo 2.3			
			alert_time=`date +%H`
			if [ ${alert_time} = "10" ];then 
				fn_d11_file_lvl_ret_cnt
				v_d_filelvl_ret_cnt=$?
				echo ${v_d_filelvl_ret_cnt}
				if [ ${v_d_filelvl_ret_cnt} -ne 30 ]	;then 
					MESSAGE_CONTENT="9��ǰ�ӿڲ�����30��������11��ǰ����!"
					sendalarmsms ${MESSAGE_CONTENT}
				fi
			fi
			
			
			#2.4��ʾ����13��ǰ�ӿ�:���13��ӿ��ϴ����,�����û���������������ѣ�
			#ÿ��12��00��
			#echo 2.4			
			alert_time=`date +%H`
			if [ ${alert_time} = "12" ];then 
				fn_d13_file_lvl_ret_cnt
				v_d_filelvl_ret_cnt=$?
				echo ${v_d_filelvl_ret_cnt}
				if [ ${v_d_filelvl_ret_cnt} -ne 12 ]	;then 
					MESSAGE_CONTENT="13��ǰ�ӿڲ�����12��������13��ǰ����!"
					sendalarmsms ${MESSAGE_CONTENT}
				fi
			fi
			
			
			#2.5��ʾ����15��ǰ�ӿ�:���15��ӿ��ϴ����,�����û���������������ѣ�
			#ÿ��14��00��
			#echo 2.5			
			alert_time=`date +%H`
			if [ ${alert_time} = "14" ];then 
			fn_d15_file_lvl_ret_cnt
			v_d_filelvl_ret_cnt=$?
			echo ${v_d_filelvl_ret_cnt}
				if [ ${v_d_filelvl_ret_cnt} -ne 6 ]	;then 
					MESSAGE_CONTENT="15��ǰ�ӿڲ�����6��������15��ǰ����!"
					sendalarmsms ${MESSAGE_CONTENT}
				fi
			fi
			


################################################################
##�����ݼ��
################################################################



################################################################
##��ռ���
################################################################
sleep 600
done

exit