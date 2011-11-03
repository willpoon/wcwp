#!/usr/bin/ksh
##############################################################
#author : panzhiwei
#���ܣ�һ���ճ����
#r1 : ��¼������ʱ���أ�ֻ��ʾһ�Σ�
#r2 : ����ʱ���ӿ��ϴ���ء��ϴ����ѡ���δ����ָ��Сʱ��10��������һ�Ρ�
#r3 : �ӿ��ļ�������أ�3-6��ÿ10����ɨ��һ�Σ����쳣�������澯��
#r4 : ���Ӽ�أ��賿4��00���ӿڵ���Ϊ0���澯��
#r5 :	2011-05-06 19:13:37����ͳ��˳����wc -l ͳ���ļ���,�ٲ����ݿ�exp����������� 
#r6 :	2011-05-06 19:13:40ֻ���verf file,�����dat file
#r7 :	make read cfgfile inside the while loop
#r8:	adjust program structure & add load_sample
##############################################################
. /bassapp/bihome/panzw/bin/bass1_common.ksh
################################################################
##main
##���ݼ�أ�������
################################################################
echo $$
##define global var


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


cfgfile=/bassapp/bihome/panzw/bass1_mon.cfg
cat $cfgfile|grep -i d_at09_cnt | nawk -F'=' '{print $2}'|read d_at09_cnt
cat $cfgfile|grep -i d_at11_cnt | nawk -F'=' '{print $2}'|read d_at11_cnt
cat $cfgfile|grep -i d_at13_cnt | nawk -F'=' '{print $2}'|read d_at13_cnt
cat $cfgfile|grep -i d_at15_cnt | nawk -F'=' '{print $2}'|read d_at15_cnt
cat $cfgfile|grep -i d_all_cnt | nawk -F'=' '{print $2}' |read d_all_cnt
cat $cfgfile|grep -i m_on03_cnt | nawk -F'=' '{print $2}'|read m_on03_cnt
cat $cfgfile|grep -i m_on05_cnt | nawk -F'=' '{print $2}'|read m_on05_cnt
cat $cfgfile|grep -i m_on08_cnt | nawk -F'=' '{print $2}'|read m_on08_cnt
cat $cfgfile|grep -i m_on10_cnt | nawk -F'=' '{print $2}'|read m_on10_cnt
cat $cfgfile|grep -i m_on15_cnt | nawk -F'=' '{print $2}'|read m_on15_cnt
cat $cfgfile|grep -i m_all_cnt | nawk -F'=' '{print $2}' |read m_all_cnt

	
	#��־ʱ��
	echo `date +%Y%m%d%H%M%S`	
	#�Բ�����������ţ�һ��ֻ��һ�Σ���Ҫ����һ�κ�Ҫ���ñ�־λ����Ƕ����ѷ���1����ÿ��06��00����Ϊδ����0.
			#��ʼ��Ϊ1���ѷ��ͣ��Է���ͣ�ط�!

			now_alert_time=`getunixtime2`
			diffs=`expr $now_alert_time - $start_run_time`
			#һ���Գ�ʼ��g_d_recordlvl_sent_flag,��ֹ�ظ���0,�������޸�Ϊ1��״̬�����ˡ�
			if [ $diffs -lt 60 ];then 
			g_d_recordlvl_sent_flag=0
			all_export_done_flag=0
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
			#echo ${g_d_recordlvl_sent_flag}
			#set 0 everyday at 04:00
			if [ ${alert_time} = "04" -a ${all_export_done_flag} -eq 1 ];then 			
				all_export_done_flag=0
			fi
#1.�ճ�֪ͨ�ࣺÿ��һ�������ݼ�¼������ʱ�䣬����ʱ������(��Ӧ�ڴ�������֮�󣬼�08��00��)
			#1.1�����ݣ���ؼ�¼�����أ���ȫ������ʱ������
			#echo 1.1
			alert_time=`date +%H`
			#����${g_d_recordlvl_sent_flag} -eq 0 ��˫���жϣ�����ɨ�����(��16*5�μ��ٵ�1*5��)
			if [ ${alert_time} -ge "08" -a ${g_d_recordlvl_sent_flag} -eq 0 ];then 			
				fn_d_recordlvl_ret_cnt
				v_d_recordlvl_ret_cnt=$?
				echo ${v_d_recordlvl_ret_cnt}
				if [ ${v_d_recordlvl_ret_cnt} -eq ${d_all_cnt} -a ${g_d_recordlvl_sent_flag} -eq 0 ]	; then 
					MESSAGE_CONTENT="�սӿ�(day)��¼��У����ȫ����������."
					sendalarmsms "${MESSAGE_CONTENT}"
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
					sendalarmsms "${MESSAGE_CONTENT}"
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
				if [ ${v_d_filelvl_ret_cnt} -ne ${d_at09_cnt} ]	;then 
					MESSAGE_CONTENT="9��ǰ�ӿ��ļ������ز�����${d_at09_cnt}��������9��ǰ����ȷ���Ƿ����ϴ�!"
					sendalarmsms "${MESSAGE_CONTENT}"
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
				if [ ${v_d_filelvl_ret_cnt} -ne ${d_at11_cnt} ]	;then 
					MESSAGE_CONTENT="11��ǰ�ӿ��ļ������ز�����${d_at11_cnt}��������11��ǰ����ȷ���Ƿ����ϴ�!"
					sendalarmsms "${MESSAGE_CONTENT}"
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
				if [ ${v_d_filelvl_ret_cnt} -ne ${d_at13_cnt} ]	;then 
					MESSAGE_CONTENT="13��ǰ�ӿ��ļ������ز�����${d_at13_cnt}��������13��ǰ����ȷ���Ƿ����ϴ�!"
					sendalarmsms "${MESSAGE_CONTENT}"
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
				if [ ${v_d_filelvl_ret_cnt} -ne ${d_at15_cnt} ]	;then 
					MESSAGE_CONTENT="15��ǰ�ӿ��ļ������ز�����${d_at15_cnt}��������15��ǰ����ȷ���Ƿ����ϴ�!"
					sendalarmsms "${MESSAGE_CONTENT}"
				fi
			fi
			


################################################################
##�������
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
					#echo "dat_file_cnt�����ļ��� :${dat_file_cnt}"
				  verf_file_cnt=`ls -lrt ${exp_dir}/*.verf | wc -l|awk '{print $1}'`
				  echo "verf_file_cnt У���ļ��� :${verf_file_cnt}"
					exp_cnt=`DB2_SQL_EXEC|grep 'xxxxx'|awk '{print $2}'`
				#4��00��鵼����0	
				echo s1
				if [ ${alert_time} = "04" ];then
						#if [ ${dat_file_cnt} -eq  0 ];then
						#		MESSAGE_CONTENT="�����ļ�����${dat_file_cnt},�����ӳ�,��˲飡"
						#		echo ${MESSAGE_CONTENT}
						#		sendalarmsms "${MESSAGE_CONTENT}"
						#fi								
						if [ ${verf_file_cnt} -eq  0 ];then
								MESSAGE_CONTENT="�����ļ�����${verf_file_cnt},�����ӳ�,��˲飡"
								echo ${MESSAGE_CONTENT}
								sendalarmsms "${MESSAGE_CONTENT}"
						fi
				fi
				#echo s2
				#if [ ${dat_file_cnt} -ne  ${exp_cnt} ];then
				#	MESSAGE_CONTENT="�����ļ�����${dat_file_cnt}������ ${exp_cnt},���ȴ���"
				#	echo ${MESSAGE_CONTENT}
				#	sendalarmsms "${MESSAGE_CONTENT}"
				#fi
				echo s3							
				if [ ${verf_file_cnt} -ne  ${exp_cnt}  ];then 
					MESSAGE_CONTENT="У���ļ���:${verf_file_cnt}������${exp_cnt},���ȴ���"
					echo ${MESSAGE_CONTENT}
					sendalarmsms "${MESSAGE_CONTENT}"
				fi
		 fi

##2.0 ��ʾ�ϴ������ݷ������
##ȫ��������ɺ󴥷�
		today=`date '+%Y%m%d'`
		deal_date=`yesterday ${today}`
		exp_dir="/bassapp/backapp/data/bass1/export/export_${deal_date}"
		verf_file_cnt=`ls -lrt ${exp_dir}/*.verf | wc -l|awk '{print $1}'`
	
	if [ ${verf_file_cnt} -eq ${d_all_cnt} -a all_export_done_flag -eq 0 ]	;then 
		MESSAGE_CONTENT="���սӿ�ȫ������!"
		sendalarmsms "${MESSAGE_CONTENT}"
		#�����ͱ����Ϊ�ѷ��ͣ�
		all_export_done_flag=1					
	fi
			

################################################################
##�����·���� "/data1/asiainfo/interface/sample"
################################################################

################################################################
##�����·���� "/data1/asiainfo/interface/imei"
################################################################


################################################################
##��ռ���
################################################################

                                                                
################################################################
##�����ݼ��                                                    
################################################################

sleep 600
done

exit