######################################################################################################
#�������ƣ�	INT_CHECK_R196TO207_DAY.tcl
#У��ӿڣ�	02054,02059,02061
#��������: DAY
#�������: 
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ�PANZHIWEI
#��дʱ�䣺2011-05-26 
#�����¼��
#�޸���ʷ:  
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

        #���� yyyymmdd
        set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]      
				puts $timestamp
				
        #��Ȼ��
				puts $op_time 
        set curr_month [string range $op_time 0 3][string range $op_time 5 6]
				puts $curr_month
        
        #������
        set app_name "INT_CHECK_R196194_DAY.tcl"



########################################################################################################3

 	  set sqlbuf "delete from  BASS1.G_RULE_CHECK 
 	  where time_id=$timestamp 
 	  and rule_code in ('R196','R198','R200','R203','R204','R205','R207') 
 	  "

	  exec_sql $sqlbuf
#�������е��ֻ��������ݶ�û�ж�����	  У������Ϊ0 �� ͨ����
#R196			����	��	02_���ſͻ�	�ֻ����䣨ADC��ʹ�ü��ſͻ���	�ֻ����䣨ADC��ʹ�ü��ſͻ�����ʹ���ֻ����䣨ADC���ļ��Ÿ��˿ͻ���	0.05		

#	�ֻ����䣨ADC��ʹ�ü��ſͻ���	�ֻ����䣨ADC��ʹ�ü��ſͻ���
#

	 set RESULT_VAL1 0
	 set RESULT_VAL2 0
	 set RESULT_VAL  0

	set sql_buff "
			select count(0) cnt
			from 
			(
			select t.*,row_number()over(partition by ENTERPRISE_ID order by time_id desc) rn 
			from (
			select *
			from g_a_02054_day where  MANAGE_MODE = '2'
			and time_id <= $timestamp and ENTERPRISE_BUSI_TYPE = '1220'
			) t 
			) t2 where rn = 1 and STATUS_ID ='1'
	"
	 set RESULT_VAL1 [get_single $sql_buff]
	 puts $RESULT_VAL1

#	 ʹ���ֻ����䣨ADC���ļ��Ÿ��˿ͻ���
#note : �ֻ�������˶�������02059���02061�
##ȥ��						and length(trim(user_id)) = 14
##������
	set sql_buff "
				select count(0)
				from 
				(
						select t.*,row_number()over(partition by user_id order by time_id desc ) rn 
						from 
						(
						select * from G_A_02061_DAY
						where 
								 ENTERPRISE_BUSI_TYPE = '1220'
						and  MANAGE_MODE = '2'
						and time_id <= $timestamp
						) t
				) t2
				where rn = 1 and STATUS_ID ='1'
	"
	 set RESULT_VAL2 [get_single $sql_buff]
	 puts $RESULT_VAL2
	 
	 set RESULT_VAL [ expr  $RESULT_VAL1 - $RESULT_VAL2 ]
	 
 	#���Ϸ���: 0 - ������ ����0 - ������
	if { $RESULT_VAL > 0 } {
		set grade 2
	        set alarmcontent " R196 У�鲻ͨ��"
	        puts ${alarmcontent}	        
	        WriteAlarm $app_name $op_time $grade ${alarmcontent}
	}
		 
	#--��У��ֵ����У������
	set sql_buff "
		INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R196',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL,0) 
		"
		
	exec_sql $sql_buff

#R198			����	��	02_���ſͻ�	�ֻ����䣨MAS��ʹ�ü��ſͻ���	�ֻ����䣨MAS��ʹ�ü��ſͻ�����ʹ���ֻ����䣨MAS���ļ��Ÿ��˿ͻ���	0.05		


	 set RESULT_VAL1 0
	 set RESULT_VAL2 0
	 set RESULT_VAL  0

	set sql_buff "
			select count(0) cnt
			from 
			(
			select t.*,row_number()over(partition by ENTERPRISE_ID order by time_id desc) rn 
			from (
			select *
			from g_a_02054_day where  MANAGE_MODE = '1'
			and time_id <= $timestamp and ENTERPRISE_BUSI_TYPE = '1220'
			) t 
			) t2 where rn = 1 and STATUS_ID ='1'
	"
	 set RESULT_VAL1 [get_single $sql_buff]
	 puts $RESULT_VAL1

#note : �ֻ�������˶�������02059���02061�
#rm:						and length(trim(user_id)) = 14

	set sql_buff "
				select count(0)
				from 
				(
						select t.*,row_number()over(partition by user_id order by time_id desc ) rn 
						from 
						(
						select * from G_A_02061_DAY
						where 
								 ENTERPRISE_BUSI_TYPE = '1220'
						and  MANAGE_MODE = '1'
						and time_id <= $timestamp
						) t
				) t2
				where rn = 1 and STATUS_ID ='1'
	"
	 set RESULT_VAL2 [get_single $sql_buff]
	 puts $RESULT_VAL2
	 
	 set RESULT_VAL [ expr  $RESULT_VAL1 - $RESULT_VAL2 ]
	 
 	#���Ϸ���: 0 - ������ ����0 - ������
	if { $RESULT_VAL > 0 } {
		set grade 2
	        set alarmcontent " R198 У�鲻ͨ��"
	        puts ${alarmcontent}	        
	        WriteAlarm $app_name $op_time $grade ${alarmcontent}
	}
		 
	#--��У��ֵ����У������
	set sql_buff "
		INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R198',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL,0) 
		"
		
	exec_sql $sql_buff		 
	

#R200			����	��	02_���ſͻ�	�ֻ����䣨��ҵ�棩ʹ�ü��ſͻ���	�ֻ����䣨��ҵ�棩ʹ�ü��ſͻ�����ʹ���ֻ����䣨��ҵ�棩���Ÿ��˿ͻ���	0.05		
#������ͨ��02054��02061ȡ��

##�ӿڵ�Ԫ���ƣ���ҵ����ҵ�񶩹����
##�ӿڵ�Ԫ���룺 02056
#
#
#	 set RESULT_VAL1 0
#	 set RESULT_VAL2 0
#	 set RESULT_VAL  0
#
#	set sql_buff "
#			select count(0) cnt
#			from 
#			(
#			select t.*,row_number()over(partition by ENTERPRISE_ID order by time_id desc) rn 
#			from (
#			select *
#			from G_A_02056_DAY where   time_id <= $timestamp and ENTERPRISE_BUSI_TYPE = '1210'
#			) t 
#			) t2 where rn = 1 and STATUS_ID ='1'
#	"
#	 set RESULT_VAL1 [get_single $sql_buff]
#	 puts $RESULT_VAL1
#
##	 ʹ���ֻ����䣨ADC���ļ��Ÿ��˿ͻ���
##note : �ֻ�������˶�������02059���02061�
##�ֻ�������ҵ�涩����02061�����ϱ���
##rm 						and length(trim(user_id)) = 14
#
#	set sql_buff "
#				select count(0)
#				from 
#				(
#						select t.*,row_number()over(partition by user_id order by time_id desc ) rn 
#						from 
#						(
#						select * from G_A_02061_DAY
#						where 
#								 ENTERPRISE_BUSI_TYPE = '1210'
#						and time_id <= $timestamp
#						) t
#				) t2
#				where rn = 1 and STATUS_ID ='1'
#	"
#	 set RESULT_VAL2 [get_single $sql_buff]
#	 puts $RESULT_VAL2
#	 
#	 set RESULT_VAL [ expr  $RESULT_VAL1 - $RESULT_VAL2 ]
#	 
# 	#���Ϸ���: 0 - ������ ����0 - ������
#	if { $RESULT_VAL > 0 } {
#		set grade 2
#	        set alarmcontent " R200 У�鲻ͨ��"
#	        puts ${alarmcontent}	        
#	        WriteAlarm $app_name $op_time $grade ${alarmcontent}
#	}
#		 
#	#--��У��ֵ����У������
#	set sql_buff "
#		INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R200',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL,0) 
#		"
#		
#	exec_sql $sql_buff		 
#	



#R200			����	��	02_���ſͻ�	�ֻ����䣨MAS��ʹ�ü��ſͻ���	�ֻ����䣨MAS��ʹ�ü��ſͻ�����ʹ���ֻ����䣨MAS���ļ��Ÿ��˿ͻ���	0.05		


	 set RESULT_VAL1 0
	 set RESULT_VAL2 0
	 set RESULT_VAL  0

	set sql_buff "
			select count(0) cnt
			from 
			(
			select t.*,row_number()over(partition by ENTERPRISE_ID order by time_id desc) rn 
			from (
			select *
			from g_a_02054_day where  MANAGE_MODE = '3'
			and time_id <= $timestamp and ENTERPRISE_BUSI_TYPE = '1220'
			) t 
			) t2 where rn = 1 and STATUS_ID ='1'
	"
	 set RESULT_VAL1 [get_single $sql_buff]
	 puts $RESULT_VAL1

#note : �ֻ�������˶�������02059���02061�
#rm:						and length(trim(user_id)) = 14

	set sql_buff "
				select count(0)
				from 
				(
						select t.*,row_number()over(partition by user_id order by time_id desc ) rn 
						from 
						(
						select * from G_A_02061_DAY
						where 
								 ENTERPRISE_BUSI_TYPE = '1220'
						and  MANAGE_MODE = '3'
						and time_id <= $timestamp
						) t
				) t2
				where rn = 1 and STATUS_ID ='1'
	"
	 set RESULT_VAL2 [get_single $sql_buff]
	 puts $RESULT_VAL2
	 
	 set RESULT_VAL [ expr  $RESULT_VAL1 - $RESULT_VAL2 ]
	 
 	#���Ϸ���: 0 - ������ ����0 - ������
	if { $RESULT_VAL > 0 } {
		set grade 2
	        set alarmcontent " R200 У�鲻ͨ��"
	        puts ${alarmcontent}	        
	        WriteAlarm $app_name $op_time $grade ${alarmcontent}
	}
		 
	#--��У��ֵ����У������
	set sql_buff "
		INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R200',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL,0) 
		"
		
	exec_sql $sql_buff		 
	
	
	
#R203			����	��	02_���ſͻ�	����V��ʹ�ü��ſͻ���	����V��ʹ�ü��ſͻ���������V����Ա���µ�����	0.05		


	 set RESULT_VAL1 0
	 set RESULT_VAL2 0
	 set RESULT_VAL  0

	set sql_buff "
			select count(0) cnt
			from 
			(
			select t.*,row_number()over(partition by ENTERPRISE_ID order by time_id desc) rn 
			from (
			select *
			from g_a_02054_day where  time_id <= $timestamp 
					and ENTERPRISE_BUSI_TYPE in ('1000','1010')
					and MANAGE_MODE = '3'
			) t 
			) t2 where rn = 1 and STATUS_ID ='1'
	"
	 set RESULT_VAL1 [get_single $sql_buff]
	 puts $RESULT_VAL1

#rm						and length(trim(user_id)) = 14

	set sql_buff "
				select count(0)
				from 
				(
						select t.*,row_number()over(partition by enterprise_id,user_id order by time_id desc ) rn 
						from 
						(
						select * from G_A_02059_DAY
						where 
								 ENTERPRISE_BUSI_TYPE  in ('1000','1010')				
						and time_id <= $timestamp
						and MANAGE_MODE = '3'
						) t
				) t2
				where rn = 1 and STATUS_ID ='1'
	"
	 set RESULT_VAL2 [get_single $sql_buff]
	 puts $RESULT_VAL2
	 
	 set RESULT_VAL [ expr  $RESULT_VAL1 - $RESULT_VAL2 ]
	 
 	#���Ϸ���: 0 - ������ ����0 - ������
	if { $RESULT_VAL > 0 } {
		set grade 2
	        set alarmcontent " R203 У�鲻ͨ��"
	        puts ${alarmcontent}	        
	        WriteAlarm $app_name $op_time $grade ${alarmcontent}
	}
		 
	#--��У��ֵ����У������
	set sql_buff "
		INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R203',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL,0) 
		"
		
	exec_sql $sql_buff		 


#R204			����	��	02_���ſͻ�	��������绰ʹ�ü��ſͻ���	��������绰ʹ�ü��ſͻ�������������绰ʹ���ն���	0.05		


	 set RESULT_VAL1 0
	 set RESULT_VAL2 0
	 set RESULT_VAL  0

	set sql_buff "
			select count(0) cnt
			from 
			(
			select t.*,row_number()over(partition by ENTERPRISE_ID order by time_id desc) rn 
			from (
			select *
			from g_a_02054_day where  time_id <= $timestamp 
					and ENTERPRISE_BUSI_TYPE in ('1040')
						and MANAGE_MODE = '3'
			) t 
			) t2 where rn = 1 and STATUS_ID ='1'
	"
	 set RESULT_VAL1 [get_single $sql_buff]
	 puts $RESULT_VAL1
#rm						and length(trim(user_id)) = 14

	set sql_buff "
				select count(0)
				from 
				(
						select t.*,row_number()over(partition by user_id order by time_id desc ) rn 
						from 
						(
						select * from G_A_02059_DAY
						where 
								 ENTERPRISE_BUSI_TYPE  in ('1040')						
						and time_id <= $timestamp
						and MANAGE_MODE = '3'						
						) t
				) t2
				where rn = 1 and STATUS_ID ='1'
	"
	 set RESULT_VAL2 [get_single $sql_buff]
	 puts $RESULT_VAL2
	 
	 set RESULT_VAL [ expr  $RESULT_VAL1 - $RESULT_VAL2 ]
	 
 	#���Ϸ���: 0 - ������ ����0 - ������
	if { $RESULT_VAL > 0 } {
		set grade 2
	        set alarmcontent " R204 У�鲻ͨ��"
	        puts ${alarmcontent}	        
	        WriteAlarm $app_name $op_time $grade ${alarmcontent}
	}
		 
	#--��У��ֵ����У������
	set sql_buff "
		INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R204',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL,0) 
		"
		
	exec_sql $sql_buff		 



#R205			����	��	02_���ſͻ�	����ר��ʹ�ü��ſͻ���	����ר��ʹ�ü��ſͻ���������ר�߶˿���	0.05		
#��У�����02057����

	 set RESULT_VAL1 0
	 set RESULT_VAL2 0
	 set RESULT_VAL  0

	set sql_buff "
			select count(0) cnt 
			from 
			(
			select t.*,row_number()over(partition by ENTERPRISE_ID order by time_id desc) rn 
			from (
			select *
			from g_a_02054_day where  time_id <= $timestamp 
					and ENTERPRISE_BUSI_TYPE in ('1180')
					and MANAGE_MODE = '3'						
			) t 
			) t2 where rn = 1 and STATUS_ID ='1'
	"
	 set RESULT_VAL1 [get_single $sql_buff]
	 puts $RESULT_VAL1


	set sql_buff "
			select value(sum(bigint(PORT_NUMS)),0) PORT_NUMS   
			from 
			(
			select t.*,row_number()over(partition by ENTERPRISE_ID order by time_id desc) rn 
			from (
			select *
			from g_a_02057_day where  time_id <= $timestamp 
					and ENTERPRISE_BUSI_TYPE in ('1180')
			) t 
			) t2 where rn = 1 and STATUS_ID ='1'
	"
	 set RESULT_VAL2 [get_single $sql_buff]
	 puts $RESULT_VAL2
	 
	 set RESULT_VAL [ expr  $RESULT_VAL1 - $RESULT_VAL2 ]
	 
 	#���Ϸ���: 0 - ������ ����0 - ������
	if { $RESULT_VAL > 0 } {
		set grade 2
	        set alarmcontent " R205 У�鲻ͨ��"
	        puts ${alarmcontent}	        
	        WriteAlarm $app_name $op_time $grade ${alarmcontent}
	}
		 
	#--��У��ֵ����У������
	set sql_buff "
		INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R205',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL,0) 
		"
		
	exec_sql $sql_buff		 

#R207			����	��	02_���ſͻ�	BLACKBERRYʹ�ü��ſͻ���	BLACKBERRYʹ�ü��ſͻ�����ʹ��BLACKBERRY���Ÿ��˿ͻ���	0.05		
#BlackBerry���ֻ����䡢M2M�ȣ����ڴ�02059�ӿ��ϱ�����02060��
	 set RESULT_VAL1 0
	 set RESULT_VAL2 0
	 set RESULT_VAL  0

	set sql_buff "
			select count(0) cnt 
			from 
			(
			select t.*,row_number()over(partition by ENTERPRISE_ID order by time_id desc) rn 
			from (
			select *
			from g_a_02054_day where  time_id <= $timestamp 
					and ENTERPRISE_BUSI_TYPE in ('1230')
					and MANAGE_MODE = '3'	
			) t 
			) t2 where rn = 1 and STATUS_ID ='1'
	"
	 set RESULT_VAL1 [get_single $sql_buff]
	 puts $RESULT_VAL1

#rm 						and length(trim(user_id)) = 14

	set sql_buff "
				select count(0)
				from 
				(
						select t.*,row_number()over(partition by user_id order by time_id desc ) rn 
						from 
						(
						select * from G_A_02060_DAY
						where 
								 ENTERPRISE_BUSI_TYPE  in ('1230')						
						and time_id <= $timestamp
						and MANAGE_MODE = '3'	
						) t
				) t2
				where rn = 1 and STATUS_ID ='1'
	"
	 set RESULT_VAL2 [get_single $sql_buff]
	 puts $RESULT_VAL2
	 
	 set RESULT_VAL [ expr  $RESULT_VAL1 - $RESULT_VAL2 ]
	 
 	#���Ϸ���: 0 - ������ ����0 - ������
	if { $RESULT_VAL > 0 } {
		set grade 2
	        set alarmcontent " R207 У�鲻ͨ��"
	        puts ${alarmcontent}	        
	        WriteAlarm $app_name $op_time $grade ${alarmcontent}
	}
		 
	#--��У��ֵ����У������
	set sql_buff "
		INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R207',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL,0) 
		"
		
	exec_sql $sql_buff		 



##R208			����	��	02_���ſͻ�	�ǹ�ͨ���¼Ʒѿͻ���	�ǹ�ͨ���¼Ʒѿͻ����ܳǹ�ͨ����ʹ�ÿͻ���	0.05		
#��У�鲻�����ڱ�tcl;
#	 set RESULT_VAL1 0
#	 set RESULT_VAL2 0
#	 set RESULT_VAL  0
#
#	set sql_buff "
#			select count(0) cnt 
#			from 
#			(
#			select t.*,row_number()over(partition by ENTERPRISE_ID order by time_id desc) rn 
#			from (
#			select *
#			from g_a_02054_day where  time_id <= $timestamp 
#					and ENTERPRISE_BUSI_TYPE in ('1230')
#			) t 
#			) t2 where rn = 1 and STATUS_ID ='1'
#	"
#	 set RESULT_VAL1 [get_single $sql_buff]
#	 puts $RESULT_VAL1
#
#
#	set sql_buff "
#				select count(0)
#				from 
#				(
#						select t.*,row_number()over(partition by user_id order by time_id desc ) rn 
#						from 
#						(
#						select * from G_A_02060_DAY
#						where 
#								 ENTERPRISE_BUSI_TYPE  in ('1230')						
#						and time_id <= $timestamp
#						and length(trim(user_id)) = 14
#						) t
#				) t2
#				where rn = 1 and STATUS_ID ='1'
#	"
#	 set RESULT_VAL2 [get_single $sql_buff]
#	 puts $RESULT_VAL2
#	 
#	 set RESULT_VAL [ expr  $RESULT_VAL1 - $RESULT_VAL2 ]
#	 
# 	#���Ϸ���: 0 - ������ ����0 - ������
#	if { $RESULT_VAL > 0 } {
#		set grade 2
#	        set alarmcontent " R207 У�鲻ͨ��"
#	        puts ${alarmcontent}	        
#	        WriteAlarm $app_name $op_time $grade ${alarmcontent}
#	}
#		 
#	#--��У��ֵ����У������
#	set sql_buff "
#		INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R207',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL,0) 
#		"
#		
#	exec_sql $sql_buff		 
#		
	return 0
}

