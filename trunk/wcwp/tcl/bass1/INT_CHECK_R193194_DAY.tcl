######################################################################################################
#�������ƣ�	INT_CHECK_R193194_DAY.tcl
#У��ӿڣ�	01007
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
        set app_name "INT_CHECK_R193194_DAY.tcl"



########################################################################################################3

#R193			����	��	02_���ſͻ�	���ſͻ���Ŀ���г��嵥�ͻ���Ӧ��ϵ�ӿ��еļ��ſͻ���ʶ�����ڼ��ſͻ�������	01007(���ſͻ���Ŀ���г��嵥�ͻ���Ӧ��ϵ)�е�"���ſͻ���ʶ"����01004(���ſͻ�)��"���ſͻ���ʶ"��	0.05		

 	  set sqlbuf "delete from  BASS1.G_RULE_CHECK where time_id=$timestamp and rule_code in ('R193','R194') "        

	  exec_sql $sqlbuf

#	G_A_01007_DAY����ӿ�BOSSû��ά�����ݣ��ֹ�װ���������������û�и��¡�
#  
	set sql_buff "
		select count(0) from 
		(select a.* , row_number()over(partition by enterprise_id,cust_id order by time_id desc ) rn 
			from G_A_01007_DAY a 
		) a where a.rn = 1 
		and RELA_STATE = '1'
		and not exists (select 1 from (select distinct value(b.NEW_ENTERPRISE_ID,a.enterprise_id) enterprise_id from bass1.G_A_01004_DAY a 
LEFT JOIN BASS2.TRANS_ENTERPRISE_ID_20100625 B on  A.enterprise_id = B.ENTERPRISE_ID  ) t where a.cust_id = t.enterprise_id )
	"
	#��ý��ֵ
 	set RESULT_VAL [get_single $sql_buff]
 	
	#--��У��ֵ����У������
	set sql_buff "
		INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R193',$RESULT_VAL,0,0,0) 
		"
		
	exec_sql $sql_buff

 	#���Ϸ���: 0 - ������ ����0 - ������
	if {[format %.3f [expr ${RESULT_VAL} ]]>0 } {
		set grade 2
	        set alarmcontent " R193 У�鲻ͨ��"
	        puts ${alarmcontent}	        
	        WriteAlarm $app_name $op_time $grade ${alarmcontent}
		 }


#R194			����	��	02_���ſͻ�	���ſͻ���Ŀ���г��嵥�ͻ���Ӧ��ϵ�ӿ��е��嵥�ͻ�ID������Ŀ���г��嵥���ſͻ���	01007(���ſͻ���Ŀ���г��嵥�ͻ���Ӧ��ϵ)�е�"�嵥�ͻ�ID"����01006(Ŀ���г��嵥���ſͻ�)��"�嵥�ͻ�ID"��	0.05		

	#01006 ���ͬ01007 
 	set RESULT_VAL 0
	set sql_buff "
		select count(0) from 
		(select a.* , row_number()over(partition by ENTERPRISE_ID,CUST_ID order by time_id desc ) rn 
			from bass1.G_A_01007_DAY a 
		) a where a.rn = 1 
		and RELA_STATE = '1'
		and not exists (select 1 from (select distinct enterprise_id from bass1.G_A_01006_DAY ) t where a.enterprise_id = t.enterprise_id )
		with ur
		"
  #��ý��ֵ
 	set RESULT_VAL [get_single $sql_buff]
 	
	#--��У��ֵ����У������
	set sql_buff "
		INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R194',$RESULT_VAL,0,0,0) 
		"
	exec_sql $sql_buff

 	#���Ϸ���: 0 - ������ ����0 - ������
	if {[format %.3f [expr ${RESULT_VAL} ]]>0 } {
		set grade 2
	        set alarmcontent " R194 У�鲻ͨ��"
	        puts ${alarmcontent}
	        WriteAlarm $app_name $op_time $grade ${alarmcontent}
		 }
		 
	return 0
}

