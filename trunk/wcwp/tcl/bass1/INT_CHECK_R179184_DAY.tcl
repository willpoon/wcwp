######################################################################################################
#�������ƣ�	INT_CHECK_R179184_DAY.tcl
#У��ӿڣ�	02061
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
        set app_name "INT_CHECK_R179184_DAY.tcl"



########################################################################################################3
	# R179			����	��	02_���ſͻ�	�ֻ���������û��󶨹�ϵ�ӿ��е��û���ʶ�������û�����	"02004 �û�
  #02061 �ֻ���������û��󶨹�ϵ"	02061(�ֻ���������û��󶨹�ϵ)�е�"�û���ʶ"����02004(�û�)��"�û���ʶ"��	0.05	
#���Ŷ���26�����֮ǰ�����оɵļ��ſͻ�ID�û����µļ��ſͻ�ID��
#���Ŷ���26�����֮ǰ�����е�����������Ϊ24�յ�ʧЧ�������ϴ���ȫ���������



 	  set sqlbuf "delete from  BASS1.G_RULE_CHECK where time_id=$timestamp and rule_code in ('R179','R184') "        

	  exec_sql $sqlbuf

#ȡ��Ӻ������
	set sql_buff "
        select count(0) from 
        (
                        select t.*
                        ,row_number()over(partition by t.user_id order by time_id desc ) rn 
                        from  G_A_02061_DAY  t  where time_id >= 20100624
          ) a
        where rn = 1	and 	STATUS_ID = '1'
    		and not exists (select 1 from (select distinct user_id from bass1.g_a_02004_day where time_id >= 20100626 ) t where a.user_id = t.user_id )
		with ur	
	"
	#��ý��ֵ
 	set RESULT_VAL [get_single $sql_buff]
 	
	#--��У��ֵ����У������
	set sql_buff "
		INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R179',$RESULT_VAL,0,0,0) 
		"
		
	exec_sql $sql_buff

 	#���Ϸ���: 0 - ������ ����0 - ������
	if {[format %.3f [expr ${RESULT_VAL} ]]>0 } {
		set grade 2
	        set alarmcontent " R179 У�鲻ͨ��"
	        puts ${alarmcontent}	        
	        WriteAlarm $app_name $op_time $grade ${alarmcontent}
		 }

#R184			����	��	02_���ſͻ�	�ֻ���������û��󶨹�ϵ�ӿ��еļ��ſͻ���ʶ�����ڼ��ſͻ�������	"01004 ���ſͻ�
#02061 �ֻ���������û��󶨹�ϵ"	02061(�ֻ���������û��󶨹�ϵ)�е�"���ſͻ���ʶ"����01004(���ſͻ�)��"���ſͻ���ʶ"��	0.05	

 	set RESULT_VAL 0
	set sql_buff "
			select count(0) from 
			(
			                select t.*
			                ,row_number()over(partition by t.enterprise_id order by time_id desc ) rn 
			                from 
			                G_A_02061_DAY  t
											where time_id >= 20100624
			  ) a
			where rn = 1	and STATUS_ID = '1'
			and not exists (select 1 from (select distinct value(b.NEW_ENTERPRISE_ID,a.enterprise_id) enterprise_id from bass1.G_A_01004_DAY a 
LEFT JOIN bass1.dim_trans_enterprise_id B on  A.enterprise_id = B.ENTERPRISE_ID  ) t where a.enterprise_id = t.enterprise_id )
		with ur		
		"
  #��ý��ֵ
 	set RESULT_VAL [get_single $sql_buff]
 	
	#--��У��ֵ����У������
	set sql_buff "
		INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R184',$RESULT_VAL,0,0,0) 
		"
	exec_sql $sql_buff

 	#���Ϸ���: 0 - ������ ����0 - ������
	if {[format %.3f [expr ${RESULT_VAL} ]]>0 } {
		set grade 2
	        set alarmcontent " R184 У�鲻ͨ��"
	        puts ${alarmcontent}
	        WriteAlarm $app_name $op_time $grade ${alarmcontent}
		 }
		 
	return 0
}

