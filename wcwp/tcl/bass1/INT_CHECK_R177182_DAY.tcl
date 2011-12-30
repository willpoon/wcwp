######################################################################################################
#�������ƣ�INT_CHECK_R177182_DAY.tcl
#У��ӿڣ�02059
#��������: DAY
#�������: 
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ�PANZHIWEI
#��дʱ�䣺2011-05-26 
#�����¼��
#�޸���ʷ:  add:02059(����ҵ������û��󶨹�ϵ)�е�"�û���ʶ"����02004(�û�)��"�û���ʶ"��
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
        set app_name "INT_CHECK_R177182_DAY.tcl"



########################################################################################################3
	# R177 02059(����ҵ������û��󶨹�ϵ)�е�"�û���ʶ"����02004(�û�)��"�û���ʶ"��


 	  set sqlbuf "delete from  BASS1.G_RULE_CHECK where time_id=$timestamp and rule_code in ('R177','R182') "        

	  exec_sql $sqlbuf


	set sql_buff "
        select count(0) from 
        (
                        select t.*
                        ,row_number()over(partition by t.user_id order by time_id desc ) rn 
                        from  G_A_02059_DAY  t
                        where time_id >= 20100626
          ) a
        where rn = 1	and 	STATUS_ID = '1'
    		and not exists (select 1 from (select distinct user_id from bass1.g_a_02004_day where time_id >= 20100626 ) t where a.user_id = t.user_id )
		with ur
	"
	#��ý��ֵ
 	set RESULT_VAL [get_single $sql_buff]
 	
	#--��У��ֵ����У������
	set sql_buff "
		INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R177',$RESULT_VAL,0,0,0) 
		"
		
	exec_sql $sql_buff

 	#���Ϸ���: 0 - ������ ����0 - ������
	if {[format %.3f [expr ${RESULT_VAL} ]]>0 } {
		set grade 2
	        set alarmcontent " R177 У�鲻ͨ��"
	        puts ${alarmcontent}	        
	        WriteAlarm $app_name $op_time $grade ${alarmcontent}
		 }

	# R182 02059(����ҵ������û��󶨹�ϵ)�зǿյ�"���ſͻ���ʶ"����01004(���ſͻ�)��"���ſͻ���ʶ"��
#���Ŷ���26�����֮ǰ�����оɵļ��ſͻ�ID�û����µļ��ſͻ�ID��
#���Ŷ���26�����֮ǰ�����е�����������Ϊ24�յ�ʧЧ�������ϴ���ȫ���������

 	set RESULT_VAL 0
	set sql_buff "
			select count(0) from 
			(
			                select t.*
			                ,row_number()over(partition by t.enterprise_id order by time_id desc ) rn 
			                from 
			                G_A_02059_DAY  t
											where time_id >= 20100626
			  ) a
			where rn = 1	and STATUS_ID = '1'
			AND trim(A.enterprise_id) <> ''
			and not exists (select 1 from (select distinct value(b.NEW_ENTERPRISE_ID,a.enterprise_id) enterprise_id from bass1.G_A_01004_DAY a 
LEFT JOIN BASS2.TRANS_ENTERPRISE_ID_20100625 B on  A.enterprise_id = B.ENTERPRISE_ID  ) t where a.enterprise_id = t.enterprise_id )
		with ur
		"
  #��ý��ֵ
 	set RESULT_VAL [get_single $sql_buff]
 	
	#--��У��ֵ����У������
	set sql_buff "
		INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R182',$RESULT_VAL,0,0,0) 
		"
		
	exec_sql $sql_buff
 	
 	#���Ϸ���: 0 - ������ ����0 - ������
	if {[format %.3f [expr ${RESULT_VAL} ]]>0 } {
		set grade 2
	        set alarmcontent " R182 У�鲻ͨ��"
	        puts ${alarmcontent}
	        WriteAlarm $app_name $op_time $grade ${alarmcontent}
		 }
		 
	return 0
}

