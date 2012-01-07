######################################################################################################
#�������ƣ�	INT_CHECK_R186_DAY.tcl
#У��ӿڣ�	02054
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
        set app_name "INT_CHECK_R186_DAY.tcl"



########################################################################################################3

#R186			����	��	02_���ſͻ�	���ſͻ�ҵ�񶩹���ϵ�ӿ��еļ��ſͻ���ʶ�����ڼ��ſͻ�������	02054(���ſͻ�ҵ�񶩹���ϵ)�е�"���ſͻ���ʶ"����01004(���ſͻ�)��"���ſͻ���ʶ"��	0.05		

#���Ŷ���26�����֮ǰ�����оɵļ��ſͻ�ID�û����µļ��ſͻ�ID

  set sqlbuf "delete from  BASS1.G_RULE_CHECK where time_id=$timestamp and rule_code in ('R186') "        


	  exec_sql $sqlbuf

 	set RESULT_VAL 0
	set sql_buff "
			select count(0) from 
			(
			                select t.*
			                ,row_number()over(partition by t.enterprise_id ,ENTERPRISE_BUSI_TYPE order by time_id desc ) rn 
			                from 
			                G_A_02054_DAY  t
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
		INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R186',$RESULT_VAL,0,0,0) 
		"
	exec_sql $sql_buff

 	#���Ϸ���: 0 - ������ ����0 - ������
	if {[format %.3f [expr ${RESULT_VAL} ]]>0 } {
		set grade 2
	        set alarmcontent " R186 У�鲻ͨ��"
	        puts ${alarmcontent}
	        WriteAlarm $app_name $op_time $grade ${alarmcontent}
		 }
		 
	return 0
}

