######################################################################################################
#�������ƣ�	INT_CHECK_R192_MONTH.tcl
#У��ӿڣ�	03017
#��������: MONTH
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
      set op_month [string range $optime_month 0 3][string range $optime_month 5 6]
      puts $op_month				
        #��Ȼ��
				puts $op_time 
        set curr_month [string range $op_time 0 3][string range $op_time 5 6]
				puts $curr_month
        
        #������
        set app_name "INT_CHECK_R192_MONTH.tcl"


########################################################################################################3
#R192			����	��	02_���ſͻ�	���ſͻ�ͳ������ӿ��еļ��ſͻ���ʶ�����ڼ��ſͻ�������	03017(���ſͻ�ͳ������)�е�"���ſͻ���ʶ"����01004(���ſͻ�)��"���ſͻ���ʶ"��	0.05		

 	  set sqlbuf "delete from  BASS1.G_RULE_CHECK where time_id=$op_month and rule_code in ('R192') "        

	  exec_sql $sqlbuf


	set sql_buff "
	select count(0) from bass1.G_S_03017_MONTH a 
	where 
		time_id = $op_month
		and not exists (select 1 from (select distinct enterprise_id from bass1.G_A_01004_DAY where time_id / 100 <= $op_month ) t where a.enterprise_id = t.enterprise_id )
	with ur
	"
	#��ý��ֵ
 	set RESULT_VAL [get_single $sql_buff]
	#--��У��ֵ����У������
	set sql_buff "
		INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R192',$RESULT_VAL,0,0,0) 
		"
		
	exec_sql $sql_buff

 	#���Ϸ���: 0 - ������ ����0 - ������
	if {[format %.3f [expr ${RESULT_VAL} ]]>0 } {
		set grade 2
	        set alarmcontent " R192 У�鲻ͨ��"
	        puts ${alarmcontent}	        
	        WriteAlarm $app_name $op_time $grade ${alarmcontent}
		 }

	return 0
}
