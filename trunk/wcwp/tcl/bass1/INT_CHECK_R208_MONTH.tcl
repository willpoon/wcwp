######################################################################################################
#�������ƣ�	INT_CHECK_R208_MONTH.tcl
#У��ӿڣ�	03017 02054
#��������: MONTH
#�������: 
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ�PANZHIWEI
#��дʱ�䣺2011-05-26 
#�����¼��
#�޸���ʷ:  #���ֿھ����죬�����������Ƚ������0��ͬ�򣩵����⡣
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
        global app_name
        set app_name "INT_CHECK_R208_MONTH.tcl"


########################################################################################################3


#R208			����	��	02_���ſͻ�	�ǹ�ͨ���¼Ʒѿͻ���	�ǹ�ͨ���¼Ʒѿͻ����ܳǹ�ͨ����ʹ�ÿͻ���	0.05		



 	  set sqlbuf "delete from  BASS1.G_RULE_CHECK where time_id=$op_month and rule_code in ('R208') "        

	  exec_sql $sqlbuf


 	set RESULT_VAL 0
 	
	set sql_buff "
		select count(0) from   G_S_22303_MONTH where ENT_BUSI_ID = '1370'
		and time_id = $op_month
		and bigint(BILL_CUST_NUMS) >  bigint(USE_CUST_NUMS)
		and  MANAGE_MOD = '3'
	with ur
	"
 	set RESULT_VAL [get_single $sql_buff]
	#--��У��ֵ����У������
	set sql_buff "
		INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R208',$RESULT_VAL,0,0,0) 
		"
		
	exec_sql $sql_buff

 	#���Ϸ���: 0 - ������ ����0 - ������
	if {[format %.3f [expr ${RESULT_VAL} ]]>0 } {
		set grade 2
	        set alarmcontent " R208 У�鲻ͨ��"
	        puts ${alarmcontent}	        
	        WriteAlarm $app_name $op_time $grade ${alarmcontent}
		 }



#R210			����	��	02_���ſͻ�	����ͨ���¼Ʒѿͻ���	����ͨ���¼Ʒѿͻ����ܾ���ͨ����ʹ�ÿͻ���	0.05		



 	  set sqlbuf "delete from  BASS1.G_RULE_CHECK where time_id=$op_month and rule_code in ('R210') "        

	  exec_sql $sqlbuf


 	set RESULT_VAL 0
 	
	set sql_buff "
		select count(0) from   G_S_22303_MONTH where ENT_BUSI_ID = '1360'
		and time_id = $op_month
		and bigint(BILL_CUST_NUMS) >  bigint(USE_CUST_NUMS)
		and  MANAGE_MOD = '3'
	with ur
	"
 	set RESULT_VAL [get_single $sql_buff]
	#--��У��ֵ����У������
	set sql_buff "
		INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R210',$RESULT_VAL,0,0,0) 
		"
		
	exec_sql $sql_buff

 	#���Ϸ���: 0 - ������ ����0 - ������
	if {[format %.3f [expr ${RESULT_VAL} ]]>0 } {
		set grade 2
	        set alarmcontent " R210 У�鲻ͨ��"
	        puts ${alarmcontent}	        
	        WriteAlarm $app_name $op_time $grade ${alarmcontent}
		 }


#R212			����	��	02_���ſͻ�	УѶͨ��ADC�����¼Ʒѿͻ���	УѶͨ��ADC�����¼Ʒѿͻ�����УѶͨ��ADC������ʹ�ÿͻ���	0.05		



 	  set sqlbuf "delete from  BASS1.G_RULE_CHECK where time_id=$op_month and rule_code in ('R212') "        

	  exec_sql $sqlbuf


 	set RESULT_VAL 0
 	
	set sql_buff "
		select count(0) from   G_S_22303_MONTH where ENT_BUSI_ID = '1310'
		and time_id = $op_month
		and bigint(BILL_CUST_NUMS) >  bigint(USE_CUST_NUMS)
		and MANAGE_MOD = '2'
	with ur
	"
 	set RESULT_VAL [get_single $sql_buff]
	#--��У��ֵ����У������
	set sql_buff "
		INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R212',$RESULT_VAL,0,0,0) 
		"
		
	exec_sql $sql_buff

 	#���Ϸ���: 0 - ������ ����0 - ������
	if {[format %.3f [expr ${RESULT_VAL} ]]>0 } {
		set grade 2
	        set alarmcontent " R212 У�鲻ͨ��"
	        puts ${alarmcontent}	        
	        WriteAlarm $app_name $op_time $grade ${alarmcontent}
		 }


#R214			����	��	02_���ſͻ�	����ͨ���¼Ʒѿͻ���	����ͨ���¼Ʒѿͻ���������ͨ����ʹ�ÿͻ���	0.05		

 	  set sqlbuf "delete from  BASS1.G_RULE_CHECK where time_id=$op_month and rule_code in ('R214') "        

	  exec_sql $sqlbuf


 	set RESULT_VAL 0
 	
	set sql_buff "
		select count(0) from   G_S_22303_MONTH where ENT_BUSI_ID = '1380'
		and time_id = $op_month
		and bigint(BILL_CUST_NUMS) >  bigint(USE_CUST_NUMS)
		and MANAGE_MOD = '3'
	with ur
	"
 	set RESULT_VAL [get_single $sql_buff]
	#--��У��ֵ����У������
	set sql_buff "
		INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R214',$RESULT_VAL,0,0,0) 
		"
		
	exec_sql $sql_buff

 	#���Ϸ���: 0 - ������ ����0 - ������
	if {[format %.3f [expr ${RESULT_VAL} ]]>0 } {
		set grade 2
	        set alarmcontent " R214 У�鲻ͨ��"
	        puts ${alarmcontent}	        
	        WriteAlarm $app_name $op_time $grade ${alarmcontent}
		 }


	return 0
}
