######################################################################################################
#�������ƣ�	INT_CHECK_R229_MONTH.tcl
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
        set app_name "INT_CHECK_R229_MONTH.tcl"



###########################################################

#R229			����	��	04_TDҵ��	�������ܼ������"ͨ���ͻ���"���û�ʹ���ն�ͨ������е�ͨ���û�����ƫ��	02047�е�ͨ���û�����ͨ���������ܼ������ͨ���ͻ�����ƫ����3%����	0.05		

 	  set sqlbuf "delete from  BASS1.G_RULE_CHECK 
 	  		where time_id=$op_month 
 	  		and rule_code in ('R229') 
 	  		"        
	  exec_sql $sqlbuf

   set RESULT_VAL1 0
   set RESULT_VAL2 0
   set RESULT_VAL  0
	set sql_buff "
	select a.val1*1.0000 , b.val2*1.0000
				from 
				(
				select count(distinct product_no ) val1 
				from     bass1.G_S_02047_MONTH
				where time_id = $op_month

			 ) a,
				(
                    select count(0) val2 from  
											(
											select   distinct product_no from        G_S_21003_MONTH where time_id = $op_month
											union 
											select   distinct product_no from        G_S_21006_MONTH where time_id = $op_month
											union 
											select   distinct product_no from        G_S_21009_DAY where time_id/100 = $op_month
											) a
													 
				) b
with ur  
	"
	
   set p_row [get_row $sql_buff]
   set RESULT_VAL1 [lindex $p_row 0]
   set RESULT_VAL2 [lindex $p_row 1]
	 set RESULT_VAL [expr $RESULT_VAL1/$RESULT_VAL2 - 1.0000 ]
	 puts $RESULT_VAL

 	#���Ϸ���: 0 - �������� ����0 - ����
 # У��ֵ����ʱ�澯	
	if { $RESULT_VAL>0.03||$RESULT_VAL<-0.03 } {
		set grade 2
	  set alarmcontent "R229 У�鲻ͨ��"
	  WriteAlarm $app_name $op_time $grade ${alarmcontent}
	}

	set sql_buff "
		INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R229',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL,0) 
		"
		exec_sql $sql_buff

  	
	return 0
}
