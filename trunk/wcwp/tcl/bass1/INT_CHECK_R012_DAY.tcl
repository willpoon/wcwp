######################################################################################################
#�������ƣ�	INT_CHECK_R012_DAY.tcl
#У��ӿڣ�	22038
#��������: DAY
#�������: 
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ�PANZHIWEI
#��дʱ�䣺2012-05-31
#�����¼��
#�޸���ʷ:  
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

        #���� yyyymmdd
        set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]      
				puts $timestamp
      set op_month [string range $optime_month 0 3][string range $optime_month 5 6]				
        #��Ȼ��
				puts $op_time 
        set curr_month [string range $op_time 0 3][string range $op_time 5 6]
				puts $curr_month
      #��Ȼ�� ���� 01 ��
      set last_month_first_day ${op_month}01
      set last_day [GetLastDay [string range $timestamp 0 7]]
        #������
        set app_name "INT_CHECK_R012_DAY.tcl"


  set sqlbuf "delete from  BASS1.G_RULE_CHECK where time_id=$timestamp and rule_code in ('R012') "        
  exec_sql $sqlbuf
########################################################################################################3
##~   --~   R012	��	01_���˿ͻ�	ͨ�ſͻ����ձ䶯��	22038 ��KPI��Ʒ��	ͨ�ſͻ����ձ䶯�� �� 10%


   set RESULT_VAL1 0
   set RESULT_VAL2 0
   set RESULT_VAL3 0
        set sql_buff "
				   
			select 
			 THIS_D_COMM_USERS
			,LAST_D_COMM_USERS
			,THIS_D_COMM_USERS*1.0/LAST_D_COMM_USERS - 1 RATE 
			from 
			(
				select
				sum(bigint( D_COMM_USERS ) ) THIS_D_COMM_USERS
				from G_S_22038_DAY where time_id = $timestamp
			) a
			,(
				select
				sum(bigint( D_COMM_USERS ) ) LAST_D_COMM_USERS
				from G_S_22038_DAY where time_id = $last_day
			) b where 1 = 1 
			with ur
        "
   set p_row [get_row $sql_buff]
   set RESULT_VAL1 [lindex $p_row 0]
   set RESULT_VAL2 [lindex $p_row 1]
   set RESULT_VAL3 [lindex $p_row 2]
        #set RESULT_VAL [get_single $sql_buff]
        #--��У��ֵ����У������
        set sql_buff "
                INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R012',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL3,0) 
                "
                exec_sql $sql_buff

set RESULT_VAL3 [format %.3f [expr abs(${RESULT_VAL3}) ]]

        #���Ϸ���: 0 - �������� ����0 - ����
        if {[format %.3f [expr ${RESULT_VAL3} ]] > 0.1 } {
                set grade 2
                set alarmcontent " R012 У�鲻ͨ��"
                puts ${alarmcontent}            
                WriteAlarm $app_name $op_time $grade ${alarmcontent}
                 }




	

	return 0
}

