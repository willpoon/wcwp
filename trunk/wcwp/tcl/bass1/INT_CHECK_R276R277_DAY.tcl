######################################################################################################
#�������ƣ�	INT_CHECK_R276R277_DAY.tcl
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
        set app_name "INT_CHECK_R276R277_DAY.tcl"


  set sqlbuf "delete from  BASS1.G_RULE_CHECK where time_id=$timestamp and rule_code in ('R276','R277') "        
  exec_sql $sqlbuf

########################################################################################################3
##~   --~   R276	��	09_������Ӫ	����Ӫҵ���ź���	22066 ���������ؼ������ջ���	����Ӫҵ���ķź��� = 0	0.05	


   set RESULT_VAL1 0
   set RESULT_VAL2 0
   set RESULT_VAL3 0
        set sql_buff "
			
			select sum(bigint(ACTIVATE_CNT))
			from G_S_22066_DAY 
			where E_CHANNEL_TYPE = '03'
			and time_id = $timestamp
			with ur
        "
   set p_row [get_row $sql_buff]
   set RESULT_VAL1 [lindex $p_row 0]
   ##~   set RESULT_VAL2 [lindex $p_row 1]
##~   set RESULT_VAL3 [lindex $p_row 2]

##~   set RESULT_VAL3 [format %.3f [expr ${RESULT_VAL1} - ${RESULT_VAL2} ]]


        #set RESULT_VAL [get_single $sql_buff]
        #--��У��ֵ����У������
        set sql_buff "
                INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R276',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL3,0) 
                "
                exec_sql $sql_buff


        #���Ϸ���: 0 - �������� ����0 - ����
        if {[format %.3f [expr ${RESULT_VAL1} ]] != 0 } {
                set grade 2
                set alarmcontent " R276 У�鲻ͨ��"
                puts ${alarmcontent}            
                WriteAlarm $app_name $op_time $grade ${alarmcontent}
                 }






########################################################################################################3

##~   --~   R277	��	09_������Ӫ	����Ӫҵ�������ն�������	22066 ���������ؼ������ջ���	����Ӫҵ���Ķ����ն������� = 0	0.05	


   set RESULT_VAL1 0
   set RESULT_VAL2 0
   set RESULT_VAL3 0
        set sql_buff "
			select sum(bigint(CONTRACT_DZ_SALE_ALLCNT))
			from G_S_22066_DAY 
			where E_CHANNEL_TYPE = '03'
			and time_id = $timestamp
				with ur
        "
   set p_row [get_row $sql_buff]
   set RESULT_VAL1 [lindex $p_row 0]
   ##~   set RESULT_VAL2 [lindex $p_row 1]
##~   set RESULT_VAL3 [lindex $p_row 2]

##~   set RESULT_VAL3 [format %.3f [expr ${RESULT_VAL1} - ${RESULT_VAL2} ]]


        #set RESULT_VAL [get_single $sql_buff]
        #--��У��ֵ����У������
        set sql_buff "
                INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R277',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL3,0) 
                "
                exec_sql $sql_buff


        #���Ϸ���: 0 - �������� ����0 - ����
        if {[format %.3f [expr ${RESULT_VAL3} ]] != 0 } {
                set grade 2
                set alarmcontent " R277 У�鲻ͨ��"
                puts ${alarmcontent}            
                WriteAlarm $app_name $op_time $grade ${alarmcontent}
                 }

	return 0
}

