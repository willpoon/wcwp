######################################################################################################
#�������ƣ�	INT_CHECK_R263_DAY.tcl
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
        set app_name "INT_CHECK_R263_DAY.tcl"


  set sqlbuf "delete from  BASS1.G_RULE_CHECK where time_id=$timestamp and rule_code in ('R263') "        
  exec_sql $sqlbuf

########################################################################################################3

##~   --~   R263	��	03_������־	�굥������Ķ��żƷ�������ܽӿ��ϱ��Ķ��żƷ���֮���ƽ���ϵ	
##~   --~   "04005 �������Ż���
##~   --~   21007 ��ͨ����ҵ����ʹ��
##~   --~   22012 ��KPI"	ͨ�����굥������Ķ��żƷ�����22012�еĶ��żƷ���	0.05		ȡKPI���еĶ��żƷ�������ͳ���յ�22012�еĶ��żƷ����Ƚϣ�����ȣ���ͨ���ù���

   set RESULT_VAL1 0
   set RESULT_VAL2 0
   set RESULT_VAL3 0
        set sql_buff "
			select *
			from (
			select (
					select count(0) from   (
							select  PRODUCT_NO  from bass1.g_s_04005_day 
							where time_id =$timestamp
							and calltype_id in ('00','01','10','11')
							and sms_status='0'
					) a
			) + (
				select sum(bigint(SMS_COUNTS)) from bass1.g_s_21007_day
				where time_id =$timestamp
				  and SVC_TYPE_ID in ('11','12','13','70')
				  and END_STATUS='0'
				  and CDR_TYPE_ID in ('00','10','21')
			 ) from bass2.dual
			) a , ( 		
					select bigint(M_BILL_SMS) from G_S_22012_DAY where time_id = $timestamp
				) b where 1 = 1
        "
   set p_row [get_row $sql_buff]
   set RESULT_VAL1 [lindex $p_row 0]
   set RESULT_VAL2 [lindex $p_row 1]
##~   set RESULT_VAL3 [lindex $p_row 2]

set RESULT_VAL3 [format %.3f [expr ${RESULT_VAL1} - ${RESULT_VAL2} ]]


        #set RESULT_VAL [get_single $sql_buff]
        #--��У��ֵ����У������
        set sql_buff "
                INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R263',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL3,0) 
                "
                exec_sql $sql_buff


        #���Ϸ���: 0 - �������� ����0 - ����
        if {[format %.3f [expr ${RESULT_VAL3} ]] != 0 } {
                set grade 2
                set alarmcontent " R263 У�鲻ͨ��"
                puts ${alarmcontent}            
                WriteAlarm $app_name $op_time $grade ${alarmcontent}
                 }






########################################################################################################3

##~   --~   R264	��	03_������־	�굥���������ҵ���ض��żƷ�������ܽӿ��ϱ�����ҵ���ض��żƷ���֮���ƽ���ϵ	"04016 ��ҵ���ض��Ż���
##~   --~   22012 ��KPI"	ͨ�����굥���������ҵ���żƷ�����22012�е���ҵ���żƷ���	0.05		"��һ��������04016���������Ϊͳ����,���Ż������ͱ������ڣ�00��SMO��,01��SMT��, 10��SMO_F��,11 ��SMT_F�������ҷ���״̬Ϊ�ɹ��ļ�¼������Ϊ���յ���ҵ���żƷ�����
##~   --~   �ڶ�����ȡ22012���������Ϊͳ���յ���ҵ���żƷ����������һ���Ľ�����бȶԡ�"	


   set RESULT_VAL1 0
   set RESULT_VAL2 0
   set RESULT_VAL3 0
        set sql_buff "
			select *
			from (
			select count(0) from    G_S_04016_DAY where time_id = $timestamp
				and RECORD_TYPE in ('00','01','10','11')
				and SEND_STATUS = '0'
			) a , ( 		
					select bigint(M_BILL_HANGYE_SMS) from G_S_22012_DAY where time_id = $timestamp
				) b where 1 = 1
				with ur
        "
   set p_row [get_row $sql_buff]
   set RESULT_VAL1 [lindex $p_row 0]
   set RESULT_VAL2 [lindex $p_row 1]
##~   set RESULT_VAL3 [lindex $p_row 2]

set RESULT_VAL3 [format %.3f [expr ${RESULT_VAL1} - ${RESULT_VAL2} ]]


        #set RESULT_VAL [get_single $sql_buff]
        #--��У��ֵ����У������
        set sql_buff "
                INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R264',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL3,0) 
                "
                exec_sql $sql_buff


        #���Ϸ���: 0 - �������� ����0 - ����
        if {[format %.3f [expr ${RESULT_VAL3} ]] != 0 } {
                set grade 2
                set alarmcontent " R264 У�鲻ͨ��"
                puts ${alarmcontent}            
                WriteAlarm $app_name $op_time $grade ${alarmcontent}
                 }

	return 0
}

