######################################################################################################
#�������ƣ�	INT_CHECK_R023R024_DAY.tcl
#У��ӿڣ�	02004
#��������: MONTH
#�������: 
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ�PANZHIWEI
#��дʱ�䣺2011-12-26 
#�����¼��
#�޸���ʷ:  
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {
        #set op_time 2011-06-01
        #set optime_month 2011-05
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
        set app_name "INT_CHECK_R023R024_DAY.tcl"



###########################################################

 	  set sqlbuf "delete from  BASS1.G_RULE_CHECK 
 	  				where time_id=$timestamp and rule_code in ('R023NEW','R024NEW') 
 	  "        

	  exec_sql $sqlbuf



#R023	|(�������û�/��(�������û�����-1) x 100% | �� 3%	
#|(�������û�/��(�������û�����-1) x 100% | �� 3%

   set RESULT_VAL1 0
   set RESULT_VAL2 0
   set RESULT_VAL3 0

set sql_buff "
select
(
select count(distinct user_id) from  bass1.g_a_02004_02008_stage  
                        where bigint(CREATE_DATE)/100 = $curr_month
                          and test_flag='0'
			  ) MONTH_NEW
,(
select sum(bigint(TARGET2))
from bass1.g_rule_check
where 
rule_code='R159_1'
and time_id / 100  = $curr_month
)  MONTH_ALL_DAY_NEW
, ((
select count(distinct user_id) from  bass1.g_a_02004_02008_stage  
                        where bigint(CREATE_DATE)/100 = $curr_month
                          and test_flag='0'
			  ) *1.00/(
select sum(bigint(TARGET2))
from bass1.g_rule_check
where 
rule_code='R159_1'
and time_id / 100  = $curr_month
)-1)*100 PERCENT
from bass2.dual

"
   set p_row [get_row $sql_buff]
   set RESULT_VAL1 [lindex $p_row 0]
   set RESULT_VAL2 [lindex $p_row 1]
   set RESULT_VAL3 [lindex $p_row 2]

        #--��У��ֵ����У������
        set sql_buff "
                INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R023NEW',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL3,0) 
                "
                exec_sql $sql_buff
		
 # У��ֵ����ʱ�澯	
	if { $RESULT_VAL3 > 1.00||$RESULT_VAL3 < -1.00 } {
		set grade 2
	  set alarmcontent "R023 У�鲻ͨ��"
	  WriteAlarm $app_name $op_time $grade ${alarmcontent}
	}

      	

 #|(�������û�/��(�������û�����-1) x 100% | �� 5%



   set RESULT_VAL1 0
   set RESULT_VAL2 0
   set RESULT_VAL3 0

set sql_buff "
select
(
select count(distinct user_id) from  bass1.g_a_02004_02008_stage  
                        where USERSTATUS IN ('2010','2020','2030','9000')
                          and test_flag='0'
                          and time_id/100 = $curr_month
			  ) MONTH_OFF
,(
select sum(bigint(TARGET2))
from bass1.g_rule_check
where 
rule_code='R159_4'
and time_id / 100  = $curr_month
)  MONTH_ALL_DAY_OFF
, ((
select count(distinct user_id) from  bass1.g_a_02004_02008_stage  
                        where USERSTATUS IN ('2010','2020','2030','9000')
                          and test_flag='0'
                          and time_id/100 = $curr_month
			  ) *1.00/(
select sum(bigint(TARGET2))
from bass1.g_rule_check
where 
rule_code='R159_4'
and time_id / 100  = $curr_month
)-1)*100 PERCENT
from bass2.dual

"
   set p_row [get_row $sql_buff]
   set RESULT_VAL1 [lindex $p_row 0]
   set RESULT_VAL2 [lindex $p_row 1]
   set RESULT_VAL3 [lindex $p_row 2]

        #--��У��ֵ����У������
        set sql_buff "
                INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R024NEW',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL3,0) 
                "
                exec_sql $sql_buff
		
 # У��ֵ����ʱ�澯	
 #|(�������û�/��(�������û�����-1) x 100% | �� 5%
	if { $RESULT_VAL3 > 3.00||$RESULT_VAL3 < -3.00 } {
		set grade 2
	  set alarmcontent "R024 У�鲻ͨ��"
	  WriteAlarm $app_name $op_time $grade ${alarmcontent}
	}

      	
	return 0
}
