######################################################################################################
#�������ƣ�	INT_CHECK_R256_MONTH.tcl
#У��ӿڣ�	03004
#��������: MONTH
#�������: 
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ�PANZHIWEI
#��дʱ�䣺2011-05-26 
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
        set app_name "INT_CHECK_R256_MONTH.tcl"



###########################################################

 	  set sqlbuf "delete from  BASS1.G_RULE_CHECK 
 	  				where time_id=$op_month and rule_code in ('R256') 
 	  "        

	  exec_sql $sqlbuf



#��������ʶ�������û�ռ�� �� 85%

   set RESULT_VAL1 0
   set RESULT_VAL2 0
   set RESULT_VAL3 0

set sql_buff "

select no_chnl_cnt,all_cnt,no_chnl_cnt*1.00/all_cnt
from (
	select  count(0) no_chnl_cnt
	from bass1.g_a_02004_02008_stage 
	where bigint(create_date)/100 = $op_month
	and CHANNEL_ID not in ('BASS1_WB','BASS1_HL','BASS1_SM','BASS1_WP','BASS1_ST','BASS1_DS')
	and CHANNEL_ID not in (
	select CHANNEL_ID from   bass1.G_I_06021_MONTH
	where time_id = $op_month
	and CHANNEL_STATUS ='1'
		)
) a
,(
	select count(distinct user_id) all_cnt from    bass1.g_a_02004_02008_stage 
	where bigint(create_date)/100 = $op_month
) b where 1 =1 


"
   set p_row [get_row $sql_buff]
   set RESULT_VAL1 [lindex $p_row 0]
   set RESULT_VAL2 [lindex $p_row 1]
   set RESULT_VAL3 [lindex $p_row 2]

        #--��У��ֵ����У������
        set sql_buff "
                INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R256',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL3,0) 
                "
                exec_sql $sql_buff
		
 # У��ֵ����ʱ�澯	
	if { $RESULT_VAL3 > 0.15 } {
		set grade 2
	  set alarmcontent "R256 У�鲻ͨ��"
	  WriteAlarm $app_name $op_time $grade ${alarmcontent}
	}

      	
	return 0
}
