######################################################################################################
#�������ƣ�	INT_CHECK_R255_MONTH.tcl
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
        set app_name "INT_CHECK_R255_MONTH.tcl"



###########################################################

 	  set sqlbuf "delete from  BASS1.G_RULE_CHECK 
 	  				where time_id=$op_month and rule_code in ('R255') 
 	  "        

	  exec_sql $sqlbuf



#60% �ܣ�ʵ�������ź���/���������ͻ�������100% �� 100%

   set RESULT_VAL1 0
   set RESULT_VAL2 0
   set RESULT_VAL3 0

set sql_buff "
select b.chnl_new_cnt,a.all_cnt , b.chnl_new_cnt*1.00/a.all_cnt
from (
select count(distinct user_id) all_cnt from    bass1.int_02004_02008_month_stage 
where bigint(create_date)/100 = $op_month
) a ,
(
select sum(bigint(NEW_USER_CNT)) chnl_new_cnt 
from G_S_22091_DAY
where time_id / 100 = $op_month
) b where 1 = 1

"
   set p_row [get_row $sql_buff]
   set RESULT_VAL1 [lindex $p_row 0]
   set RESULT_VAL2 [lindex $p_row 1]
   set RESULT_VAL3 [lindex $p_row 2]

        #--��У��ֵ����У������
        set sql_buff "
                INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R255',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL3,0) 
                "
                exec_sql $sql_buff
		
 # У��ֵ����ʱ�澯	
	if { $RESULT_VAL3 >= 1.00||$RESULT_VAL3 < 0.60 } {
		set grade 2
	  set alarmcontent "R255 У�鲻ͨ��"
	  WriteAlarm $app_name $op_time $grade ${alarmcontent}
	}

      	
	return 0
}
