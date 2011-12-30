######################################################################################################
#�������ƣ�	INT_CHECK_R216_MONTH.tcl
#У��ӿڣ�	03017 02054
#��������: MONTH
#�������: 
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ�PANZHIWEI
#��дʱ�䣺2011-05-26 
#�����¼��
#�޸���ʷ:  #���ֿھ����죬�����������Ƚ������0��ͬ�򣩵����⡣
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
        set app_name "INT_CHECK_R216_MONTH.tcl"



###########################################################

#R216			����	��	02_���ſͻ�	�ƶ�400ʹ�ü��ſͻ���������ͬ��ƥ��	�ƶ�400ʹ�ü��ſͻ�����0 �� �ƶ�400���������룾0	0.05		
 	  set sqlbuf "delete from  BASS1.G_RULE_CHECK where time_id=$op_month and rule_code in ('R216') "        

	  exec_sql $sqlbuf

   set RESULT_VAL1 0
   set RESULT_VAL2 0
	set sql_buff "
				select a.cnt*1.00 ,value(b.income,0) income
				from
				(select count(0) cnt
							from 
							(
							select t.*,row_number()over(partition by ENTERPRISE_ID order by time_id desc) rn 
							from (
							select *
							from g_a_02054_day where 
							 time_id/100 <= $op_month and ENTERPRISE_BUSI_TYPE = '1520'
							 and MANAGE_MODE = '3'
							) t 
							) t2 where rn = 1 and STATUS_ID ='1'
				) a,(
				select sum(income)*1.00/100 income
				from (
				select sum(bigint(income)) income from   g_s_03017_month
				where time_id = $op_month
				and ent_busi_id = '1520'
				and MANAGE_MOD = '3'
				union all 
				select sum(bigint(income)) income from   g_s_03018_month
				where time_id = $op_month
				and ent_busi_id = '1520'
				and MANAGE_MOD = '3'
				) t 
				) b 
			with ur
	"
   set p_row [get_row $sql_buff]
   set RESULT_VAL1 [lindex $p_row 0]
   set RESULT_VAL2 [lindex $p_row 1]

 	#set RESULT_VAL [get_single $sql_buff]
	#--��У��ֵ����У������
	set sql_buff "
		INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R216',$RESULT_VAL1,$RESULT_VAL2,0,0) 
		"
		exec_sql $sql_buff
		
 	#���Ϸ���: 0 - �������� ����0 - ����
	if {[format %.3f [expr ${RESULT_VAL1} ]] <= 0 } {
		set grade 2
	        set alarmcontent " R216 У��1��ͨ��"
	        puts ${alarmcontent}	        
	        WriteAlarm $app_name $op_time $grade ${alarmcontent}
		 }


 	#���Ϸ���: 0 - �������� ����0 - ����
	if {[format %.3f [expr ${RESULT_VAL2} ]] <= 0 } {
		set grade 2
	        set alarmcontent " R216 У��2��ͨ��"
	        puts ${alarmcontent}	        
	        WriteAlarm $app_name $op_time $grade ${alarmcontent}
		 }

  
      	
	return 0
}
