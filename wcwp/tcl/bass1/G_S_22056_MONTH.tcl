######################################################################################################
#�ӿ����ƣ��������������������
#�ӿڱ��룺22056
#�ӿ�˵������¼�������ֵ�������Ϣ��
#��������: G_S_22056_MONTH.tcl
#��������: ����22056������
#��������: ��
#Դ    ��
#�������: 
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ�liuqf
#��дʱ�䣺2011-01-17
#�����¼��
#�޸���ʷ: 
#######################################################################################################


proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

	#���� yyyymmdd
        set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
        #���� yyyy-mm-dd
        set optime $op_time
        
        #���� yyyymm
        set op_month [string range $optime_month 0 3][string range $optime_month 5 6]          
        puts $op_month

				#����  yyyymm
				set last_month [GetLastMonth [string range $op_month 0 5]]                      
				puts $last_month

        #���µ�һ�� yyyy-mm-dd
        set this_month_first_day [string range $op_month 0 3][string range $op_time 4 4][string range $op_month 4 5][string range $op_time 4 4]01
        puts $this_month_first_day

        #�������һ�� yyyy-mm-dd
            #�������һ�� yyyy-mm-dd    set this_month_last_day [string range $op_month 0 3][string range $op_month 4 5][GetThisMonthDays [string range $op_month 0 5]01]    puts $this_month_last_day
        #ɾ����������
	set sql_buff "delete from bass1.g_s_22056_month where time_id=$op_month"
	puts $sql_buff
  exec_sql $sql_buff
       

  #����  
set sql_buff "insert into bass1.g_s_22056_month				select 	$op_month	,'$op_month'	,coalesce(bass1.fn_get_all_dim('BASS_STD1_0054',char(substr(REGION_CODE,2))),'13101') 	,case when GEOGRAPHY_TYPE in (1,2,3) then '1'		when GEOGRAPHY_TYPE in (4) then '2'		else '3' end GEOGRAPHY_TYPE	,case when operate_type in (0) then '2' 	when operate_type in (1) then '1'	end operate_type	,char(count(distinct OPPONENT_ID))from bass2.dw_opponent_base_info_$this_month_last_daywhere operate_type in (0,1)group by coalesce(bass1.fn_get_all_dim('BASS_STD1_0054',char(substr(REGION_CODE,2))),'13101'),case when GEOGRAPHY_TYPE in (1,2,3) then '1'	when GEOGRAPHY_TYPE in (4) then '2'	else '3' end ,case when operate_type in (0) then '2' 	when operate_type in (1) then '1'	endwith ur
"

  exec_sql $sql_buff
  
 
	return 0
}


