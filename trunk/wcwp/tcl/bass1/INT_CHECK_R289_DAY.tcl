######################################################################################################
#�������ƣ�	INT_CHECK_R289_DAY.tcl
#У��ӿڣ�	
#��������: MONTH
#�������: 
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ�PANZHIWEI
#��дʱ�䣺2012-06-09 
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
        set curr_month 201206
        ##~   set curr_month [string range $op_time 0 3][string range $op_time 5 6]
				puts $curr_month
        set last_month 201205		
        ##~   set last_month [GetLastMonth [string range $op_month 0 5]]
		
        #������
        global app_name
        set app_name "INT_CHECK_R289_DAY.tcl"



###########################################################

 	  set sql_buff "delete from  BASS1.G_RULE_CHECK 
 	  				where time_id=$last_month and rule_code in (
					'R289' -- δУ��					
					)
			"

		exec_sql $sql_buff




##~   R289	��	02_���ſͻ�	�̻��ܼҼ��ſͻ���ҵ��ʽ��һһ��Ӧ	02065 �̻��ܼ�ҵ�񶩹���ϵ	02065�ӿ��У������̻��ܼ�ҵ��ļ��ſͻ�ֻ�ܶ�Ӧһ��ҵ��ʽ����������������	0.05		

##~   02065���̻��ܼ�ҵ�񶩹���ϵ���ӿ��У�ͳ�Ƽ��ſͻ���ʶ��Ӧȥ�صġ�ҵ��ʽ������������1�����ϵļ��ſͻ���Ӧ����ҵ��ʽ����Υ������



##~   G_A_02065_DAY


##~   --~   select count(0)
##~   --~   from
##~   --~   (
##~   --~   select a.*,row_number()over(partition by channel_id order by time_id desc ) rn 
##~   --~   from G_A_02065_DAY a
##~   --~   where time_id / 100 <= $curr_month
##~   --~   ) t where t.rn =1  and CHNL_STATE = '1'


##~   select count(0) from G_A_02065_DAY

##~   �����ݣ��ݲ�У�飡




	return 0
}
