######################################################################################################
#�������ƣ�	INT_CHECK_R290_DAY.tcl
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
        set app_name "INT_CHECK_R290_DAY.tcl"



###########################################################

 	  set sql_buff "delete from  BASS1.G_RULE_CHECK 
 	  				where time_id=$last_month and rule_code in (
					'R290' -- δУ��					
					)
			"

		exec_sql $sql_buff



##~   R290	��	02_���ſͻ�	�̻��ܼҶ������ն��������ڼ��ſͻ���ʶ�Ķ���һһ��Ӧ	"02054 ����ҵ�񶩹���ϵ
##~   02059 ����ҵ������û��󶨹�ϵ
##~   02065 �̻��ܼ�ҵ�񶩹���ϵ"	�����̻��ܼ�ҵ����ն����ڼ��ſͻ�Ҳ�����̻��ܼ�ҵ��Ķ�����ϵ��¼	0.05		"Step1.ͳ��02059������ҵ������û��󶨹�ϵ���ӿ��У���ֹͳ����ĩ����״̬����������ҵ��Ϊ���̻��ܼҡ���Ʒ�ķǿռ��ſͻ���ʶ���ϣ�
##~   Step2.ͳ��02054������ҵ�񶩹���ϵ����02065���̻��ܼ�ҵ�񶩹���ϵ���ӿ��ж���״̬����������ҵ��Ϊ���̻��ܼҡ���Ʒ�ļ��ſͻ���ʶ���ϣ�
##~   Step3.ͳ��Step1���ϲ���Step2���ϵļ��ſͻ���ʶ���������������0����Υ������"	

##~   �����ݣ��ݲ�У�飡





	return 0
}
