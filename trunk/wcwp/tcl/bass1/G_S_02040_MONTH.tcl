
######################################################################################################		
#�ӿ�����: �쳣�û�                                                               
#�ӿڱ��룺02040                                                                                          
#�ӿ�˵����ʡ�ݸ����쳣�ͻ�ʶ��ģ����ʶ������쳣�û���Ϣ��
#��������: G_S_02040_MONTH.tcl                                                                            
#��������: ����02040������
#��������: MONTH
#Դ    ��1.
#�������: void
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ�panzw
#��дʱ�䣺20120801
#�����¼��
#�޸���ʷ: 1. panzw 20120801	�й��ƶ�һ����Ӫ����ϵͳʡ�����ݽӿڹ淶 (V1.8.2) 
#######################################################################################################   
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

      set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]      
      set op_month [string range $optime_month 0 3][string range $optime_month 5 6]
      puts $op_month
      set this_month_last_day [string range $op_month 0 5][GetThisMonthDays [string range $op_month 0 5]01] 
      set last_month [GetLastMonth [string range $op_month 0 5]]
      #set curr_month_first_day [string range $timestamp 0 5]01
      #puts $curr_month_first_day
      #yyyy--mm-dd
      set ThisMonthFirstDay [string range $op_month 0 3][string range $op_time 4 4][string range $op_month 4 5][string range $op_time 4 4]01
      puts $ThisMonthFirstDay      

        global app_name
        set app_name "G_S_02040_MONTH.tcl"
    
	return 0
}


