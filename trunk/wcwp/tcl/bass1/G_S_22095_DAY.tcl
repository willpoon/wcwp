
######################################################################################################		
#�ӿ�����: ���������ն�������Ϣ                                                               
#�ӿڱ��룺22095                                                                                          
#�ӿ�˵������¼���ա��㶫��������֧����Ӧʡ���ն�����ͳ����Ϣ
#��������: G_S_22095_DAY.tcl                                                                            
#��������: ����22095������
#��������: DAY
#Դ    ��1.
#�������: void
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ�panzw
#��дʱ�䣺20120507
#�����¼��
#�޸���ʷ: 1. panzw 20120507	�й��ƶ�һ����Ӫ����ϵͳʡ�����ݽӿڹ淶 (V1.8.0) 
##~   2�����ӿڽ����ա��㶫ʡ���ϴ���
#######################################################################################################   
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {
      set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
      puts $timestamp
    #���� yyyymm
    set op_month [string range $optime_month 0 3][string range $optime_month 5 6]
    puts $op_month
      
    #�ϸ��� yyyymm
    set last_month [GetLastMonth [string range $op_month 0 5]]
    puts $last_month

        #������
        global app_name
        set app_name "G_S_22095_DAY.tcl"
	
  #ɾ����������
	set sql_buff "delete from bass1.G_S_22095_DAY where time_id=$timestamp"
	exec_sql $sql_buff

	return 0
}

