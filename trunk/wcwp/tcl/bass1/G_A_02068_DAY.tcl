
######################################################################################################		
#�ӿ�����: �̻��ܼ��ն��û��󶨹�ϵ                                                               
#�ӿڱ��룺02068                                                                                          
#�ӿ�˵�������ӿ�Ϊ�������ӿڣ��״��ϱ�����״̬Ϊ������ȫ��������ϵ��
#��������: G_A_02068_DAY.tcl                                                                            
#��������: ����02068������
#��������: DAY
#Դ    ��1.
#�������: void
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ�panzw
#��дʱ�䣺20120801
#�����¼��
#�޸���ʷ: 1. panzw 20120801	�й��ƶ�һ����Ӫ����ϵͳʡ�����ݽӿڹ淶 (V1.8.2) 
#######################################################################################################   
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

        #���� yyyymmdd
        set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]

        #���� yyyy-mm-dd
        set optime $op_time

	set sql_buff "delete from BASS1.G_A_02068_DAY where TIME_ID = $timestamp"
	exec_sql $sql_buff


	return 0
}