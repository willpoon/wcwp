######################################################################################################
#�ӿ����ƣ�������������
#�ӿڱ��룺06019
#�ӿ�˵�����й��ƶ���Ϊ�����ĺ������İ�����1860��1861���Զ�����1259���Զ�����12580
#          ÿ��ʡ��ÿ������������һ��������
#��������: G_I_06019_MONTH.tcl
#��������: ����06019������
#��������: ��
#Դ    ��1.
#�������: 
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ�����
#��дʱ�䣺2007-03-22
#�����¼��1.��Ϊ���صĺ�����������Ŀǰû�нӿڣ����Գ������ֱ�Ӳ������ݵķ�����
#�޸���ʷ: 1.
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

        #���� yyyymm
        set op_month [string range $optime_month 0 3][string range $optime_month 5 6]          
      
        #ɾ����������
        set handle [aidb_open $conn]
	set sql_buff "delete from bass1.g_i_06019_month where time_id=$op_month"
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle


        set handle [aidb_open $conn]

	set sql_buff "insert into bass1.g_i_06019_month values
	           ($op_month,'10086','10086','10086','20000101','20300101','10086','1','01','13100'),
	           ($op_month,'10085','10085','10085','20000101','20300101','10085','1','01','13100'),
                   ($op_month,'12582','12582','12582','20000101','20300101','12582','1','01','13100'),
                   ($op_month,'12580','12580','12580','20000101','20300101','12580','0','01','13100')"
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle

	return 0
}
###########################�ο�######################################
#������ʶ  2M��·�м���	    ��ϯ��	����Ա��	
#10086	  ����8���м���	    20	        45	
#10085		            10	        4	
#12582		            1	        1	
#12580	  6�м���	    3	        3	
#-------
#ǰ��������8M��·��10086Ϊ�˹����Զ���ϣ�10085Ϊ�����12582Ϊũ��ͨ��12580Ϊ�˹���
#################################################################