######################################################################################################
#�ӿ����ƣ���������
#�ӿڱ��룺06016
#�ӿ�˵�������й��ƶ��������ʡ��˾����й�˾ǩ������Э�������ʵ�塣
#��������: G_I_06016_MONTH.tcl
#��������: ����06016������
#��������: ��
#Դ    ��1.bass2.dim_cust_bank(����ά��)
#�������: 
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ�����
#��дʱ�䣺2007-03-22
#�����¼��1.Ŀǰ����ά��ֻ������ýӿڵ�������ʶ/��������/���ڵ�����������ʶ�ֶΡ�
#�޸���ʷ: 1.
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

        #���� yyyymm
        set op_month [string range $optime_month 0 3][string range $optime_month 5 6]          
       
        #ɾ����������
        set handle [aidb_open $conn]
	set sql_buff "delete from bass1.g_i_06016_month where time_id=$op_month"
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle


        set handle [aidb_open $conn]
	set sql_buff "insert into bass1.g_i_06016_month
                        select
                          $op_month 
                          ,char(bank_id)
                          ,substr(bank_name,1,20)
                          ,' '
                          ,' '
                          ,'20000101'
                          ,'20300101'
                          ,char(int(city_id)+12210)
                          ,'13101'
                        from 
                          bass2.dim_cust_bank "
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