######################################################################################################
#�ӿ����ƣ�������������������ҵ��
#�ӿڱ��룺06024
#�ӿ�˵������¼��������������������ҵ�����ࡣ
#��������: G_I_06024_MONTH.tcl
#��������: ����06022������
#��������: ��
#Դ    ��1.bass2.dim_pub_channel(����ά��)
#�������: 
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ�����
#��дʱ�䣺2007-03-22
#�����¼��1.Ŀǰ��������BOSSû�д���ҵ�����ͣ�����ͳһ��"99:����"��
#�޸���ʷ: 1.
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

        #���� yyyymm
        set op_month [string range $optime_month 0 3][string range $optime_month 5 6]          
      
        #ɾ����������
        set handle [aidb_open $conn]
	set sql_buff "delete from bass1.g_i_06024_month where time_id=$op_month"
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle


        set handle [aidb_open $conn]
	set sql_buff "insert into bass1.g_i_06024_month
                          select
                            $op_month
                            ,'03'
                            ,char(channel_id)
                          from bass2.dim_pub_channel
                          where sts=1 
                            and channeltype_id in (6,7,8)"
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