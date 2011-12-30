######################################################################################################
#�ӿ����ƣ��ʻ�
#�ӿڱ��룺03001
#�ӿ�˵�����ʻ����û�ʹ���ƶ�����ĸ���ʵ�塣
#          1. �ʻ����û��ɷѡ������ۺ��ʵ�����С����Ԫ������Ψһ��ʶ�����ʻ���ʶ��
#          2. һ���ʻ�����Ϊ����û���Ӧ����Ŀ���ѣ�
#          3. һ���û���Ӧ��������Ŀ�����ɶ���ʻ����ѡ�
#��������: G_I_03001_MONTH.tcl
#��������: ����03001������
#��������: ��
#Դ    ��1.bass2.dw_acct_msg_yyyymm(�ʻ�)
#�������: 
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ�����
#��дʱ�䣺2007-03-22
#�����¼��1.
#�޸���ʷ: 1.
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

        #���� yyyymm
        set op_month [string range $optime_month 0 3][string range $optime_month 5 6]          
   
        #ɾ����������
        set handle [aidb_open $conn]
	set sql_buff "delete from bass1.g_i_03001_month where time_id=$op_month"
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
            
       
        set handle [aidb_open $conn]
	set sql_buff "insert into bass1.g_i_03001_month
                      select
                        $op_month
                        ,acct_id
                        ,value(acct_name,'δ֪')
                        ,coalesce(bass1.fn_get_all_dim('BASS_STD1_0059',char(sts_id)),'9')
                        ,replace(char(date(create_date)),'-','')
                        ,cust_id
                        ,coalesce(bass1.fn_get_all_dim('BASS_STD1_0025',char(pay_type)),'01')
                        ,coalesce(bass1.fn_get_all_dim('BASS_STD1_0057',char(post_type)),'9')
                      from 
                        bass2.dw_acct_msg_$op_month "
        puts $sql_buff
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