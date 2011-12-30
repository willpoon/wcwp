######################################################################################################
#�ӿ����ƣ��ʱ�
#�ӿڱ��룺03003
#�ӿ�˵�����ʱ����ʻ��ĸ��ֽ�����������֧�����Ϣ���ʱ����ʻ���ʶ���ʱ���ĿΪ��Ԫ�Ǽ��ʻ�����֧�����Ϣ��
#��������: G_I_03003_MONTH.tcl
#��������: ����03003������
#��������: ��
#Դ    ��1.bass2.dw_acct_book_yyyymm(�ʱ�)
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
	set sql_buff "delete from bass1.g_i_03003_month where time_id=$op_month"
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
              
              
        set handle [aidb_open $conn]
       	set sql_buff "insert into bass1.g_i_03003_month
                      select
                        $op_month
                        ,acct_id
                        ,coalesce(bass1.fn_get_all_dim('BASS_STD1_0056',char(book_item_id)),'99')
                        ,char(bigint(sum(amount)*100))
                      from 
                        bass2.dw_acct_book_$op_month
                      group by
                        acct_id
                        ,coalesce(bass1.fn_get_all_dim('BASS_STD1_0056',char(book_item_id)),'99') "
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