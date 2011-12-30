######################################################################################################
#�ӿ����ƣ��ͷ��º�����
#�ӿڱ��룺22023
#�ӿ�˵������¼�ͷ��º����������Ϣ��
#��������: G_S_22023_MONTH.tcl
#��������: ����22023������
#��������: ��
#Դ    ��1.bass2.dw_custsvc_manual_inbound_dm_yyyymm(�˹�����ͳ�Ʊ�)
#          2.bass2.dw_custsvc_inbound_dm_yyyymm(ϵͳ����ͳ�Ʊ�)
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
	set sql_buff "delete from bass1.g_s_22023_month where time_id=$op_month"
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
       
       
       
       
        set handle [aidb_open $conn]
	set sql_buff "insert into bass1.g_s_22023_month
                      select
                        ${op_month}
                        ,'${op_month}'
                        ,'00040001'
                        ,char(int(sum(call_count)))
                      from 
                        bass2.dw_custsvc_manual_inbound_dm_$op_month 
                      union all
                      select
                        ${op_month}
                        ,'${op_month}'
                        ,'00040002'
                        ,char(int(sum(call_count)))
                      from
                        bass2.dw_custsvc_inbound_dm_$op_month "
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