######################################################################################################
#�ӿ����ƣ��ͷ�����ѯ��
#�ӿڱ��룺22024
#�ӿ�˵������¼�ͷ�����ѯ���������Ϣ��
#��������: G_S_22024_MONTH.tcl
#��������: ����22024������
#��������: ��
#Դ    ��1bass2.dw_custsvc_agent_tele_dm_yyyymm(��ϯ����ͨ����־��)
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
	set sql_buff "delete from bass1.g_s_22024_month where time_id=$op_month"
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
       
      
       
        set handle [aidb_open $conn]
	set sql_buff "insert into bass1.g_s_22024_month
	     select
               ${op_month}
               ,'${op_month}'
               ,coalesce(bass1.fn_get_all_dim('BASS_STD1_0055',char(user_brand)),'2')
               ,coalesce(bass1.fn_get_all_dim('BASS_STD1_0096',char(call_purpose)),'900000') 
               ,'1'
               ,char(count(*))
             from 
               bass2.dw_custsvc_agent_tele_dm_$op_month 
             where 
                call_purpose in (201,201101,201102,201103,201104,2012,201201,201202,201203,201204,201205,
                   201206,201207,201208,201209,201210,201211,201212,201213,201214,202,2021,202101,202102,
                   202103,202104,202105,202106,202107,202108,202109,202110,202111,202112,202113,2022,20221,
                   202211,202212,202213,20222,202221,202222,202223,20223,202231,202232,202241,202242,202243,
                   202244,202245,202246,202247,202248,202249,2023,202301,202302,202303,20231,202311,202312,
                   202313,202321,203001,203002,203003,203004,203005,203006,203007)
             group by
               coalesce(bass1.fn_get_all_dim('BASS_STD1_0055',char(user_brand)),'2')
               ,coalesce(bass1.fn_get_all_dim('BASS_STD1_0096',char(call_purpose)),'900000')"
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