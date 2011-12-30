######################################################################################################
#�ӿ����ƣ��ͻ���Ͷ��
#�ӿڱ��룺22029
#�ӿ�˵������¼�ͻ���Ͷ�ߵ������Ϣ��
#��������: G_S_22029_MONTH.tcl
#��������: ����22029������
#��������: ��
#Դ    ��1.bass2.dw_custsvc_work_accept_yyyymm(��湤�������)
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
	set sql_buff "delete from bass1.g_s_22029_month where time_id=$op_month"
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
            
       
        set handle [aidb_open $conn]
	set sql_buff "insert into bass1.g_s_22029_month
                      select
                        ${op_month}
                        ,'${op_month}'
                        ,coalesce(bass1.fn_get_all_dim('BASS_STD1_0040',char(appeal_type)),'999999')
                        ,case 
                           when accept_from=10 then '01'
                           when accept_from=20 then '11'
                           when accept_from=80 then '08'
                           else '99' 
                          end
                        ,char(count(*))
                      from 
                        bass2.dw_custsvc_work_accept_$op_month
                      group by
                        coalesce(bass1.fn_get_all_dim('BASS_STD1_0040',char(appeal_type)),'999999')
                        ,case 
                           when accept_from=10 then '01'
                           when accept_from=20 then '11'
                           when accept_from=80 then '08'
                           else '99' 
                         end "
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