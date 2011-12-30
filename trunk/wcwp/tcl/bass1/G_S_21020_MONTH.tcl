######################################################################################################
#�ӿ����ƣ��������������û�����12593���
#�ӿڱ��룺21020
#�ӿ�˵������¼�����������õ��û��ڱ�ʡ�ڲ���12593�����ͨ�������
#��������: G_S_21020_MONTH.tcl
#��������: ����21020������
#��������: ��
#Դ    ��1.bass2.dw_call_roamin_dm_yyyymm(���뻰�����ձ�)
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
	set sql_buff "delete from bass1.g_s_21020_month where time_id=$op_month"
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
	
	#ֱ�ӻ��ܵ������
        set handle [aidb_open $conn]      
	set sql_buff "insert into bass1.g_s_21020_month
                      select
                         $op_month
                        ,'$op_month'
                        ,char(count(distinct opp_noaccess_number))
                        ,char(bigint(sum(call_counts)))
                        ,char(bigint(sum(call_duration)))
                        ,char(bigint(sum(call_duration_m)))
                        ,char(bigint(sum(basecall_fee)*100))
                        ,char(bigint(sum(toll_fee)*100))
                      from
                        bass2.dw_call_roamin_dm_$op_month
                      where
                        opp_access_type_id=83 "
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