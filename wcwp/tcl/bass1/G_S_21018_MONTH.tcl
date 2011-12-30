######################################################################################################
#�ӿ����ƣ�ʡ����������ͨ�������ʹ��
#�ӿڱ��룺21018
#�ӿ�˵������¼ʡ����������ͨ�������ʹ�������Ϣ��
#��������: G_S_21018_MONTH.tcl
#��������: ����21018������
#��������: ��
#Դ    ��1. bass2.dw_call_roamin_dm_yyyymm(���뻰���±�)
#�������: 
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ�����
#��дʱ�䣺2007-03-22
#�����¼��1.��ΪҪ�漰�������ۻ������ݣ�����Ҫ�����ӱ�����ʼ�����ݡ�
#          2.�����ȷ��call_duration_s�ĵ�λ�Ƿ���
#�޸���ʷ: 1.
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {
        
        #���� yyyy
        set op_year [string range $optime_month 0 3]
        #���� yyyymm
        set op_month [string range $optime_month 0 3][string range $optime_month 5 6]          
      
        #ɾ����������
        set handle [aidb_open $conn]
	set sql_buff "delete from bass1.g_s_21018_month where time_id=$op_month"
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
        
        #����session�м��
        set handle [aidb_open $conn]
	set sql_buff "declare global temporary table session.g_s_21018_month_tmp
                       (
                         tolltype_id               varchar(3),
                         calltype_id               varchar(2),
                         call_duration_month       bigint,
                         call_duration_year        bigint,
                         s_call_duration_month     bigint,
                         s_call_duration_year      bigint
                       )with replace on commit preserve rows not logged in tbs_user_temp "  
        puts   $sql_buff  
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}        
	aidb_commit $conn
	aidb_close $handle
	
	#���뱾������
        set handle [aidb_open $conn]
	set sql_buff "insert into session.g_s_21018_month_tmp
                        select
                          COALESCE(BASS1.FN_GET_ALL_DIM('BASS_STD2_0001',CHAR(tolltype_id)),'052')
                          ,COALESCE(BASS1.FN_GET_ALL_DIM('BASS_STD2_0011',CHAR(calltype_id)),'01')
                          ,sum(call_duration_m)
                          ,sum(call_duration_m)
                          ,sum(call_duration_s)
                          ,sum(call_duration_s)
                        from 
                          bass2.dw_call_roamin_dm_$op_month
                        group by
                           COALESCE(BASS1.FN_GET_ALL_DIM('BASS_STD2_0001',CHAR(tolltype_id)),'052')
                          ,COALESCE(BASS1.FN_GET_ALL_DIM('BASS_STD2_0011',CHAR(calltype_id)),'01')"
        puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}        
	aidb_commit $conn
	aidb_close $handle	
	
	#�����ۻ�
        set handle [aidb_open $conn]
	set sql_buff "insert into session.g_s_21018_month_tmp
                        select
                          tolltype_id
                          ,calltype_id
                          ,0
                          ,sum(bigint(call_duration_year))
                          ,0
                          ,sum(bigint(s_call_duration_year))
                        from 
                          bass1.g_s_21018_month
                        where 
                          time_id/100=$op_year
                        group by 
                          tolltype_id
                          ,calltype_id "
        puts $sql_buff                         
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}        
	aidb_commit $conn
	aidb_close $handle
	
	#���ܵ������
        set handle [aidb_open $conn]
	set sql_buff "insert into bass1.g_s_21018_month
                        select
                          $op_month 
                          ,'$op_month'
                          ,tolltype_id
                          ,calltype_id
                          ,char(sum(call_duration_month))
                          ,char(sum(call_duration_year))
                          ,char(sum(s_call_duration_month))
                          ,char(sum(s_call_duration_year))
                        from 
                          session.g_s_21018_month_tmp
                        group by 
                          tolltype_id
                          ,calltype_id "  
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