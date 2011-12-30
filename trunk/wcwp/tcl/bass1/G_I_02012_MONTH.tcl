######################################################################################################
#�ӿ����ƣ��û���ϵ��ʷ
#�ӿڱ��룺02012
#�ӿ�˵������¼�û�֮��Ĺ�ϵ�����ݹ�ϵ�������֡�Ŀǰ��ע���û���ϵ���Ͱ�����
#          һ��˫�š���ĸ����һ����š�
#��������: G_I_02012_MONTH.tcl
#��������: ����02012������
#��������: ��
#Դ    ��1.bass2.dwd_product_relation_yyyymm(�û���ϵ��)
#�������: 
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ�����
#��дʱ�䣺2007-03-22
#�����¼��1.����û��һ��˫�ţ�һ����ŵ�ҵ�񣬹�ϵ����ֻȡ��ĸ��,�ο�dim_busi_type
#          2.���ǵ��ýӿ��������û������������壬����ֻ��������
#�޸���ʷ: 1.20091217 dwd_product_relation_yyyymm �޸ĳ�dw���±�
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {
       
        #���� yyyymm
        set op_month [string range $optime_month 0 3][string range $optime_month 5 6]          
     
        #ɾ����������
        set handle [aidb_open $conn]
	set sql_buff "delete from bass1.g_i_02012_month where time_id=$op_month"
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
       
       
        set handle [aidb_open $conn]
	set sql_buff "insert into bass1.g_i_02012_month
                select
                  $op_month
                  ,'02'
                  ,user_id
                  ,rserv_id
                from 
                  bass2.dw_product_relation_$op_month
                where 
                  busi_type=3 "
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