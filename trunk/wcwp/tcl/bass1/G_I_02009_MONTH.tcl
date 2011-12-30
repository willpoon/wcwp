######################################################################################################
#�ӿ����ƣ��û����ö��
#�ӿڱ��룺02009
#�ӿ�˵�����û��ĵ�ǰ���ö�ȡ�
#��������: G_I_02009_MONTH.tcl
#��������: ����02009������
#��������: ��
#Դ    ��1.bass2.dw_product_yyyymm(�û������±�)
#�������: 
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ�����
#��дʱ�䣺2007-03-22
#�����¼��1.���ǵ��ýӿ��������û������������壬����ֻ��������
#�޸���ʷ: 1.
#######################################################################################################

proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

        #���� yyyymm
        set op_month [string range $optime_month 0 3][string range $optime_month 5 6]          
             
        #ɾ����������
        set handle [aidb_open $conn]
	set sql_buff "delete from bass1.g_i_02009_month where time_id=$op_month"
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
            
        set handle [aidb_open $conn]
	set sql_buff "insert into bass1.g_i_02009_month
                       select
                         $op_month
                         ,user_id
                         ,char(int(CREDIT/10))
                       from 
                         bass2.dw_product_$op_month
                       where 
                         userstatus_id in (1,2,3,6)
                         and usertype_id in (1,2,9)  "   
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