######################################################################################################
#�ӿ����ƣ�Ա��
#�ӿڱ��룺06017
#�ӿ�˵�������νӿ��и�ʵ���¼Ӫ����Ա/�ͷ���Ա��Ա����Ϣ��
#          �������ͻ������������Ļ���Ա��Ӫҵ��������Ա��ֱ����Ա��ʹ��"Ӫ����Ա���ʹ���"����������Ա��
#��������: G_I_06017_MONTH.tcl
#��������: ����06017������
#��������: ��
#Դ    ��1.bass2.DIM_CUSTSVC_STAFF(�ͷ�Ա��)
#�������: 
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ�����
#��дʱ�䣺2007-03-22
#�����¼��1.Ŀǰbass2.DIM_CUSTSVC_STAFF(�ͷ�Ա��)������ֻ�ܳ��ýӿ�Ա����ʶ/������
#�޸���ʷ: 1.
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

        #���� yyyymm
        set op_month [string range $optime_month 0 3][string range $optime_month 5 6]          
  
        #ɾ����������
        set handle [aidb_open $conn]
	set sql_buff "delete from bass1.g_i_06017_month where time_id=$op_month"
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "Error: SQL query failed! The error message is:\n	$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle


        set handle [aidb_open $conn]
	set sql_buff "insert into bass1.g_i_06017_month
                        select
                          $op_month
                          ,char(op_id)
                          ,substr(op_name,1,20)
                          ,' '
                          ,' '
                          ,'20300101'
                          ,' '
                          ,' '
                          ,' '
                          ,' '
                          ,'01'
                          ,'13101'
                        from bass2.dim_custsvc_staff "

	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "Error: SQL query failed! The error message is:\n	$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle

	return 0
}