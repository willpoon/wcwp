######################################################################################################
#�ӿ����ƣ�
#�ӿڱ��룺21008
#�ӿ�˵��:
#��������: G_S_21008_MONTH.tcl
#��������: ����21008������
#��������: ��
#Դ    ��1.bass1.g_s_21008_to_day(21008�սӿ�)
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
	set sql_buff "delete from bass1.g_s_21008_month where time_id=$op_month"
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
       
      
        set handle [aidb_open $conn]
	set sql_buff "insert into bass1.G_S_21008_MONTH
                       SELECT
		         $op_month
		         ,'$op_month'
		         ,product_no
		         ,brnd_id
		         ,svc_type_id
		         ,cdr_type_id
		         ,end_status
		         ,adversary_id
		         ,char(sum(bigint(sms_counts)))
		         ,char(sum(bigint(sms_base_fee)))
		         ,char(sum(bigint(sms_info_fee)))
		         ,char(sum(bigint(sms_month_fee)))
		       from
			 bass1.g_s_21008_to_day
		       where   
		         time_id/100=$op_month
		       group by
		         product_no
		         ,brnd_id
		         ,svc_type_id
		         ,cdr_type_id
		         ,end_status
		         ,adversary_id"
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