######################################################################################################
#�ӿ����ƣ��ʷ�Ӫ��������ҵ����
#�ӿڱ��룺02003
#�ӿ�˵���������ʷ�Ӫ�����а�����ҵ������ϡ�
#          ҵ������ָ�й��ƶ���Ӫ������ṩ�ܹ�����ͻ����ƶ�ͨ�����������ĸ��ַ���
#          ���磺���������š����š�GPRS��WAP���ٱ���ȡ��ͻ�ͨ������ҵ���������ܴ�����Ϣ������
#          ������Ч�ã�ͬʱΪ�й��ƶ������������롣ҵ������Բ�ͬ�Ŀͻ�Ʒ���в�ͬ��ѡ��Ȩ�޺�
#          ��׼�۸�ͬʱҵ�����ڲ�ͬ���ʷ�Ӫ����������Ҳ�������¶���۸�

#��������: G_I_02003_MONTH.tcl
#��������: ����02003������
#��������: ��
#Դ    ��1.bass2.dwd_promoplan_yyyymmdd(�ʷ�Ӫ����)
#�������: 
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ�����
#��дʱ�䣺2007-03-22
#�����¼��1.Ŀǰҵ���ܱ�ʶ���ܳ���������ϵͳ����ͳһ��'010'.��ȡ�Ժ���BOSS�����졣
#�޸���ʷ: 1.
#######################################################################################################

proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

        
        #���� yyyymm
        set op_month [string range $optime_month 0 3][string range $optime_month 5 6]          
        #�������һ�� yyyymmdd
        set this_month_last_day [string range $op_month 0 5][GetThisMonthDays [string range $op_month 0 5]01]
        
        #ɾ����������
        set handle [aidb_open $conn]
	set sql_buff "delete from bass1.g_i_02003_month where time_id=$op_month"
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
        
             
        set handle [aidb_open $conn]
	set sql_buff "insert into bass1.g_i_02003_month
	               select a.time_id,
	                      a.plan_id,
	                      a.bus_func_id
	               from (       
                       select
                          $op_month time_id
                          ,char(prod_id) plan_id
                          ,'010' bus_func_id
                          ,row_number()over(partition by char(prod_id) order by $op_month ) row_id
                          from bass2.dwd_promoplan_$this_month_last_day
                       ) a
                 where a.row_id=1         "       
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