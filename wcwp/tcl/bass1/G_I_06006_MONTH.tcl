######################################################################################################
#�ӿ����ƣ�IMSI��Դ
#�ӿڱ��룺06006
#�ӿ�˵����IMSI��Դ�⡣
#��������: G_I_06006_MONTH.tcl
#��������: ����06006������
#��������: ��
#Դ    ��1.bass2.dwd_res_sim_yyyymmdd(����Դ)
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
        #�������һ�� yyyymmdd
        set this_month_last_day [string range $op_month 0 5][GetThisMonthDays [string range $op_month 0 5]01]
        
        #ɾ����������
        set handle [aidb_open $conn]
	set sql_buff "delete from bass1.g_i_06006_month where time_id=$op_month"
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle

        set handle [aidb_open $conn]
	set sql_buff "insert into bass1.g_i_06006_month
                        select
                          $op_month
                          ,substr(imsi,1,15)
                          ,coalesce(bass1.fn_get_all_dim('BASS_STD1_0054',char(city_id)),'13100')
                          ,value(sim_id,' ')
                          ,char(channel_id)
                          ,case
                             when sts=1 then '01'
                             when sts=2 then '02'
                             when sts=3 then '03'
                             when sts=4 then '04'
                             when sts=5 then '06'
                             else '03'
                           end 
                        from  bass2.dwd_res_sim_$this_month_last_day  "
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