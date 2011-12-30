######################################################################################################
#�ӿ����ƣ��ʷ�Ӫ�����̶�����
#�ӿڱ��룺02022
#�ӿ�˵���������ʷ�Ӫ�����а�һ����ʱ��Ƶ�ʣ��¡��꣩��ȡ�Ĺ̶����á��������޹�˾�г���
#          �����ڵ���GPRS�ʷѱ�׼��֪ͨ.������ͨ[2005]474�ţ���Ϊͳ�Ƹ�ʡGPRS�ײ������
#           ���ʡ�ڴ˽ӿ��ϱ���ʡ��ӦGPRS�ײ����ݡ�GPRS�ײ͵����ʽΪ��������������д11��
#           ���ý����д�ײ���Ӧ������ѣ���5��20��100��200��ͬʱ������λΪ�֡�
#��������: G_I_02022_MONTH.tcl
#��������: ����02022������
#��������: ��
#Դ    ��1.bass2.dwd_promoplan_rent_yyyymmdd(�ʷ�Ӫ�����̶�����)
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
	set sql_buff "delete from bass1.g_i_02022_month where time_id=$op_month"
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle

       
        set handle [aidb_open $conn]
	set sql_buff "insert into bass1.g_i_02022_month
                         select
                           $op_month
                           ,char(prod_id)
                           ,coalesce(bass1.fn_get_all_dim('BASS_STD1_0094',char(item_code)),'37')
                           ,char(sum(case
                              when fee_unit=10 or coalesce(bass1.fn_get_all_dim('BASS_STD1_0094',char(item_code)),'37')='11' then bigint(month_fee/10)                        
                              else month_fee
                            end))
                           ,case 
                              when fee_unit=10 or coalesce(bass1.fn_get_all_dim('BASS_STD1_0094',char(item_code)),'37')='11' then '08'
                              else '09'
                            end 
                         from 
                           bass2.dwd_promoplan_rent_$this_month_last_day
                         group by
                          char(prod_id)
                           ,coalesce(bass1.fn_get_all_dim('BASS_STD1_0094',char(item_code)),'37')
						               ,case 
                              when fee_unit=10 or coalesce(bass1.fn_get_all_dim('BASS_STD1_0094',char(item_code)),'37')='11' then '08'
                              else '09'
                            end
							order by char(prod_id)"
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