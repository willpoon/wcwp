######################################################################################################
#�ӿ����ƣ����ʳ�;��������»���
#�ӿڱ��룺22071
#�ӿ�˵�������ʳ�;ҵ����������»��� �������й��ƶ��û��͹������������û������й��ƶ���IDD��
#          IP������Դ�����У�������ʳ�;���û����й��ƶ��û����ͶԷ���Ӫ�̣����������û���ȡ�õ�
#          ʵ�����롣
#��������: G_I_22071_MONTH.tcl
#��������: ����22071������
#��������: ��
#Դ    ��1.bass2.dw_call_yyyymm(ҵ�����±�)

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
        #����  yyyymm
        set last_month [GetLastMonth [string range $op_month 0 5]]
        
        #ɾ����������
        set handle [aidb_open $conn]
	set sql_buff "delete from bass1.g_i_22071_month where time_id=$op_month"
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle

       #ֱ�ӻ��ܵ������
        set handle [aidb_open $conn]      
	set sql_buff "insert into bass1.g_i_22071_month
                      select
                         ${op_month}
                        ,'${op_month}'
                        ,coalesce(bass1.fn_get_all_dim('BASS_STD1_0007',char(roam_city_id)),'900')
                        ,case
                          when ip_mark=1 then '2'
                          else '1'
                         end
                        ,char(bigint(sum(basecall_fee+toll_fee+info_fee+other_fee)*100))                       
                      from 
                        bass2.dw_call_$op_month 
                      where 
                        tolltype_id in (3,4,5,6,7,8,9,10,11,12,13,99,103,104,105,106,107,108,109,110,111,112,113,999)
                      group by
                        coalesce(bass1.fn_get_all_dim('BASS_STD1_0007',char(roam_city_id)),'900')
                        ,case
                           when ip_mark=1 then '2'
                           else '1'
                         end                        
                        "
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
	return 0
}