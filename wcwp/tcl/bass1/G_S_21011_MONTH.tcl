######################################################################################################
#�ӿ����ƣ��ƶ�ɳ��ҵ��ʹ��
#�ӿڱ��룺21011
#�ӿ�˵������¼�й��ƶ���˾�û��ƶ�ɳ��ҵ��ʹ����Ϣ��
#��������: G_S_21011_MONTH.tcl
#��������: ����21011������
#��������: ��
#Դ    ��1.bass2.dw_newbusi_call_yyyymm(�����ձ���ҵ���»���)
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
	set sql_buff "delete from bass1.g_s_21011_month where time_id=$op_month"
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
            
       
        set handle [aidb_open $conn]
	set sql_buff "insert into bass1.g_s_21011_month
                             select
                               $op_month
                               ,'$op_month'
                               ,product_no
                               ,coalesce(bass1.fn_get_all_dim('BASS_STD2_0011',char(calltype_id)),'01')
                               ,coalesce(bass1.fn_get_all_dim('BASS_STD1_0055',char(brand_id)),'2')
                               ,'12586'
                               ,coalesce(bass1.fn_get_all_dim('BASS_STD2_0013',char(callmoment_id)),'114')
                               ,char(int(sum(call_duration)))
                               ,char(int(sum(base_fee*100)))
                               ,char(int(sum(info_fee*100)))
                             from 
                               bass2.dw_newbusi_call_$op_month
                             where 
                               svcitem_id=100001
                             group by 
                               product_no
                               ,coalesce(bass1.fn_get_all_dim('BASS_STD2_0011',char(calltype_id)),'01')
                               ,coalesce(bass1.fn_get_all_dim('BASS_STD1_0055',char(brand_id)),'2')
                               ,'12586'
                               ,coalesce(bass1.fn_get_all_dim('BASS_STD2_0013',char(callmoment_id)),'114') "                             
                               
        puts  $sql_buff
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