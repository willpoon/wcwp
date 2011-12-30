######################################################################################################
#�ӿ����ƣ���ʡ�������ҵ��������
#�ӿڱ��룺05009
#�ӿ�˵������¼��ʡ��ʡ����Ķ���ҵ��Ļ��������
#��������: G_S_05009_MONTH.tcl
#��������: ����05009������
#��������: ��
#Դ    ��1. bass2.dwd_js_ismgjh_sp_yyyymm(��ʡ�������ҵ�����)
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
	set sql_buff "delete from bass1.g_s_05009_month where time_id=$op_month"
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
              
       
        set handle [aidb_open $conn]
	set sql_buff "insert into bass1.g_s_05009_month
                      select
                        ${op_month}
                        ,'${op_month}'
                        ,sp_code
                        ,char(int(bal_fee      ))
                        ,char(int(error_fee    ))
                        ,char(int(error_user   ))
                        ,char(int(tj_fee       ))
                        ,char(int(tj_sheet_cnt ))
                        ,char(int(tj_user      ))
                        ,char(int(cj_fee       ))
                        ,char(int(cj_sheet_cnt ))
                        ,char(int(cj_user      ))
                        ,char(int(dg_fee       ))
                        ,char(int(dg_sheet_cnt ))
                        ,char(int(dg_user      ))
                        ,char(int(cm_fee       ))
                        ,char(int(cm_user      ))
                        ,char(int(dt_fee       ))
                        ,char(int(dt_sheet_cnt ))
                        ,char(int(dt_user      ))
                        ,char(int(hj_user      ))
                        ,char(int(other_fee    ))
                        ,char(int(other_user   ))
                      from 
                        bass2.dwd_js_ismgjh_sp_$op_month"
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