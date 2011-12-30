######################################################################################################
#�ӿ����ƣ�������־ҵ��ʹ��
#�ӿڱ��룺21012
#�ӿ�˵������¼�й��ƶ���˾�û�������־����ҵ��ʹ����Ϣ��
#��������: G_S_21012_MONTH.tcl
#��������: ����21012������
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
	set sql_buff "delete from bass1.g_s_21012_month where time_id=$op_month"
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle

              
        set handle [aidb_open $conn]
	set sql_buff "insert into bass1.g_s_21012_month
                          select
                            $op_month
                            ,'$op_month'
                            ,product_no
                            ,coalesce(bass1.fn_get_all_dim('BASS_STD1_0055',char(brand_id)),'2')
                            ,coalesce(bass1.fn_get_all_dim('BASS_STD2_0011',char(calltype_id)),'01')
                            ,opp_number
                            ,coalesce(bass1.fn_get_all_dim('BASS_STD2_0013',char(callmoment_id)),'114')
                            ,char(bigint(sum(call_duration)))
                            ,char(bigint(sum(basecall_fee*100)))
                            ,char(bigint(sum(info_fee*100)))
                          from 
                            bass2.dw_call_opposite_$op_month
                          where 
                            substr(opp_number,1,5) = '12590'
                          group by 
                            product_no
                            ,coalesce(bass1.fn_get_all_dim('BASS_STD1_0055',char(brand_id)),'2')
                            ,coalesce(bass1.fn_get_all_dim('BASS_STD2_0011',char(calltype_id)),'01')
                            ,opp_number
                            ,coalesce(bass1.fn_get_all_dim('BASS_STD2_0013',char(callmoment_id)),'114') "
                            
        puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}        
	aidb_commit $conn
	aidb_close $handle
	
	

        set handle [aidb_open $conn]
	set sql_buff "insert into bass1.g_s_21012_month
                          select
                            $op_month
                            ,'$op_month'
                            ,product_no
                            ,coalesce(bass1.fn_get_all_dim('BASS_STD1_0055',char(brand_id)),'2')
                            ,coalesce(bass1.fn_get_all_dim('BASS_STD2_0011',char(calltype_id)),'01')
                            ,opp_number
                            ,coalesce(bass1.fn_get_all_dim('BASS_STD2_0013',char(callmoment_id)),'114')
                            ,char(bigint(sum(call_duration)))
                            ,char(bigint(sum(basecall_fee*100)))
                            ,char(bigint(sum(info_fee*100)))
                          from 
                            bass2.dw_call_opposite_$op_month
                          where 
                            substr(opp_number,1,5)='12596'
                          group by 
                            product_no
                            ,coalesce(bass1.fn_get_all_dim('BASS_STD1_0055',char(brand_id)),'2')
                            ,coalesce(bass1.fn_get_all_dim('BASS_STD2_0011',char(calltype_id)),'01')
                            ,opp_number
                            ,coalesce(bass1.fn_get_all_dim('BASS_STD2_0013',char(callmoment_id)),'114') "
                            

        puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}        
	aidb_commit $conn
	aidb_close $handle



        set handle [aidb_open $conn]
	set sql_buff "insert into bass1.g_s_21012_month
                          select
                            $op_month
                            ,'$op_month'
                            ,product_no
                            ,coalesce(bass1.fn_get_all_dim('BASS_STD1_0055',char(brand_id)),'2')
                            ,coalesce(bass1.fn_get_all_dim('BASS_STD2_0011',char(calltype_id)),'01')
                            ,opp_number
                            ,coalesce(bass1.fn_get_all_dim('BASS_STD2_0013',char(callmoment_id)),'114')
                            ,char(bigint(sum(call_duration)))
                            ,char(bigint(sum(basecall_fee*100)))
                            ,char(bigint(sum(info_fee*100)))
                          from 
                            bass2.dw_call_opposite_$op_month
                          where 
                            substr(opp_number,1,5)='12559' 
                          group by 
                            product_no
                            ,coalesce(bass1.fn_get_all_dim('BASS_STD1_0055',char(brand_id)),'2')
                            ,coalesce(bass1.fn_get_all_dim('BASS_STD2_0011',char(calltype_id)),'01')
                            ,opp_number
                            ,coalesce(bass1.fn_get_all_dim('BASS_STD2_0013',char(callmoment_id)),'114') "
                            
                            
                                                    
#when substr(opp_regular_number,1,6) in ('125904','125905','125906','125907','125908') or
#                                substr(opp_regular_number,1,5)='12596'                    then 100002
#                           when substr(opp_regular_number,1,6) in ('125900','125901','125902','125903','125909') or
#                                substr(opp_regular_number,1,5)='12559'                    then 100026


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
