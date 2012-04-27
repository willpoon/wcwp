######################################################################################################
#�ӿ����ƣ����Ż���
#�ӿڱ��룺04004
#�ӿ�˵����
#��������: G_S_04004_DAY.tcl
#��������: ����04004������
#��������: ��
#Դ    ��1.bass2.cdr_mms_yyyymmdd(���Ż���)  
#�������: 
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ�����
#��дʱ�䣺2007-03-22
#�����¼��
#�޸���ʷ: 
#  20070902  �Ļ�ѧ
#            ��char(usertype_id)��Ϊ��ֵ '0' --��ͨ�û�
#            ��send_status in (0,1,2,3) AND RTRIM(char(usertype_id)) <> '201'  ��Ϊ  send_status in (0,1,2,3) 
#  20090901 1.6.2�淶ȥ��imei�ֶ�

#######################################################################################################


proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

	#���� yyyymmdd
        set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]       

	set sql_buff "values ( int(replace(char(current date - 118 days),'-','')) )"
   exec_sql $sql_buff
   set DeletedDate [get_single $sql_buff]
   puts $DeletedDate
	set sql_buff "delete from  bass1.G_S_04004_DAY_STORE2  where time_id= int(replace(char(current date - 118 days),'-',''))"
   exec_sql $sql_buff

	set sql_buff "insert into  bass1.G_S_04004_DAY_STORE2  select * from  bass1.G_S_04004_DAY where time_id= int(replace(char(current date - 118 days),'-','')) "
   exec_sql $sql_buff

	set sql_buff "delete from bass1.g_s_04004_day where time_id= int(replace(char(current date - 118 days),'-','')) "
   exec_sql $sql_buff

	set sql_buff "delete from bass1.g_s_04004_day where time_id=$timestamp" 
	exec_sql $sql_buff            
	
        set handle [aidb_open $conn]
	set sql_buff "insert into bass1.G_S_04004_DAY
                           (
                             time_id
                            ,product_no
                            ,roam_type_id
                            ,user_type
                            ,mm_bill_type
                            ,code_of_prov_prefecture_users
                            ,send_s_addr
                            ,receiver_s_addr
                            ,fwd_product_no
                            ,send_date
                            ,send_time
                            ,info_type
                            ,applcn_type
                            ,fwd_copy_type
                            ,billing_type
                            ,bearer_mode
                            ,call_fee
                            ,info_fee
                            ,mm_len
                            ,mm_send_status
                            ,mmsc_id_of_orig_prty
                            ,mmsc_id_of_receiver
                            ,mm_content_type
                            ,sp_ent_code
                            ,svc_code
                            ,bus_code
                            ,mm_kind
                            ,bus_srv_id
                            ,imsi
                             )
                          select
                            ${timestamp}
                            ,product_no
                            ,coalesce(bass1.fn_get_all_dim('BASS_STD2_0012',char(roam_type)),'500') 
                            ,'0'
                            
                            ,coalesce(bass1.fn_get_all_dim('BASS_STD2_0038',char(mm_type)),'')
                            ,value(char(province_id),'891')
                            ,value(char(send_address),' ')
                            ,value(char(receive_address),' ')
                            ,''
                            ,substr(char(start_time),1,4)||substr(char(start_time),6,2)||substr(char(start_time),9,2)			
                            ,substr(char(start_time),12,2)||substr(char(start_time),15,2)||substr(char(start_time),18,2)
                            ,COALESCE(BASS1.FN_GET_ALL_DIM('BASS_STD2_0041',CHAR(INFO_TYPE)),'')
                            ,COALESCE(BASS1.FN_GET_ALL_DIM('BASS_STD2_0042',CHAR(APP_TYPE)),'')
                            ,COALESCE(BASS1.FN_GET_ALL_DIM('BASS_STD2_0044',CHAR(TRANSMIT_TYPE)),'')
                            ,COALESCE(BASS1.FN_GET_ALL_DIM('BASS_STD2_0039',CHAR(CHARGE_TYPE)),'')
                            ,COALESCE(BASS1.FN_GET_ALL_DIM('BASS_STD2_0036',CHAR(CARRY_TYPE)),'')
                            ,value(char((charge1 + charge2 + charge3)/10),'0')
                            ,char(charge4/10)
                            ,char(mm_length)
                            ,coalesce(bass1.fn_get_all_dim('BASS_STD2_0037',char(send_status)),'')
                            ,send_mmsc_id
                            ,receive_mmsc_id
                            ,content_type
                            ,value(sp_code,'0')
                            ,value(ser_code,'0')
                            ,value(oper_code,'0')
                            ,char(mm_class)
                            ,case 
                              when drtype_id=103 and app_type=0        then '1'
                              when drtype_id=103 and app_type in (1,2) then '3'
                              when drtype_id=103 and app_type in (3,4) then '2'
                              else '4'
			     end
                           ,value(imsi,' ')
                         from  
                           bass2.cdr_mms_${timestamp} 
                         where 
                           send_status in (0,1,2,3) and send_mmsc_id is not null"
        puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}        
	aidb_commit $conn
	aidb_close $handle

aidb_runstats bass1.g_s_04004_day 3
	return 0
}