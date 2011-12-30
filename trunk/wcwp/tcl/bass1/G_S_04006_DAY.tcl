######################################################################################################
#�ӿ����ƣ�����WAP����
#�ӿڱ��룺04006
#�ӿ�˵��������WAP����������PDAҵ�񻰵���
#��������: G_S_04006_DAY.tcl
#��������: ����04006������
#��������: ��
#Դ    ��1.bass2.cdr_wap_yyyymmdd(����WAP�嵥)      
#�������: 
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ�����
#��дʱ�䣺2007-03-22
#�����¼��1.���·��ظ���¼��Ϊ�����㼯�Ź�˾У��F1
#�޸���ʷ: ͨ�ŷ� char((charge1 + charge2 +charge3 +charge4)/10) ��Ϊ char(charge1/10)   zhanght 20090611
#          20090901 1.6.2�淶ȥ��imei�ֶ�
#######################################################################################################


proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

	#���� yyyymmdd
        set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]       
        
        #ɾ����������
        set handle [aidb_open $conn]
	set sql_buff "delete from bass1.g_s_04006_day where time_id=$timestamp"
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
       
       
        set handle [aidb_open $conn]
	set sql_buff "insert into bass1.G_S_04006_DAY
                (
                time_id
                ,product_no
                ,imsi
                ,ticke_type
                ,wap_gate_name
                ,misc_num
                ,home_locn_billing_users
                ,access_code_of_billing_users
                ,usertype_id
                ,sp_code
                ,oper_code
                ,svc_attributes
                ,bearer_type
                ,disc_rate
                ,cdr_status
                ,billing_cat_of_wap_info
                ,wap_basefee
                ,info_fee
                ,month_fee
                ,info_len
                ,date_of_url_req
                ,time_of_url_req
                ,date_of_termtn_of_url
                ,time_of_termtn_of_url
                ) 
               select
                ${timestamp}
                ,product_no
                ,imsi
                ,coalesce(bass1.fn_get_all_dim('BASS_STD2_0031',char(record_type)),'00')
                ,value(gateway_id,'0')
                ,misc_id
                ,value(char(province_id),'891')
                ,value(char(roam_province_id),'891')
                ,'0'
                ,sp_code
                ,service_code
                ,coalesce(bass1.fn_get_all_dim('BASS_STD2_0033',char(service_attr)),'99')
                ,coalesce(bass1.fn_get_all_dim('BASS_STD2_0004',char(service_type)),'9')
                ,char(discount)
                ,'00'
                ,coalesce(bass1.fn_get_all_dim('BASS_STD2_0032',char(charge_type)),'01')
                ,value(char(charge1/10),'0')
                ,value(char(charge4/10),'0')
                ,case
                   when coalesce(bass1.fn_get_all_dim('BASS_STD2_0032',char(charge_type)),'01')='03' then char(charge4/10)
                   else '0'
                 end 
                ,char(message_length)
                ,substr(char(start_time),1,4)||substr(char(start_time),6,2)||substr(char(start_time),9,2)
                ,substr(char(start_time),12,2)||substr(char(start_time),15,2)||substr(char(start_time),18,2)
                ,substr(char(stop_time),1,4)||substr(char(stop_time),6,2)||substr(char(stop_time),9,2)
                ,substr(char(stop_time),12,2)||substr(char(stop_time),15,2)||substr(char(stop_time),18,2)
              from 
                bass2.cdr_wap_${timestamp}
              where
                misc_id is not null "
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