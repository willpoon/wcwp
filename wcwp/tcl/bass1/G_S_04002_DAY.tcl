######################################################################################################
#�ӿ����ƣ�GPRS����
#�ӿڱ��룺04002
#�ӿ�˵����GPRS������
#��������: G_S_04002_DAY.tcl
#��������: ����04002������
#��������: ��
#Դ    ��1.bass2.cdr_gprs_local_yyyymmdd(GPRS�嵥(���أ����ݼƷ�) )   
#�������: 
#�������: ����ֵ:0 �ɹ�;-1 ʧ��
#�� д �ˣ�����
#��дʱ�䣺2007-03-22 
#�����¼��
#�޸���ʷ: liuzhilong 20090630 1.6.0�淶 ȥ��������GPRS����
#          20090901 1.6.2�淶 ����imei�ֶ�char(17)
####       20091123 �޸�ȥ�������������Ŀھ� apn_ni not in ('CMTDS') ��Ϊdrtype_id not in (8307)
####       1.6.5�淶ȥ�����ﹲģ�ն��嵥 bigint(product_no) not between 14734500000 and 14734999999
####            ����ҵ����� service_code �Լ�ɾ��һЩ�ֶ�,����¹淶
####       1.6.7�淶 �޳����ź˼����� service_code not in (1010000001,1010000002,2000000000)
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

	#���� yyyymmdd
        set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]       
        

    #ɾ����������
    set handle [aidb_open $conn]
	set sql_buff "delete from bass1.g_s_04002_day where time_id=$timestamp"
	puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle


    set handle [aidb_open $conn]
	set sql_buff "insert into bass1.G_S_04002_DAY
                    (
                     time_id
					,product_no
					,roam_locn
					,roam_type_id
					,apnni
					,start_time
					,call_duration
					,up_flows
					,down_flows
					,all_fee
					,mns_type
					,imei
					,service_code
                     )
             select
                  $timestamp
                  ,product_no
                  ,COALESCE(char(roam_city_id),'891') as roam_locn
                  ,COALESCE(BASS1.FN_GET_ALL_DIM('BASS_STD2_0012',char(roamtype_id)),'500') as roam_type_id
                  ,apn_ni
                  ,replace(char(date(start_time)),'-','')||replace(char(time(start_time)),'.','') as start_time
                  ,char(sum(duration))        as call_duration
                  ,char(sum(data_flow_up1+data_flow_up2))   as up_flows
                  ,char(sum(data_flow_down1+data_flow_down2)) as down_flows
                  ,char(sum(charge1+charge2+charge3+charge4)/10) as all_fee
                  ,value(char(mns_type),'0')  as mns_type
                  ,value(char(imei),'0')      as imei
                  ,case when upper(apn_ni)<>'CMWAP' then ''
                        when upper(apn_ni)='CMWAP' and service_code in ('999998','999999') then ''
                   else service_code end      as service_code
            from bass2.CDR_GPRS_LOCAL_$timestamp
            where drtype_id not in (8307)
              and bigint(product_no) not between 14734500000 
              and 14734999999 and apn_ni <> 'JF.XZ.IP.MOBILE.LAN.CHINAMOBILE'
              and service_code not in ('1010000001','1010000002','2000000000')
            group by product_no
                  ,COALESCE(char(roam_city_id),'891')
                  ,COALESCE(BASS1.FN_GET_ALL_DIM('BASS_STD2_0012',char(roamtype_id)),'500')
                  ,apn_ni
                  ,start_time
                  ,value(char(mns_type),'0')
                  ,value(char(imei),'0')
                  ,case when upper(apn_ni)<>'CMWAP' then ''
                        when upper(apn_ni)='CMWAP' and service_code in ('999998','999999') then ''
                   else service_code end
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
}
