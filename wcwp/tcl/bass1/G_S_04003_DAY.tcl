######################################################################################################
#接口名称：WLAN话单
#接口编码：04003
#接口说明：
#程序名称: G_S_04003_DAY.tcl
#功能描述: 生成04003的数据
#运行粒度: 日
#源    表：1.bass2.cdr_wlan_yyyymmdd(wlan清单)        
#输入参数: 
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：王琦
#编写时间：2007-03-22
#问题记录：
#修改历史: 20090901 1.6.2规范去掉imei字段
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

        #当天 yyyy-mm-dd
	set optime $op_time
	#当天 yyyymmdd
        set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]       
        #本月 yyyymm
        set op_month [string range $op_time 0 3][string range $op_time 5 6]
        
        #删除本期数据
        set handle [aidb_open $conn]
	set sql_buff "delete from bass1.g_s_04003_day where time_id=$timestamp"
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle

       
     
        set handle [aidb_open $conn]
	set sql_buff "insert into bass1.G_S_04003_DAY
                        (
                         TIME_ID
                         ,PRODUCT_NO
                         ,IMSI
                         ,HOME_LOCN
                         ,ROAM_LOCN
                         ,USER_TYPE
                         ,ROAM_TYPE_ID
                         ,START_DATE
                         ,START_TIME
                         ,END_DATE
                         ,END_TIME
                         ,CALL_DURATION
                         ,FLOWUP
                         ,FLOWDOWN
                         ,SVCITEM_ID
                         ,SERVICE_CODE
                         ,WLAN_ATTESTATION_CODE
                         ,HOTSPOT_AREA_ID
                         ,AS_IP
                         ,ATTESTATION_AS_IP
                         ,CALL_FEE
                         ,INFO_FEE
                         ,SERVICE_ID
                         ,ISP_ID
                         ,BELONG_OPER_ID
                         ,ROAM_OPER_ID
                         ,REASON_OF_STOP_CODE
                         ) 
               select
                        ${timestamp} 
                        ,PRODUCT_NO 
                        ,IMSI
                        ,value(char(PROVINCE_ID),'891')
                        ,value(char(ROAM_PROVINCE_ID),'891')
                        ,'1'
                        ,COALESCE(BASS1.FN_GET_ALL_DIM('BASS_STD2_0012',char(ROAMTYPE_ID)),'500') 
                        ,substr(char(START_TIME),1,4)||substr(char(START_TIME),6,2)||substr(char(START_TIME),9,2)
                        ,substr(char(START_TIME),12,2)||substr(char(START_TIME),15,2)||substr(char(START_TIME),18,2)
                        ,substr(char(STOP_TIME),1,4)||substr(char(STOP_TIME),6,2)||substr(char(STOP_TIME),9,2)
                        ,substr(char(STOP_TIME),12,2)||substr(char(STOP_TIME),15,2)||substr(char(STOP_TIME),18,2)
                        ,char(DURATION)
                        ,char(DATA_FLOW_UP)
                        ,char(DATA_FLOW_DOWN)
                        ,'03'
                        ,'01'
                        ,case when AUTH_TYPE=1 THEN '01' else '02' end
                        ,HOSTSPOT_ID
                        ,COALESCE(FN_CHANGE_DTOH(AS_ADDRESS),' ')
                        ,COALESCE(FN_CHANGE_DTOH(AC_ADDRESS),' ')
                        ,char(CHARGE1/10 + CHARGE4/10)
                        ,char(CHARGE4/10)
                        ,''
                        ,''
                        ,substr(char(bigint(IMSI)),1,5)
                        ,substr(char(bigint(IMSI)),1,5)
                        ,''
                        from bass2.cdr_wlan_${timestamp}
                        where IMSI is not null and DATA_FLOW_DOWN >= 0"
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