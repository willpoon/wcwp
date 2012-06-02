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
#修改历史:  
##~   20090901 1.6.2规范去掉imei字段
##~   20120331 1.7.9:
##~   522	修改接口04003（WLAN话单）：
##~   1、	增加以下字段：认证平台类型、用户终端类型、用户MAC地址、AP的MAC地址；
##~   2、	修改字段"开始日期"、"开始时间"、"结束日期"、"结束时间"，增加填写格式属性描述；
##~   3、	修改字段"业务标识"，删除其字段描述内容"01：WLAN业务标识"，增加备注说明业务标识分类；
##~   4、	修改字段"WLAN认证类型编码"，增加"03：PEAP认证"、"04：客户端认证"，具体参见话单分类规范中的BASS_STD2_0006；
##~   5、	修改字段"热点地区标识"，增加属性描述格式填写说明，并修改备注内容；	1.7.9	2012-03-20	自数据日期20120401起生效
		##~  20120430 FN_CHANGE_DTOH --> FN_CHANGE_DTOH2
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
							,SERVICE_CODE	--业务标识 
							,WLAN_ATTESTATION_CODE
							,WLAN_ATTESTATION_TYPE --认证平台类型
							,HOTSPOT_AREA_ID
							,AS_IP
							,ATTESTATION_AS_IP
							,USER_TERM_TYPE	--用户终端类型
							,USER_MAC_ADDR	--用户MAC地址
							,AP_MAC	--AP的MAC地址
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
                        ,case  when RESERVE1  = '01' then '01'
							   when RESERVE1 = '02' then '02'
							   when RESERVE1 = '03' then '03'
						else '01' end SERVICE_CODE /*1.7.9：西藏尚未实现，无法区分，保持原口径*/
                        ,case when AUTH_TYPE=1 THEN '01' else '02' end WLAN_ATTESTATION_CODE
						,'01' WLAN_ATTESTATION_TYPE --认证平台类型 /*1.7.9：西藏尚未实现，无法区分,按 01：集团统一认证平台接入 上报*/
                        ,HOSTSPOT_ID
                        ,COALESCE(FN_CHANGE_DTOH2(AS_ADDRESS),' ') AS_IP
                        ,COALESCE(FN_CHANGE_DTOH2(AC_ADDRESS),' ') ATTESTATION_AS_IP
						,'' USER_TERM_TYPE /*用户终端类型 西藏尚未实现，无法区分;报空*/
						,'' USER_MAC_ADDR	--用户MAC地址/* 西藏尚未实现，无法提取;报空*/
						,'' AP_MAC	--AP的MAC地址/* 西藏尚未实现，无法提取;报空*/
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