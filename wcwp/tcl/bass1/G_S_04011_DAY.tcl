######################################################################################################
#接口名称：普通短信话单
#接口编码：04011
#接口说明：
#程序名称: G_S_04011_DAY.tcl
#功能描述: 生成04011的数据
#运行粒度: 日
#源    表：1.bass2.cdr_sms_dtl_yyyymmdd
#          2.bass1.g_user_lst 
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

        set handle [aidb_open $conn]
	set sql_buff "delete from bass1.g_s_04011_day where time_id=$timestamp"
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
       

  set handle [aidb_open $conn]
	set sql_buff "insert into bass1.G_S_04011_DAY
                     (
                     time_id
                     ,product_no
                     ,imsi
                     ,opposite_no
                     ,third_no
                     ,city_id
                     ,roam_locn
                     ,cdr_type
                     ,smsc_code
                     ,ismg_code
                     ,forw_ismg
                     ,svc_type
                     ,comm_fee
                     ,info_fee
                     ,roam_type_id
                     ,paytype_id
                     ,info_len
                     ,sms_status
                     ,input_date
                     ,input_time
                     ,process_date
                     ,process_time
                     )
                   select
                     $timestamp
                     ,a.product_no
                     ,imsi
                     ,value(a.opp_number,' ') as opposite_no
                     ,'0' as third_no
                     ,value(char(a.city_id),'891')
                     ,'0' as roam_locn
                     ,case 
                       when a.calltype_id=0 then '00'
                       else '01' end	 as cdr_type
                     ,value(a.sms_center_number,' ')  as smsc_code
                     ,value(a.gateway,' ') as ismg_code
                     ,value(a.cfw_gateway,' ') as forw_ismg
                     ,COALESCE(BASS1.FN_GET_ALL_DIM('BASS_STD2_0010',char(a.svcitem_id)),'70') as svc_type
                     ,char(int(a.base_fee*100)) as comm_fee
                     ,'0' as info_fee
                     ,'500' as roam_type_id
                     ,'02'  as paytype_id
                     ,char(a.message_len) as info_len
                     ,'0' as sms_status
                     ,substr(char(a.start_time),1,4)||substr(char(a.start_time),6,2)||substr(char(a.start_time),9,2)
                     ,substr(char(a.start_time),12,2)||substr(char(a.start_time),15,2)||substr(char(a.start_time),18,2)
                     ,substr(char(a.stop_time),1,4)||substr(char(a.stop_time),6,2)||substr(char(a.stop_time),9,2)
                     ,substr(char(a.stop_time),12,2)||substr(char(a.stop_time),15,2)||substr(char(a.stop_time),18,2)
                   from 
                     bass2.cdr_sms_dtl_${timestamp} a,
                     bass1.g_user_lst b 
                   where 
                     b.time_id=${op_month} 
                     and a.user_id=b.user_id "
        puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "\$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}    
	aidb_commit $conn
	aidb_close $handle

	return 0
}