######################################################################################################
#接口名称：通信用户累积
#接口编码：
#接口说明：通信用户累积
#程序名称: INT_22038_YYYYMM.tcl
#功能描述: 目前供22038接口使用
#运行粒度: 日
#源    表：1.bass2.dw_product_yyyymmdd
#输入参数: 
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：王琦
#编写时间：2007-03-22
#问题记录：1.
#修改历史: 20090901 定义：统计周期内产生语音话单（含普通语音、智能网语音、VPMN）、
#点对点短信上行、点对点彩信上行、GPRS话单（彩信核减后且流量大于0）的客户，不判断客户是否在网
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

	#当天 yyyymmdd
        set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]       
        #当天 yyyy-mm-dd
        set optime $op_time
        #前一天 yyyymmdd
        set last_day [GetLastDay [string range $timestamp 0 7]]
        #今天的日期，格式dd(例：输入20070411 返回11)
        set today_dd [string range $op_time 8 9]
        #本月第一天 yyyymmdd
        set op_month [string range $op_time 0 3][string range $op_time 5 6]
        set this_month_first_day [string range $op_month 0 5]01

        #删除本期数据
        set handle [aidb_open $conn]
	set sql_buff "delete from bass1.int_22038_$op_month where op_time=$timestamp"
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
	
 ###普通语音(其中无智能网语音)
	set handle [aidb_open $conn]
	set sql_buff "insert into bass1.int_22038_$op_month
	             select distinct
	               $timestamp
	               ,brand_id
	               ,user_id
                from bass2.cdr_call_dtl_$timestamp"
  puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
	
 ###点对点短信上行
	set handle [aidb_open $conn]
	set sql_buff "insert into bass1.int_22038_$op_month
	             select distinct
	               $timestamp
	               ,case when user_id='89160000038050' then 4 else brand_id end
	               ,user_id
                from bass2.cdr_sms_dtl_$timestamp
               where calltype_id=0
                 and svcitem_id in (200001,200002,200003,200004,200005,200006)"
  puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle	
	

 ###点对点彩信上行
	set handle [aidb_open $conn]
	set sql_buff "insert into bass1.int_22038_$op_month
	             select distinct
	               $timestamp
	               ,brand_id
	               ,user_id
                from bass2.cdr_mms_dtl_$timestamp
               where calltype_id=0
                 and svcitem_id in (400001,400002)"
  puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle	

 ###GPRS话单（彩信核减后且流量大于0）
	set handle [aidb_open $conn]
	set sql_buff "insert into bass1.int_22038_$op_month
	             select distinct
	               $timestamp
	               ,brand_id
	               ,user_id
                from bass2.cdr_gprs_dtl_$timestamp
                where svcitem_id not in (400001,400002,400003,400004,400005,400006)
                  and bigint(upflow1)+bigint(upflow2)+bigint(downflow1)+bigint(downflow2)>0"
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
