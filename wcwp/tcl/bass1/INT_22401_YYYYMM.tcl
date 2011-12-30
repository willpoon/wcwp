######################################################################################################
#接口名称：
#接口编码：
#接口说明：
#程序名称: INT_22401_YYYYMM.tcl
#功能描述: 生成22401的中间表数据
#运行粒度: 日
#源    表：1.bass2.CDR_GPRS_LOCAL_yyyymmdd
#          
#输入参数: 
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：panzhiwei
#编写时间: 20110809
#问题记录：
#修改历史: 1. panzw 20110807	1.7.4 newly added
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {
        #当天 yyyy-mm-dd
	set optime $op_time
	#当天 yyyymmdd
        set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]       
        #本月 yyyymm
        set op_month [string range $op_time 0 3][string range $op_time 5 6]

	set sql_buff "delete from bass1.int_22401_$op_month where TIME_ID=$timestamp"
	exec_sql $sql_buff
       
       
       
       
	set sql_buff "insert into bass1.int_22401_$op_month
                (
		 TIME_ID
		,USER_ID
		,CELL_ID
		,LAC_ID
		,CALL_CNT
                )
           SELECT $timestamp as TIME_ID
	          ,USER_ID
		  ,CELL_ID
		  ,LAC_ID
		  ,count(0)
		  from bass2.CDR_GPRS_LOCAL_$timestamp a
		  group by 
		  USER_ID
		  ,CELL_ID
		  ,LAC_ID
		  with ur

"
exec_sql $sql_buff

	
	return 0
}