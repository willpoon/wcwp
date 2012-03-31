
######################################################################################################		
#接口名称: WLAN用户（手机）的IMEI信息                                                               
#接口编码：02048                                                                                          
#接口说明：记录使用手机进行WLAN上网行为的用户IMEI信息。
#程序名称: G_S_02048_DAY.tcl                                                                            
#功能描述: 生成02048的数据
#运行粒度: DAY
#源    表：1.
#输入参数: void
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：panzw
#编写时间：20120328
#问题记录：
#修改历史: 1. panzw 20120328	中国移动一级经营分析系统省级数据接口规范 (V1.7.9) 
#######################################################################################################   

proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

	#当天 yyyymmdd
        set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]       
        #当天 yyyy-mm-dd
        set optime $op_time
	set sql_buff "
	insert into bass1.G_S_02048_DAY
		  (
         TIME_ID
        ,USER_ID
        ,MSISDN
        ,IMEI
		  )
 select	TIME_ID
        ,USER_ID
        ,MSISDN
        ,IMEI
        from G_S_02048_DAY
where USER_ID = 'xxxxxxxxxxx'
with ur
  "
	exec_sql $sql_buff
        
	return 0
}
