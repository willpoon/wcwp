
######################################################################################################		
#接口名称: 电子渠道终端销售信息                                                               
#接口编码：22095                                                                                          
#接口说明：记录江苏、广东电子渠道支撑相应省份终端销售统计信息
#程序名称: G_S_22095_DAY.tcl                                                                            
#功能描述: 生成22095的数据
#运行粒度: DAY
#源    表：1.
#输入参数: void
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：panzw
#编写时间：20120507
#问题记录：
#修改历史: 1. panzw 20120507	中国移动一级经营分析系统省级数据接口规范 (V1.8.0) 
##~   2、本接口仅江苏、广东省份上传。
#######################################################################################################   
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {
      set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
      puts $timestamp
    #本月 yyyymm
    set op_month [string range $optime_month 0 3][string range $optime_month 5 6]
    puts $op_month
      
    #上个月 yyyymm
    set last_month [GetLastMonth [string range $op_month 0 5]]
    puts $last_month

        #程序名
        global app_name
        set app_name "G_S_22095_DAY.tcl"
	
  #删除本期数据
	set sql_buff "delete from bass1.G_S_22095_DAY where time_id=$timestamp"
	exec_sql $sql_buff

	return 0
}

