######################################################################################################
#接口名称：渠道统计月汇总2
#接口编码：22018
#接口说明：记录渠道统计的相关信息，其中"03  渠道类型编码"的如下维值必需提供（其它维值能上报则报）。
#程序名称: G_S_22018_MONTH.tcl
#功能描述: 生成22018的数据
#运行粒度: 月
#源    表：1.
#输入参数: 
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：王琦
#编写时间：2007-03-22
#问题记录：1.
#修改历史: 1.
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {
	#本月 yyyymm
	set op_month [string range $optime_month 0 3][string range $optime_month 5 6]
	#本月 yyyy-mm
	set opmonth $optime_month	
	#上月 yyyy-mm
	set last_month [GetLastMonth [string range $op_month 0 5]]
	#本月最后一天 yyyymmdd
        set this_month_last_day [string range $op_month 0 5][GetThisMonthDays [string range $op_month 0 5]01]	     
        #本月天数
        set this_month_days [GetThisMonthDays ${op_month}01]
        #本年天数 
        set this_year_days [GetThisYearDays ${op_month}01]
        #上月天数
        set last_month_days [GetThisMonthDays ${last_month}01]
        #上月最后一天 yyyymmdd
        set this_month_first_day [string range $optime_month 0 3][string range $optime_month 5 6]01
        set last_month_last_day [GetLastDay [string range $this_month_first_day 0 7]]   
        
        set DEC_CHECK_VALUE_7 [exec get_kpi.sh $this_month_last_day 2 2]
        puts $DEC_CHECK_VALUE_7
        
        set DEC_CHECK_VALUE_6 [exec get_kpi.sh $last_month 7 2];
        puts $DEC_CHECK_VALUE_6
	return 0
}	