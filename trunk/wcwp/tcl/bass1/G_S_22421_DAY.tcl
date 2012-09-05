
######################################################################################################		
#接口名称: 垃圾短信疑似黑名单处理结果                                                               
#接口编码：22421                                                                                          
#接口说明：该接口为CRM系统的疑似黑名单经人工确认的处理日志，传给经营分析系统。
#程序名称: G_S_22421_DAY.tcl                                                                            
#功能描述: 生成22421的数据
#运行粒度: DAY
#源    表：1.
#输入参数: void
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：panzw
#编写时间：20110811
#问题记录：
#修改历史: 1. panzw 20110811	1.7.4 newly added
#######################################################################################################   
#“垃圾短信疑似黑名单处理结果”表中的“用户标识”都应该在“用户”表中存在


proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {



# 通过 while 循环
# set i 0 设置重跑日期上限 0 为 昨日
	##~   set i 1
# 设置重跑日期下限 , $i<= n   ,  n 越大，越久远
	##~   while { $i<=200 } {
	        ##~   set sql_buff "select char(current date - ( 30+31+30+31+15+7+1 - $i ) days) from bass2.dual"
	        ##~   set op_time [get_single $sql_buff]
	
	##~   if { $op_time <= "2012-08-22" } {
	##~   puts $op_time
	##~   p22421 $op_time $optime_month
	
	##~   }
	##~   incr i
	##~   }
	
#set op_time 2011-11-13
p22421 $op_time $optime_month




return 0

}

proc p22421 { op_time optime_month } {
#set op_time 2011-06-07
   #当天 yyyymmdd
    set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
		
    #当天 yyyy-mm-dd
    set optime $op_time

    #本月 yyyymm
    set op_month [string range $optime_month 0 3][string range $optime_month 5 6]
    puts $op_month

    #本月第一天 yyyy-mm-dd
    set this_month_first_day [string range $op_month 0 3][string range $op_time 4 4][string range $op_month 4 5][string range $op_time 4 4]01
    puts $this_month_first_day
		#自然月
		set curr_month [string range $op_time 0 3][string range $op_time 5 6]
    #本月最后一天 yyyy-mm-dd
    set this_month_last_day [string range $op_month 0 3][string range $op_time 4 4][string range $op_month 4 5][string range $op_time 4 4][GetThisMonthDays [string range $op_month 0 5]01]
    puts $this_month_last_day
		global app_name
		set app_name "G_S_22421_DAY.tcl"        


    #删除正式表本月数据
    set sql_buff "delete from bass1.G_S_22421_DAY where time_id=$timestamp"
    exec_sql $sql_buff


 ##~   垃圾短信疑似黑名单处理结果 （20120401-20120815）
##~   Sub_refuse_sms_deal_yyyymmdd 
##~   G_S_22421_DAY

##~   监控起始时间、监控结束时间、主叫号码、被叫号码、违规原因、超标数目、MSGID、内容类型、处理状态、处理结果、处理时间。

         ##~   ACCEPT_TIME
        ##~   ,RESULT_TIME
        ##~   ,USER_ID
        ##~   ,TARGETNO
        ##~   ,REASON
        ##~   ,SUM
        ##~   ,PROC_STATUS
        ##~   ,DEAL_RESULT
        ##~   ,SUBMIT_TIME
		
##~   记录行号
##~   监控起始日期
##~   监控结束日期
##~   用户标识
##~   疑似黑名单号码
##~   违规原因
##~   超标数目
##~   处理状态
##~   处理结果


    set sql_buff "
	insert into G_S_22421_DAY
	select 
		 $timestamp TIME_ID
        ,substr(ACCEPT_TIME,1,8) MON_BEGIN_DT
        ,'$timestamp' MON_END_DT
        ,USER_ID USER_ID
        ,TARGETNO PRODUCT_NO
        ,case when REASON is null or REASON = '' or REASON = ' ' then '5' else REASON end ILLEGAL_REASON
        ,case when SUM is null or SUM = '' or SUM = ' ' then '0' else SUM end OVER_CNT
        ,'1' DEAL_STS
        ,DEAL_RESULT DEAL_RESULT
        ,substr(SUBMIT_TIME,1,8) DEAL_DT
	from bass2.Sub_refuse_sms_deal_$timestamp
	where substr(SUBMIT_TIME,1,8) = '$timestamp'
	with ur
	"
    exec_sql $sql_buff
	
	return 0
}


