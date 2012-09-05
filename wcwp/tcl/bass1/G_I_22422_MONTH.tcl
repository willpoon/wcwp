
######################################################################################################		
#接口名称: 经分系统识别高风险用户名单                                                               
#接口编码：22422                                                                                          
#接口说明：经营分析系统将生成的高风险用户名单传给CRM系统，由垃圾短信监控平台根据名单对疑似黑名单客户进行优先级过滤
#程序名称: G_I_22422_MONTH.tcl                                                                            
#功能描述: 生成22422的数据
#运行粒度: MONTH
#源    表：1.
#输入参数: void
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：panzw
#编写时间：20111129
#问题记录：
#修改历史: 1. panzw 20111129	垃圾短信1.2 newly added
#######################################################################################################   

proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {



# 通过 while 循环
# set i 0 设置重跑日期上限 0 为 昨日
	##~   set i 1
##~   # 设置重跑日期下限 , $i<= n   ,  n 越大，越久远
	##~   while { $i<=30 } {
	        ##~   set sql_buff "select substr(char(current date - (20 - $i ) months ),1,7) from bass2.dual"
	        ##~   set optime_month [get_single $sql_buff]
	
	##~   if { $optime_month <= "2012-07" } {
	##~   puts $optime_month
	##~   p22422 $op_time $optime_month
	
	##~   }
	##~   incr i
	##~   }
	
#set op_time 2011-11-13
##~   p22421 $op_time $optime_month




return 0

}

##~   经分系统识别高风险用户名单 (201204-201207)
##~   bass2.sub_refuse_sms_danger_yyyymm 查了下此期间无数据。
 
 
proc p22422 { op_time optime_month } {
#set op_time 2011-06-07
   #当天 yyyymmdd
    set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
		
    #当天 yyyy-mm-dd
    set optime $op_time

    #本月 yyyymm
    set op_month [string range $optime_month 0 3][string range $optime_month 5 6]
    puts $op_month
	set last_month [GetLastMonth [string range $op_month 0 5]]
    #本月第一天 yyyy-mm-dd
    set this_month_first_day [string range $op_month 0 3][string range $op_time 4 4][string range $op_month 4 5][string range $op_time 4 4]01
    puts $this_month_first_day
		#自然月
		set curr_month [string range $op_time 0 3][string range $op_time 5 6]
    #本月最后一天 yyyy-mm-dd
    set this_month_last_day [string range $op_month 0 3][string range $op_time 4 4][string range $op_month 4 5][string range $op_time 4 4][GetThisMonthDays [string range $op_month 0 5]01]
    puts $this_month_last_day
		global app_name
		set app_name "G_I_22422_MONTH.tcl"        


    #删除正式表本月数据
    set sql_buff "delete from bass1.G_I_22422_MONTH where time_id=$timestamp"
    exec_sql $sql_buff




    ##~   set sql_buff "
	##~   insert into G_I_22422_MONTH
	##~   select 
         ##~   $op_month TIME_ID
        ##~   ,PRODUCT_NO
        ##~   ,USER_ID
        ##~   ,'$op_month' ADD_MONTH
	##~   from bass2.sub_refuse_sms_danger_$op_month a
	##~   union all
	##~   select
	     ##~   $op_month TIME_ID
        ##~   ,PRODUCT_NO
        ##~   ,USER_ID
        ##~   ,ADD_MONTH
	##~   from G_I_22422_MONTH
	##~   where time_id = $last_month
	##~   with ur	
	##~   "
    ##~   exec_sql $sql_buff



    set sql_buff "
	insert into G_I_22422_MONTH
	select 
         $op_month TIME_ID
        ,PRODUCT_NO
        ,USER_ID
        ,'$op_month' ADD_MONTH
	from bass2.sub_refuse_sms_danger_$op_month a
	with ur	
	"
    exec_sql $sql_buff
	
	
	return 0
}


