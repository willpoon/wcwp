
######################################################################################################		
#接口名称: 垃圾短信黑名单                                                               
#接口编码：22420                                                                                          
#接口说明：该接口为CRM系统经确认、维护的黑名单，传给经营分析系统，进行黑名单客户的分析、跟踪。
#程序名称: G_I_22420_DAY.tcl                                                                            
#功能描述: 生成22420的数据
#运行粒度: DAY
#源    表：1.
#输入参数: void
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：panzw
#编写时间：20110811
#问题记录：
#修改历史: 1. panzw 20110811	1.7.4 newly added
#######################################################################################################   
#“垃圾短信黑名单”表中的“用户标识”都应该在“用户”表中存在
#22420 22421 为中测造数，一经保持原口径上报，报空！ 为满足9点前出数，分两次出。上报的在 9点前出。中测的在9点后出。

proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {



# 通过 while 循环
# set i 0 设置重跑日期上限 0 为 昨日
	##~   set i 1
##~   # 设置重跑日期下限 , $i<= n   ,  n 越大，越久远
	##~   while { $i<=300 } {
	        ##~   set sql_buff "select char(current date - ( 30+31+30+31+15+7+1 - $i ) days) from bass2.dual"
	        ##~   set op_time [get_single $sql_buff]
	
	##~   if { $op_time <= "2012-08-22" } {
	##~   puts $op_time
	##~   p22420 $op_time $optime_month
	
	##~   }
	##~   incr i
	##~   }
	
##~   #set op_time 2011-11-13
p22420 $op_time $optime_month




return 0

}

proc p22420 { op_time optime_month } {
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
	
	set last_day [GetLastDay [string range $timestamp 0 7]]
	 
    puts $this_month_last_day
		global app_name
		set app_name "G_I_22420_DAY.tcl"        


    #删除正式表本月数据
    set sql_buff "delete from bass1.G_I_22420_DAY where time_id=$timestamp"
    exec_sql $sql_buff

##~   alter table bass1.G_I_22420_DAY activate not logged initially with empty table
 


    ##~   set sql_buff "
	##~   insert into G_I_22420_DAY
	##~   select distinct 
		 ##~   $timestamp TIME_ID
        ##~   ,COMPLAINED_NO BLACK_NBR
        ##~   ,USER_ID USER_ID
        ##~   ,replace(char(date(ACCEPT_TIME)),'-','') ADD_DT
        ##~   ,replace(char(time(ACCEPT_TIME)),'.','') ADD_TIME
	##~   from bass2.Sub_refuse_sms_junk_$timestamp
	##~   union all
	##~   select distinct
        ##~   $timestamp TIME_ID
        ##~   ,BLACK_NBR
        ##~   ,USER_ID
        ##~   ,ADD_DT
        ##~   ,ADD_TIME
	##~   from G_I_22420_DAY  where time_id = $last_day
	##~   with ur
	
	##~   "
    ##~   exec_sql $sql_buff


    set sql_buff "
	insert into G_I_22420_DAY
	select distinct 
		 $timestamp TIME_ID
        ,COMPLAINED_NO BLACK_NBR
        ,USER_ID USER_ID
        ,replace(char(date(ACCEPT_TIME)),'-','') ADD_DT
        ,replace(char(time(ACCEPT_TIME)),'.','') ADD_TIME
	from (
			select i.*,row_number()over(partition by user_id order by ACCEPT_TIME asc) rn 
			from bass2.Sub_refuse_sms_junk_$timestamp i
	) o where o.rn = 1
	with ur
	
	"
    exec_sql $sql_buff
	
	return 0
}

