
######################################################################################################		
#接口名称: 用户有效通信情况信息                                                               
#接口编码：21022                                                                                          
#接口说明：上报月末最后一天仍在网用户当月每日通信情况。
#程序名称: G_S_21022_MONTH.tcl                                                                            
#功能描述: 生成21022的数据
#运行粒度: MONTH
#源    表：1.
#输入参数: void
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：panzw
#编写时间：20120605
#问题记录：
#修改历史: 1. panzw 20120605	中国移动一级经营分析系统省级数据接口规范 (V1.8.1) 
#######################################################################################################   
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

      set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]      
      set op_month [string range $optime_month 0 3][string range $optime_month 5 6]
      puts $op_month
      #set this_month_last_day [string range $op_month 0 5][GetThisMonthDays [string range $op_month 0 5]01] 
      set last_month [GetLastMonth [string range $op_month 0 5]]
      #set curr_month_first_day [string range $timestamp 0 5]01
      #puts $curr_month_first_day
      #yyyy--mm-dd
      set ThisMonthFirstDay [string range $op_month 0 3][string range $op_time 4 4][string range $op_month 4 5][string range $op_time 4 4]01
      puts $ThisMonthFirstDay      

set this_month_first_day [string range $op_month 0 3][string range $op_time 4 4][string range $op_month 4 5][string range $op_time 4 4]01
puts $this_month_first_day
set this_month_last_day  [string range $op_month 0 3][string range $op_time 4 4][string range $op_month 4 5][string range $op_time 4 4][GetThisMonthDays [string range $op_month 0 5]01]
puts $this_month_last_day

        global app_name
        set app_name "G_S_21022_MONTH.tcl"
          
    #删除本期数据
	set sql_buf "delete from bass1.G_S_21022_MONTH where time_id=$op_month"
	exec_sql $sql_buf
#


	set sql_buf "ALTER TABLE BASS1.G_S_21022_MONTH_2 ACTIVATE NOT LOGGED INITIALLY WITH EMPTY TABLE"
	exec_sql $sql_buf



# 通过 while 循环
# set i 0 设置重跑日期上限 0 为 昨日
	set i 0
# 设置重跑日期下限 , $i<= n   ,  n 越大，越久远
	while { $i<=62 } {
	        set sql_buff "select char(current date - ( 1+$i ) days) from bass2.dual"
	        set i_time [get_single $sql_buff]
	
	if { $i_time >= ${this_month_first_day} && $i_time <= ${this_month_last_day} } {
	puts $i_time
	        set sql_buff "select replace(char(current date - ( 1+$i ) days),'-','') from bass2.dual"
	        set i_timestamp [get_single $sql_buff]
		Deal_21022_cdr $i_timestamp $optime_month
	}
	incr i
	}


	
	set sql_buf "
		insert into G_S_21022_MONTH
		(
         TIME_ID
        ,CALL_DT
        ,USER_ID
        ,ACT_CALLCNT
        ,ACT_CALLDUR
        ,SMS_SENDCNT		
		)
		select
        $op_month TIME_ID
        ,char(a.TIME_ID) CALL_DT
        ,a.USER_ID
        ,char(a.ACT_CALLCNT)
        ,char(a.ACT_CALLDUR)
        ,char(a.SMS_SENDCNT)
		 from  G_S_21022_MONTH_2 a,bass2.dw_product_$op_month b 
	   where b.usertype_id in (1,2,9) 
		 and b.userstatus_id in (1,2,3,6,8)
		 and b.test_mark<>1
		 and a.user_id = b.user_id 
		with ur
	"
	exec_sql $sql_buf





        


  #进行结果数据检查
  #1.检查chkpkunique
  set tabname "G_S_21022_MONTH"
  set pk   "CALL_DT||USER_ID"
        chkpkunique ${tabname} ${pk} ${op_month}
        #
        
  aidb_runstats bass1.G_S_21022_MONTH 3


	return 0
}




proc Deal_21022_cdr { op_time optime_month } {

  #set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
  set timestamp $op_time
  #当天 yyyy-mm-dd
  set optime $op_time

  set op_month [string range $optime_month 0 3][string range $optime_month 5 6]
  
##~   set sql_buff "delete from G_S_21022_MONTH_1 where TIME_ID = $timestamp"
##~   exec_sql $sql_buff

	set sql_buf "ALTER TABLE BASS1.G_S_21022_MONTH_1 ACTIVATE NOT LOGGED INITIALLY WITH EMPTY TABLE"
	exec_sql $sql_buf
	
set sql_buff "


	insert into G_S_21022_MONTH_1
	(
         TIME_ID
        ,USER_ID
        ,ACT_CALLCNT
        ,ACT_CALLDUR
	)
	select 
	$timestamp time_id
	,a.user_id USER_ID
	,sum(BILL_CALL_COUNTS) ACT_CALLCNT
	,sum(BILL_DURATION) ACT_CALLDUR
	from  bass2.DW_CALL_$timestamp a
	where CALLTYPE_ID = 0
	group by a.user_id 
with ur
"
exec_sql $sql_buff



set sql_buff "
	insert into G_S_21022_MONTH_1
	(
         TIME_ID
        ,USER_ID
        ,SMS_SENDCNT
	)	
	select 
	$timestamp TIME_ID
	,a.user_id USER_ID
	,sum(BILL_COUNTS) SMS_SENDCNT
	from  bass2.DW_NEWBUSI_SMS_$timestamp a
	where CALLTYPE_ID = 0
	group by a.user_id 
with ur
"
exec_sql $sql_buff




set sql_buff "

insert into G_S_21022_MONTH_2
(
         TIME_ID
        ,USER_ID
        ,ACT_CALLCNT
        ,ACT_CALLDUR
        ,SMS_SENDCNT
)
select 
         TIME_ID
        ,USER_ID
        ,sum(value(ACT_CALLCNT,0))
        ,sum(value(ACT_CALLDUR,0))
        ,sum(value(SMS_SENDCNT,0))
from 	G_S_21022_MONTH_1
group by 
TIME_ID
,USER_ID
with ur
"
exec_sql $sql_buff

	

return 0

}
