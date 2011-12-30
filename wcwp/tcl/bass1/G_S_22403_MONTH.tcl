
######################################################################################################		
#接口名称: 校园区域本网用户语音交往圈                                                               
#接口编码：22403                                                                                          
#接口说明："与校园区域本网用户发生语音通话行为的所有的对端号码（包含竞争对手号码）集合。"
#程序名称: G_S_22403_MONTH.tcl                                                                            
#功能描述: 生成22403的数据
#运行粒度: MONTH
#源    表：1.
#输入参数: void
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：panzw
#编写时间：20110807
#问题记录：
#修改历史: 1. panzw 20110807	1.7.4 newly added
#######################################################################################################   

proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

	#当天 yyyymmdd
  set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
  #当天 yyyy-mm-dd
  set optime $op_time

  set op_month [string range $optime_month 0 3][string range $optime_month 5 6]
set this_month_first_day [string range $op_month 0 3][string range $op_time 4 4][string range $op_month 4 5][string range $op_time 4 4]01
puts $this_month_first_day
set this_month_last_day  [string range $op_month 0 3][string range $op_time 4 4][string range $op_month 4 5][string range $op_time 4 4][GetThisMonthDays [string range $op_month 0 5]01]
puts $this_month_last_day
#程序名
  global app_name
  set app_name "G_S_22403_MONTH.tcl"
        
  #删除本期数据
	set sql_buff "delete from bass1.G_S_22403_MONTH where time_id=$op_month"
  exec_sql $sql_buff



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
	Deal_22403_cdr $i_timestamp $optime_month
	}
	incr i
	}
	

set sql_buff "ALTER TABLE g_s_22403_month_2 ACTIVATE NOT LOGGED INITIALLY WITH EMPTY TABLE"
exec_sql $sql_buff


set sql_buff "
insert into g_s_22403_month_2
select 
	$op_month
	,user_id
	,product_no
	,school_name
	,opposite_no
	,sum(call_counts)
	,sum(a.call_duration_s) 
	,sum(a.call_duration_m)
from g_s_22403_month_1 a
where OP_TIME/100 = $op_month
group by  user_id
	,product_no
	,school_name
	,opposite_no
with ur
"
exec_sql $sql_buff
			

  set sql_buff "
  insert into G_S_22403_MONTH
  (
         TIME_ID
        ,USER_ID
        ,OPP_NBR
        ,CALL_CNT
        ,CALL_DUR
  )
select  
	$op_month TIME_ID
        ,a.USER_ID
	,opposite_no
        ,char(sum(call_counts)) CALL_CNT
	,char(sum(call_duration_s)) CALL_DUR
from   g_s_22403_month_2 a
where op_time = $op_month
and a.user_id is not null 
and a.opposite_no is not null
group by 
        a.USER_ID
	,opposite_no
with ur
  "
  exec_sql $sql_buff
  
  #进行结果数据检查
  #1.检查chkpkunique
  set tabname "G_S_22403_MONTH"
  set pk   "USER_ID||OPP_NBR"
  chkpkunique ${tabname} ${pk} ${op_month}
  #
  aidb_runstats bass1.$tabname 3
    
           
	return 0
}

proc Deal_22403_cdr { op_time optime_month } {

  #set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
  set timestamp $op_time
  #当天 yyyy-mm-dd
  set optime $op_time

  set op_month [string range $optime_month 0 3][string range $optime_month 5 6]
  
set sql_buff "delete from g_s_22403_month_1 where op_time = $timestamp"
exec_sql $sql_buff

set sql_buff "
insert into g_s_22403_month_1
		    	  select
				 $timestamp,
		    	  	 b.user_id,
		    	  	 b.product_no,
		    	  	 b.school_name,
		    	  	 a.opp_regular_number,
		    	  	 count(1),
				 sum(a.CALL_DURATION) ,
		    	  	 sum(a.call_duration_m)
		    	  from bass2.cdr_call_dtl_$timestamp a inner join bass2.dw_xysc_school_real_user_dt_$op_month b
		    	    on a.user_id=b.user_id and a.product_no=b.product_no
		    	  group by b.user_id,
		    	  	 b.product_no,
		    	  	 b.school_name,
		    	  	 a.opp_regular_number
with ur				 
"
exec_sql $sql_buff


}
