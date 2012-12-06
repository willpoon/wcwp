
######################################################################################################		
#接口名称: 移动400业务量日汇总                                                               
#接口编码：22305                                                                                          
#接口说明：本接口上报移动400业务的业务量日使用情况。
#程序名称: G_S_22305_DAY.tcl                                                                            
#功能描述: 生成22305的数据
#运行粒度: DAY
#源    表：1.
#输入参数: void
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：panzw
#编写时间：20120801
#问题记录：
#修改历史: 1. panzw 20120801	中国移动一级经营分析系统省级数据接口规范 (V1.8.2) 
#######################################################################################################   

proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {



# 通过 while 循环
# set i 0 设置重跑日期上限 0 为 昨日
	##~   set i 1
##~   # 设置重跑日期下限 , $i<= n   ,  n 越大，越久远
	##~   while { $i<=100 } {
	        ##~   set sql_buff "select char(current date - ( 1+$i ) days) from bass2.dual"
	        ##~   set op_time [get_single $sql_buff]
	
	##~   if { $op_time >= "2012-07-01" } {
	##~   puts $op_time
	##~   Deal_22305_day $op_time $optime_month
	
	##~   }
	##~   incr i
	##~   }
	
	Deal_22305_day $op_time $optime_month


return 0
}



proc Deal_22305_day { op_time optime_month } {

	      #当天 yyyymmdd

        set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]       

        #当天 yyyy-mm-dd

        set optime $op_time

        #今天的日期，格式dd(例：输入20070411 返回11)

        set today_dd [string range $op_time 8 9]

        #本月 yyyymm

        set op_month [string range $op_time 0 3][string range $op_time 5 6]

        #删除本期数据

	set sql_buff "delete from bass1.G_S_22305_DAY where time_id=$timestamp"
	exec_sql $sql_buff
	
	set sql_buff "
insert into G_S_22305_DAY
select 
        $timestamp TIME_ID
        ,a.cust_id ENTERPRISE_ID
        ,a.product_no NUMBER400
        ,char(value(UPMESSAGE,0))
        ,char(value(DOWNMESSAGE,0))
        ,char(LOCAL_IN_COUNTS)
        ,char(CHANG_IN_COUNTS)
        ,char(LOCAL_OUT_COUNTS)
        ,char(CHANG_OUT_COUNTS)
        ,char(LOCAL_IN_DUR)
        ,char(CHANG_IN_DUR)
        ,char(LOCAL_OUT_DUR)
        ,char(CHANG_OUT_DUR)
		from bass2.dw_product_$timestamp  a
		 join table(
		select 
			PRODUCT_NO
			,sum(case when  CALLTYPE_ID = 1 and TOLLTYPE_ID  = 0 then CALL_COUNTS else 0 end) LOCAL_IN_COUNTS
			,sum(case when  CALLTYPE_ID = 1 and TOLLTYPE_ID <> 0 then CALL_COUNTS else 0 end) CHANG_IN_COUNTS
			,sum(case when  CALLTYPE_ID = 0 and TOLLTYPE_ID  = 0 then CALL_COUNTS else 0 end) LOCAL_OUT_COUNTS
			,sum(case when  CALLTYPE_ID = 0 and TOLLTYPE_ID <> 0 then CALL_COUNTS else 0 end) CHANG_OUT_COUNTS
			,sum(case when  CALLTYPE_ID = 1 and TOLLTYPE_ID  = 0 then CALL_DURATION_M else 0 end) LOCAL_IN_DUR
			,sum(case when  CALLTYPE_ID = 1 and TOLLTYPE_ID <> 0 then CALL_DURATION_M else 0 end) CHANG_IN_DUR
			,sum(case when  CALLTYPE_ID = 0 and TOLLTYPE_ID  = 0 then CALL_DURATION_M else 0 end) LOCAL_OUT_DUR
			,sum(case when  CALLTYPE_ID = 0 and TOLLTYPE_ID <> 0 then CALL_DURATION_M else 0 end) CHANG_OUT_DUR
			from bass2.dw_call_$timestamp			
			where product_no like '4001%'
			group by PRODUCT_NO
		) b on a.product_no = b.PRODUCT_NO
		left join table(		
			select PRODUCT_NO
					,sum(case when  CALLTYPE_ID = 0 then  COUNTS else 0 end) UPMESSAGE
					,sum(case when  CALLTYPE_ID = 0 then  COUNTS else 0 end) DOWNMESSAGE
			from bass2.dw_newbusi_sms_$timestamp
			where product_no like '4001%'
			group by PRODUCT_NO
		) c on a.product_no = c.PRODUCT_NO
		where USERTYPE_ID = 8
		and USERSTATUS_ID in (1,2,3,6,8)
		and a.test_mark = 0
		with ur
		
"		
	exec_sql $sql_buff


set sql_buff "
select count(0)
FROM (
select ENTERPRISE_ID
from G_S_22305_DAY WHERE TIME_ID = $timestamp
EXCEPT
select ENTERPRISE_ID FROM G_A_01004_DAY 
) T

"

chkzero2 $sql_buff "invalid ENTERPRISE_ID !"
	return 0      


}

		




##~   select PRODUCT_NO
        ##~   ,sum(case when  CALLTYPE_ID = 0  COUNTS else 0 end) UPMESSAGE
        ##~   ,sum(case when  CALLTYPE_ID = 0  COUNTS else 0 end) DOWNMESSAGE
##~   from bass2.dw_newbusi_sms_20120801
##~   where product_no like '4001%'
##~   group by PRODUCT_NO

