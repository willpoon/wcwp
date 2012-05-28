
######################################################################################################		
#接口名称: 增值业务1111订购服务月汇总                                                               
#接口编码：22088                                                                                          
#接口说明：采集增值业务1111便捷订购服务的月报数据，包括当月“客户退订的业务名称、业务代码、业务提供商、业务退订量、业务在网客户数”等。
#程序名称: G_S_22088_MONTH.tcl                                                                            
#功能描述: 生成22088的数据
#运行粒度: MONTH
#源    表：1.
#输入参数: void
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：panzw
#编写时间：20120507
#问题记录：
#修改历史: 1. panzw 20120507	中国移动一级经营分析系统省级数据接口规范 (V1.8.0) 
#######################################################################################################   
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

	  ##~   set op_month 2012-05
	  ##~   set op_time 2012-06-01
	  
      set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]      
      set op_month [string range $optime_month 0 3][string range $optime_month 5 6]
	  
      puts $op_month
      #set this_month_last_day [string range $op_month 0 5][GetThisMonthDays [string range $op_month 0 5]01] 
      set last_month [GetLastMonth [string range $op_month 0 5]]
      #set curr_month_first_day [string range $timestamp 0 5]01
      #puts $curr_month_first_day
      set ThisMonthFirstDay [string range $op_month 0 3][string range $op_time 4 4][string range $op_month 4 5][string range $op_time 4 4]01
      puts $ThisMonthFirstDay      


  #删除本期数据
	set sql_buff "delete from bass1.G_S_22088_MONTH where time_id=$op_month"
	exec_sql $sql_buff
	
##~   G_S_22083_MONTH_3  是 sp 信息表！

set sql_buff "
	insert into bass1.G_S_22088_MONTH
	(
			 TIME_ID
			,OP_TIME
			,BUSI_CODE
			,BUSI_NAME
			,SP_NAME
			,ORDER_CNT
			,ONLINE_CNT
	)
	select 
			 $op_month TIME_ID
			,'$op_month' OP_TIME
			,a.SP_CODE BUSI_CODE
			,a.NAME BUSI_NAME
			,b.SP_NAME SP_NAME
			,char(count(distinct a.PRODUCT_NO||a.SP_CODE)) ORDER_CNT
			,char(count(distinct c.PRODUCT_NO )) ONLINE_CNT
	from  bass2.dw_product_unite_order_dm_$op_month a 
	join  BASS1.G_S_22083_MONTH_3  b on char(a.sp_id ) = b.SP_CODE
	left join (select PRODUCT_NO from bass2.dw_product_$op_month  where usertype_id in (1,2,9) 
						 and userstatus_id  in (1,2,3,6,8)
						 and test_mark<>1) c on a.PRODUCT_NO = c.PRODUCT_NO
	where a.STS = 1 
		  and substr(char(date(a.CREATE_DATE)),1,7) =  '$optime_month'
		  and a.SP_CODE <> '0'
	group by 
			 a.SP_CODE 
			,a.NAME 
			,b.SP_NAME	
	with ur
	"

exec_sql $sql_buff


  aidb_runstats bass1.G_S_22088_MONTH 3	  

	
  #1.检查chkpkunique
	set tabname "G_S_22088_MONTH"
	set pk 			"OP_TIME||BUSI_CODE||SP_NAME"
	chkpkunique ${tabname} ${pk} ${op_month}


##~   “订购用户数<=0 ”
set sql_buff "
select count(0) from   G_S_22088_MONTH
where bigint(ORDER_CNT) <= 0
and time_id = $op_month
"

chkzero 	$sql_buff 1


	return 0
}
