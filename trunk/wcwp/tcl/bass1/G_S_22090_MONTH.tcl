
######################################################################################################		
#接口名称: 自有增值业务先退费后查证月汇总                                                               
#接口编码：22090                                                                                          
#接口说明：采集自有增值业务“先退费，后查证”服务月报数据，区分包月业务和点播业务。
#程序名称: G_S_22090_MONTH.tcl                                                                            
#功能描述: 生成22090的数据
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
	set sql_buff "delete from bass1.G_S_22090_MONTH where time_id=$op_month"
	exec_sql $sql_buff
		set sql_buff "
			INSERT INTO  BASS1.G_S_22090_MONTH
			( 
				 TIME_ID
				,OP_TIME
				,BUSI_CODE
				,BUSI_NAME
				,BILLING_TYPE
				,SP_CODE
				,SP_NAME
				,ONLINE_CNT
				,TUIFEI_CNT
				,TUIFEI
			)
			 
select 
				 $op_month TIME_ID
				,'$op_month' OP_MONTH
				,'700002614000' BUSI_CODE
				,'魔戒传说T' BUSI_NAME
				,BILLING_TYPE
				,'701167' SP_CODE
				,'福州卓龙天讯信息技术有限公司' SP_NAME
				,ONLINE_CNT
				,TUIFEI_CNT
				,TUIFEI
		from bass2.dual with ur 
	"
	exec_sql $sql_buff

  aidb_runstats bass1.G_S_22090_MONTH 3

  #1.检查chkpkunique
	set tabname "G_S_22090_MONTH"
	set pk 			"OP_TIME||BUSI_CODE||SP_CODE"
	chkpkunique ${tabname} ${pk} ${op_month}
	
	
	return 0
}
