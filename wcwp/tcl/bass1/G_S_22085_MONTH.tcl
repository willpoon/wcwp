######################################################################################################
#接口单元名称：收费争议先退费后查证月汇总 
#接口单元编码：22085
#接口单元说明：采集梦网收费争议“先退费，后查证”服务月报数据，包括退费的笔数、退费的SP名称、SP企业代码、退费总额、当月SP实收账款”等。
#程序名称: G_S_22085_MONTH.tcl
#功能描述: 生成22085的数据
#运行粒度: 月
#源    表：1.
#输入参数: 
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：panzhiwei
#编写时间：2011-04-26
#问题记录：1.
#修改历史: 1. 1.7.2 规范
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

      set op_month [string range $optime_month 0 3][string range $optime_month 5 6]
      puts $op_month


  #删除本期数据
	set sql_buff "delete from bass1.G_S_22085_MONTH where time_id=$op_month"
	exec_sql $sql_buff

	set sql_buff "
	insert into bass1.G_S_22085_MONTH
		  (
			 TIME_ID
			,OP_TIME
			,BACK_SP_NAME
			,BACK_SP_CODE
			,BACK_CNT
			,BACK_FEE
			,SP_ACT_INCOME
		  )
	select 
			 $op_month TIME_ID
			,'$op_month' OP_TIME
			,'福州卓龙天讯信息技术有限公司' BACK_SP_NAME
			,'701167' BACK_SP_CODE
			,'0' BACK_CNT
			,'0' BACK_FEE
			,'100' SP_ACT_INCOME
	from sysibm.sysdummy1 a
  "

	exec_sql $sql_buff
	
  #1.检查chkpkunique
	set tabname "G_S_22085_MONTH"
	set pk 			"OP_TIME||BACK_SP_NAME||BACK_SP_CODE"
	chkpkunique ${tabname} ${pk} ${op_month}
	
		
	return 0
}
