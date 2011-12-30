######################################################################################################
#接口单元名称：收费争议先退费后查证日汇总 
#接口单元编码：22084
#接口单元说明：采集梦网收费争议“先退费，后查证”服务日报数据，包括“退费笔数、退费金额”等。
#程序名称: G_S_22084_DAY.tcl
#功能描述: 生成22084的数据
#运行粒度: 日
#源    表：1.
#输入参数: 
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：panzhiwei
#编写时间：2011-04-26
#问题记录：1.
#修改历史: 1. 1.7.2 规范
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

      set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
      puts $timestamp



  #删除本期数据
	set sql_buff "delete from bass1.G_S_22084_DAY where time_id=$timestamp"
	exec_sql $sql_buff


#  #来源于二经缴费表数据
#	set sql_buff "
#	insert into bass1.G_S_22084_DAY
#		  (
#         TIME_ID
#        ,OP_TIME
#        ,BACK_CNT
#        ,BACK_FEE
#		  )
#		select 
#					$timestamp 	time_id
#					,replace(char(date(a.OP_TIME)),'-','')  OP_TIME
#				--,char(count(0)) back_cnt	--为了与月保持一致，置0
#				--,char(bigint(sum(amount))) back_fee --为了与月保持一致，置0
#				,'0' back_cnt
#				,'0' back_fee
#		from bass2.DW_ACCT_PAYMENT_DM_$op_month a
#		where a.remarks like '%SP退费%'
#				and replace(char(date(a.OP_TIME)),'-','') = '$timestamp'
#		group by replace(char(date(a.OP_TIME)),'-','')
#  "

# 	puts $sql_buff
#	exec_sql $sql_buff


  #来源于二经缴费表数据 --为了与月接口22085保持一致，back_cnt,back_fee置0
	set sql_buff "
	insert into bass1.G_S_22084_DAY
		  (
			 TIME_ID
			,OP_TIME
			,BACK_CNT
			,BACK_FEE
		  )
		select 
			$timestamp 	time_id		
			,'$timestamp' 	OP_TIME
			,'0' back_cnt	
			,'0' back_fee  
	from sysibm.sysdummy1 a
  "
	exec_sql $sql_buff
	
	return 0
}
