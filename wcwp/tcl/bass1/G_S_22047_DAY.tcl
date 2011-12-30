
######################################################################################################		
#接口名称: 手机充值卡缴费日汇总                                                               
#接口编码：22047                                                                                          
#接口说明：记录手机充值卡充值券缴费金额及冲正金额信息。
#程序名称: G_S_22047_DAY.tcl                                                                            
#功能描述: 生成22047的数据
#运行粒度: DAY
#源    表：1.
#输入参数: void
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：panzw
#编写时间：20111128
#问题记录：
#修改历史: 1. panzw 20111128	1.7.7 newly added
#######################################################################################################   
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {
      
      set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
      puts $timestamp
      set op_month [string range $timestamp 0 5]

      set curr_month [string range $op_time 0 3][string range $op_time 5 6]


  #删除本期数据

	set sql_buff "delete from bass1.G_S_22047_DAY where time_id=$timestamp"
	exec_sql $sql_buff
	
	set sql_buff "
	insert into bass1.G_S_22047_DAY
	select 
	$timestamp time_id
        ,'$timestamp' OP_TIME
        ,'1' CHRG_TYPE
        ,char(bigint(sum(case when OPT_CODE = '4158' then AMOUNT else 0 end ))) CHRG_AMT
        ,char(bigint(sum(case when OPT_CODE = '4159' then AMOUNT else 0 end ))) FAULT_CHRG_AMT
	from BASS2.dw_acct_payment_dm_$curr_month a
        where replace(char(a.OP_TIME),'-','') = '$timestamp' 
	and OPT_CODE in ('4158','4159')
	group by replace(char(a.OP_TIME),'-','')
with ur
  "
	exec_sql $sql_buff


  #进行结果数据检查
  #1.检查chkpkunique
  set tabname "G_S_22047_DAY"
	set pk 			"OP_TIME||CHRG_TYPE"
	chkpkunique ${tabname} ${pk} ${timestamp}
	#
  aidb_runstats bass1.$tabname 3
  
	return 0
}
