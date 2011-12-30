
######################################################################################################		
#接口名称: "疑似窜卡渠道名单"                                                               
#接口编码：22059                                                                                          
#接口说明："记录疑似窜卡异常情况的渠道及用户情况，参照《渠道管理运营省级经营分析系统二期支撑方案》中窜卡预警模型。"
#程序名称: G_S_22059_MONTH.tcl                                                                            
#功能描述: 生成22059的数据
#运行粒度: MONTH
#源    表：1.
#输入参数: void
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：panzw
#编写时间：20110727
#问题记录：
#修改历史: 1. panzw 20110727	1.7.4 newly added
#######################################################################################################   
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

      set op_month [string range $optime_month 0 3][string range $optime_month 5 6]
      puts $op_month
      set ThisMonthFirstDay [string range $op_month 0 3][string range $op_time 4 4][string range $op_month 4 5][string range $op_time 4 4]01
      puts $ThisMonthFirstDay      



  #删除本期数据
	set sql_buff "delete from bass1.G_S_22059_MONTH where time_id=$op_month"
	exec_sql $sql_buff

	set sql_buff "
			INSERT INTO  BASS1.G_S_22059_MONTH
			( 
				 TIME_ID
				,OP_TIME
				,CHANNEL_ID
				,BRAND
				,CK_CNT
			)
			select 
				$op_month TIME_ID
				,'$op_month' OP_TIME
				,char(a.CHANNEL_ID) CHANNEL_ID
				,char(b.BRAND_id) BRAND
				,char(b.USER_CNT) CK_CNT
			from bass2.stat_market_0135_a_final a
			, bass1.G_S_22059_MONTH_MRKT_0135A b 
			where a.OP_TIME = $op_month
			and b.op_time = $op_month
			and a.CHANNEL_ID = b.CHANNEL_ID
			 with ur 
	"
	exec_sql $sql_buff

  aidb_runstats bass1.G_S_22059_MONTH 3

set sql_buff "
	select count(0)
	from  bass1.G_S_22059_MONTH where time_id=$op_month
	and channel_id not in ( select channel_id from G_I_06021_MONTH where  time_id=$op_month )
	with ur
"

chkzero2 $sql_buff "G_S_22059_MONTH has invalid channel_id! "

  #1.检查chkpkunique
	set tabname "G_S_22059_MONTH"
	set pk 			"OP_TIME||BRAND||CHANNEL_ID"
	chkpkunique ${tabname} ${pk} ${op_month}
	
	return 0
}