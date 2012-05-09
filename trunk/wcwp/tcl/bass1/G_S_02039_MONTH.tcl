
######################################################################################################		
#接口名称: 疑似养卡用户                                                               
#接口编码：02039                                                                                          
#接口说明：记录渠道疑似养卡用户信息，与接口02039（疑似养卡渠道名单）中各渠道疑似养卡用户数匹配。若一个用户存在多个疑似养卡原因，则按照疑似程度最高的顺序进行上报。
#程序名称: G_S_02039_MONTH.tcl                                                                            
#功能描述: 生成02039的数据
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

      set op_month [string range $optime_month 0 3][string range $optime_month 5 6]
      puts $op_month
      set ThisMonthFirstDay [string range $op_month 0 3][string range $op_time 4 4][string range $op_month 4 5][string range $op_time 4 4]01
      puts $ThisMonthFirstDay      



  #删除本期数据
	set sql_buff "delete from bass1.G_S_02039_MONTH where time_id=$op_month"
	exec_sql $sql_buff

	set sql_buff "

insert into G_S_02039_MONTH(
         TIME_ID
        ,OP_TIME
        ,CHANNEL_ID
        ,USER_ID
        ,MSISDN
        ,YK_REASON
)
select 
         $op_month TIME_ID
        ,'$op_month' OP_TIME
        ,char(b.CHANNEL_ID) CHANNEL_ID
        ,a.USER_ID
        ,c.product_no MSISDN
        ,'01' YK_REASON
from 	BASS1.G_S_02039_MONTH_RPT0135B 	 a
		,bass2.stat_market_0135_b_final b 
		,bass2.dw_product_$op_month c
where a.CHANNEL_ID = b.CHANNEL_ID
and a.user_id = c.user_id 
and  b.WARN_LEVEL in (1,2,3)
and a.time_id = $op_month
and b.OP_TIME = $op_month
with ur
	"
	exec_sql $sql_buff

  aidb_runstats bass1.G_S_02039_MONTH 3


set sql_buff "
	select count(0)
	from  bass1.G_S_02039_MONTH where time_id=$op_month
	and channel_id not in ( select channel_id from G_I_06021_MONTH where  time_id=$op_month )
	with ur
"

chkzero2 $sql_buff "G_S_02039_MONTH has invalid channel_id! "
	
  #1.检查chkpkunique
	set tabname "G_S_02039_MONTH"
	set pk 			"USER_ID"
	chkpkunique ${tabname} ${pk} ${op_month}

	return 0
}


##~   此字段仅取以下分类：
##~   01：放号冲量套利
##~   02：垃圾短信套利
##~   03：SP业务套利
##~   04：自有数据业务套利
##~   05：其他（无法区分时填写）

