
######################################################################################################		
#接口名称: 重入网用户                                                               
#接口编码：02038                                                                                          
#接口说明：上报当月判定为重入网用户的用户信息
#程序名称: G_S_02038_MONTH.tcl                                                                            
#功能描述: 生成02038的数据
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
	set sql_buff "delete from bass1.G_S_02038_MONTH where time_id=$op_month"
	exec_sql $sql_buff

	set sql_buff "

insert into G_S_02038_MONTH(
         TIME_ID
        ,USER_ID
        ,MSISDN
        ,OLD_USER_ID
        ,OLD_MSISDN
)
select 
         $op_month TIME_ID
        ,'$op_month' OP_TIME
        ,char(b.CHANNEL_ID) CHANNEL_ID
        ,a.USER_ID
        ,c.product_no MSISDN
        ,'01' YK_REASON
from 	BASS1.G_S_02038_MONTH_RPT0135B 	 a
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

  aidb_runstats bass1.G_S_02038_MONTH 3


set sql_buff "
	select count(0)
	from  bass1.G_S_02038_MONTH where time_id=$op_month
	and channel_id not in ( select channel_id from G_I_06021_MONTH where  time_id=$op_month )
	with ur
"

chkzero2 $sql_buff "G_S_02038_MONTH has invalid channel_id! "
	
  #1.检查chkpkunique
	set tabname "G_S_02038_MONTH"
	set pk 			"USER_ID"
	chkpkunique ${tabname} ${pk} ${op_month}

	return 0
}


##~   重入网客户  	重入网是指客户已经拥有中国移动某个地市分公司一个移动号码，由于某种原因又新买了中国移动该地市分公司的另一个移动号码入网，新号码以全部或者部分替代原有号码，这样的现象称为重入网，这样的客户称为重入网客户。其中，重入网客户不包含一卡双号客户
##~   多次重入网客户 	半年内发生两次以上(含两次)重入网的客户，是重入网客户子集

