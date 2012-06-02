
######################################################################################################		
#接口名称: 增值业务1111订购服务日汇总                                                               
#接口编码：22087                                                                                          
#接口说明：采集增值业务1111便捷订购服务的日报数据，包括“客户短信查询量、短信回复量、订购失败量、客户订购业务数量”等;可计算出“订购率、订购失败率”等（订购率=客户订购业务数量/客户短信查询量；订购失败率=订购失败量/短信回复量）。
#程序名称: G_S_22087_DAY.tcl                                                                            
#功能描述: 生成22087的数据
#运行粒度: DAY
#源    表：1.
#输入参数: void
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：panzw
#编写时间：20120507
#问题记录：
#修改历史: 1. panzw 20120507	中国移动一级经营分析系统省级数据接口规范 (V1.8.0) 
#######################################################################################################   
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {
      ##~   set op_time  2012-05-02
      set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
      puts $timestamp
      set op_month [string range $timestamp 0 5]

      set curr_month [string range $op_time 0 3][string range $op_time 5 6]


  #删除本期数据
	set sql_buff "delete from bass1.G_S_22087_DAY where time_id=$timestamp"
	exec_sql $sql_buff
	
  #直接来源于二经用户表数据，新的接口表
  #更新业务退订量CANCEL_BUSI_CNT（可能不是很准，规范指的是每次查询后，发起所退订的业务量?）
  #CANCEL_CNT口径也要重新确认。
	set sql_buff "
	insert into bass1.G_S_22087_DAY
		  (
			 TIME_ID
			,OP_TIME
			,SMS_QUERY_CNT
			,SMS_REPLY_CNT
			,ORDER_FAIL_CNT
			,ORDER_CNT
		  )
	 select        $timestamp TIME_ID
				 ,'$timestamp' OP_TIME
				 ,char((	select count(0) from  bass2.dw_kf_sms_cmd_receive_dm_$curr_month 
					where cmd_id =600997 and OP_TIME = '$op_time'
				  ))  SMS_QUERY_CNT
				 ,char(count(distinct a.PRODUCT_NO||a.SP_CODE ))  SMS_REPLY_CNT
				 ,char(count(distinct case when  a.STS = 0  then  a.PRODUCT_NO||a.SP_CODE  end ))    ORDER_FAIL_CNT
				 ,char(count(distinct case when  a.STS = 1  then  a.PRODUCT_NO||a.SP_CODE  end ))    ORDER_CNT
		from   bass2.dw_product_unite_order_dm_$curr_month a
		where OP_TIME = '$op_time' and date(CREATE_DATE) =  '$op_time'
	with ur
  "
	exec_sql $sql_buff

##~   （订购率=客户订购业务数量/客户短信查询量；订购失败率=订购失败量/短信回复量）。

set sql_buff "
select sum(val) from (
	select case when  bigint(SMS_QUERY_CNT)  < bigint(ORDER_CNT) then 1 else 0 end val from bass1.G_S_22087_DAY where time_id = $timestamp
) t
"
chkzero2 $sql_buff "数据不合逻辑"


set sql_buff "
select sum(val) from (
	select case when  bigint(SMS_REPLY_CNT)  < bigint(ORDER_FAIL_CNT) then 1 else 0 end val from bass1.G_S_22087_DAY where time_id = $timestamp
) t
"
chkzero2 $sql_buff "数据不合逻辑"


  #进行结果数据检查
  #1.检查chkpkunique
  set tabname "G_S_22087_DAY"
	set pk 			"OP_TIME"
	chkpkunique ${tabname} ${pk} ${timestamp}
	#
  aidb_runstats bass1.$tabname 3
  
	return 0
}
