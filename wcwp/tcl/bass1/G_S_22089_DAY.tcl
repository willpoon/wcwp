
######################################################################################################		
#接口名称: 自有增值业务先退费后查证日汇总                                                               
#接口编码：22089                                                                                          
#接口说明：采集自有增值业务“先退费，后查证”服务日报数据，包括“退费笔数、退费金额”等。
#程序名称: G_S_22089_DAY.tcl                                                                            
#功能描述: 生成22089的数据
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
 select      $timestamp TIME_ID
             ,'$timestamp' op_time
             ,char(a.TYCX_QUERY)             qry_cnt
             ,'${RESULT_VAL}'                cancel_cnt
             ,char(a.TYCX_TUIDING_FAIL)      cancel_fail_cnt
             ,char(a.TYCX_TOUSU_LIANG)       complaint_cnt
             ,char( case when (${RESULT_VAL} - a.TYCX_TUIDING_FAIL) < 0 
			then 0 else (${RESULT_VAL} - a.TYCX_TUIDING_FAIL) 
		   end 
		  ) CANCEL_BUSI_CNT
        from  bass2.DW_THREE_ITEM_STAT_DM_$op_month a ,
              (select  replace(char(date(a.create_date)),'-','') op_time
              					,count(0) CANCEL_BUSI_CNT
                       from   
                       	BASS2.DW_PRODUCT_UNITE_CANCEL_ORDER_DM_$op_month a
                        where replace(char(date(a.create_date)),'-','') =  '$timestamp'  
                        group by replace(char(date(a.create_date)),'-','')
                    ) b 
        where replace(char(date(a.create_date)),'-','') = '$timestamp' 
and    replace(char(date(a.create_date)),'-','') = b.op_time
with ur
  "
	exec_sql $sql_buff


  #进行结果数据检查
  #1.检查chkpkunique
  set tabname "G_S_22087_DAY"
	set pk 			"OP_TIME"
	chkpkunique ${tabname} ${pk} ${timestamp}
	#
  aidb_runstats bass1.$tabname 3
  
	return 0
}
