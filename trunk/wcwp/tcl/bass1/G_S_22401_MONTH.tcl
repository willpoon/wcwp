
######################################################################################################		
#接口名称: 用户本地GPRS使用位置信息月汇总                                                               
#接口编码：22401                                                                                          
#接口说明："基于本地GPRS业务详单，对所有我网用户发生的通话按基站小区进行业务量汇总。注：不仅仅是校园基站。"
#程序名称: G_S_22401_MONTH.tcl                                                                            
#功能描述: 生成22401的数据
#运行粒度: MONTH
#源    表：1.
#输入参数: void
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：panzw
#编写时间：20110807
#问题记录：
#修改历史: 1. panzw 20110807	1.7.4 newly added
#######################################################################################################   

proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

	#当天 yyyymmdd
  set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
  #当天 yyyy-mm-dd
  set optime $op_time

  set op_month [string range $optime_month 0 3][string range $optime_month 5 6]

  #程序名
  global app_name
  set app_name "G_S_22401_MONTH.tcl"
        
  #删除本期数据
	set sql_buff "delete from bass1.G_S_22401_MONTH where time_id=$op_month"
  exec_sql $sql_buff


  set sql_buff "
  insert into G_S_22401_MONTH
  (
         TIME_ID
        ,USER_ID
        ,CELL_ID
        ,LAC_ID
        ,CALL_CNT
  )
select  
	$op_month TIME_ID
        ,a.USER_ID
	,value(a.CELL_ID,'0')
	,value(a.LAC_ID,'0')
        ,char(sum(CALL_CNT)) CALL_CNT
from   bass1.int_22401_$op_month a
where a.TIME_ID/100 = $op_month
	and a.user_id is not null 
group by 
	USER_ID
	,value(a.CELL_ID,'0')
	,value(a.LAC_ID,'0')
  "
  exec_sql $sql_buff
  
  #进行结果数据检查
  #1.检查chkpkunique
  set tabname "G_S_22401_MONTH"
  set pk   "USER_ID||CELL_ID||LAC_ID"
  chkpkunique ${tabname} ${pk} ${op_month}
  #
  aidb_runstats bass1.$tabname 3
    
           
	return 0
}