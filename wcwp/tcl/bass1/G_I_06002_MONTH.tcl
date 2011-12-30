######################################################################################################
#接口名称：校园位置信息
#接口编码：06002
#接口说明：
#程序名称: G_I_06002_MONTH.tcl
#功能描述:  
#运行粒度: 日
#源    表：
#输入参数:
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：liuzhilong
#编写时间：2010-7-26
#问题记录：西藏没此业务，因此接口暂时送空文件
#修改历史:根据二经开发的校园市场专题提取.
#######################################################################################################


proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

	#当天 yyyymmdd
  set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
  #当天 yyyy-mm-dd
  set op_month [string range $optime_month 0 3][string range $optime_month 5 6]

  #程序名
  global app_name
  set app_name "G_I_06002_MONTH.tcl"
  
##  #删除本期数据
	set sql_buff "
	delete from bass1.G_I_06002_MONTH where time_id=$op_month
	"
  exec_sql $sql_buff 
  
  set sql_buff "
  insert into G_I_06002_MONTH
  (
         TIME_ID
        ,SCHOOL_ID
        ,CELL_ID
        ,LAC_ID  
  )
  select distinct 
	$op_month
        ,SCHOOL_ID
        ,value(CELL_ID,'0') CELL_ID
        ,value(LAC_ID,'0') LAC_ID        
	from bass2.Dim_xysc_maintenance_info a
	where  a.SCHOOL_ID  in (
	select distinct SCHOOL_ID from G_I_02032_MONTH where TIME_ID = $op_month
	)
  "
  exec_sql $sql_buff
  
  #进行结果数据检查
  #1.检查chkpkunique
  set tabname "G_I_06002_MONTH"
  set pk   "SCHOOL_ID||CELL_ID||LAC_ID"
  chkpkunique ${tabname} ${pk} ${op_month}
  #
  aidb_runstats bass1.$tabname 3
    
  
	return 0
}

