
######################################################################################################		
#接口名称: 重点校园本网学生用户                                                               
#接口编码：02034                                                                                          
#接口说明："重点校园本网学生用户是指按照《中国移动省级经营分析系统校园市场分析应用业务技术方案（v1.5.0）》在校园区域本网用户的基础之上，根据学生用户属性辅助条件筛选模型和基于社交网络逐步扩散法学生用户识别模型识别出来的学生用户群体。"
#程序名称: G_I_02034_MONTH.tcl                                                                            
#功能描述: 生成02034的数据
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
  set app_name "G_I_02034_MONTH.tcl"
        
  #删除本期数据
	set sql_buff "delete from bass1.G_I_02034_MONTH where time_id=$op_month"
  exec_sql $sql_buff


  set sql_buff "
  insert into G_I_02034_MONTH
  (
         TIME_ID
        ,USER_ID
        ,SCHOOL_ID
        ,MARK_TYPE
        ,IFSEED
  )
select distinct 
	 $op_month
        ,a.USER_ID
        ,b.school_id
        ,'1' MARK_TYPE
	,'0' IFSEED
from    bass2.DW_XYSC_SCHOOL_REAL_USER_DT_$op_month a
left join (select distinct  school_id,SCHOOL_NAME 
		from  bass2.Dim_xysc_maintenance_info ) b on ( case when a.SCHOOL_NAME like '%西藏大学%' then '西藏大学' else a.SCHOOL_NAME end ) = b.SCHOOL_NAME
where 	b.school_id = '89189100000003'
and a.school_name not in (select distinct school_name from bass2.Dim_xysc_maintenance_info where SCHOOL_NAME <> '西藏大学')
with ur
  "
  exec_sql $sql_buff
  
  #进行结果数据检查
  #1.检查chkpkunique
  set tabname "G_I_02034_MONTH"
  set pk   "USER_ID"
  chkpkunique ${tabname} ${pk} ${op_month}
  #
  aidb_runstats bass1.$tabname 3
    
    
    
	return 0
}