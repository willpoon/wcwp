
######################################################################################################		
#接口名称: 重点校园竞争对手学生用户                                                               
#接口编码：02035                                                                                          
#接口说明："重点校园竞争对手学生用户是根据上月本网学生用户与本地竞争对手用户语音业务和短信业务交往圈话单信息，汇总形成竞争对手在各个校园区域与本网通话次数、通话时长、短信次数和竞争对手与所有本网用户通话次数、通话时长、短信次数；将竞争对手归属到与之通话次数、通话时长、短信次数最多为校园作为竞争对手的归属校园。"
#程序名称: G_I_02035_MONTH.tcl                                                                            
#功能描述: 生成02035的数据
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
  set app_name "G_I_02035_MONTH.tcl"
        
  #删除本期数据
	set sql_buff "delete from bass1.G_I_02035_MONTH where time_id=$op_month"
  exec_sql $sql_buff


  set sql_buff "
  insert into G_I_02035_MONTH
  (
         TIME_ID
        ,USER_ID
        ,SCHOOL_ID
        ,CMCC_BRANCH_ID
  )
select 
         $op_month TIME_ID
	,a.COMP_NO USER_ID
        ,b.SCHOOL_ID 
        ,case when d.COMP_BRAND_ID in (1,2,9,10,11) then '020000' else '030000' end COMP_BRAND_ID
from bass2.dw_xysc_school_comp_user_dt_$op_month a
left join bass2.Dim_xysc_maintenance_info b  on ( case when a.SCHOOL_NAME like '%西藏大学%' then '西藏大学' else a.SCHOOL_NAME end ) = b.SCHOOL_NAME
join bass2.dw_comp_cust_$op_month d on a.comp_no = d.COMP_PRODUCT_NO
where  d.COMP_BRAND_ID  in (1,2,9,10,11,3,4,5,7)
and b.school_id = '89189100000003'
and a.school_name not in (select distinct school_name from bass2.Dim_xysc_maintenance_info 
	where SCHOOL_NAME <> '西藏大学'
	)
group by 
	a.COMP_NO
	,b.SCHOOL_ID 
	,case when d.COMP_BRAND_ID in (1,2,9,10,11) then '020000' else '030000' end 
with ur
"
  exec_sql $sql_buff
#	and SCHOOL_NAME <> '西藏大学农牧学院'

  #进行结果数据检查
  #1.检查chkpkunique
  set tabname "G_I_02035_MONTH"
  set pk   "USER_ID"
  chkpkunique ${tabname} ${pk} ${op_month}
  #
  aidb_runstats bass1.$tabname 3
    
    
    
	return 0
}
