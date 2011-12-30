
######################################################################################################		
#接口名称: 校园竞争对手学生用户                                                               
#接口编码：02033                                                                                          
#接口说明："根据集团公司推荐的竞争对手学生用户识别模型或本省特有的竞争对手学生用户识别方案，识别出的竞争对手学生用户明细，要求真实反映本省现行实际情况。"
#程序名称: G_I_02033_MONTH.tcl                                                                            
#功能描述: 生成02033的数据
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
  set app_name "G_I_02033_MONTH.tcl"
        
  #删除本期数据
	set sql_buff "delete from bass1.G_I_02033_MONTH where time_id=$op_month"
  exec_sql $sql_buff


  set sql_buff "
  insert into G_I_02033_MONTH
  (
         TIME_ID
        ,USER_ID
        ,SCHOOL_ID
        ,CMCC_BRANCH_ID
        ,MARK_TYPE
  )
select 
         $op_month TIME_ID
	,a.COMP_NO USER_ID
        ,b.SCHOOL_ID 
        ,case when d.COMP_BRAND_ID in (1,2,9,10,11) then '020000' else '030000' end COMP_BRAND_ID
        ,'1' MARK_TYPE
from bass2.dw_xysc_school_comp_user_dt_$op_month a
left join bass2.Dim_xysc_maintenance_info b on a.SCHOOL_NAME = b.SCHOOL_NAME
join bass2.dw_comp_cust_$op_month d on a.comp_no = d.COMP_PRODUCT_NO
where  d.COMP_BRAND_ID  in (1,2,9,10,11,3,4,5,7)
and a.PHONE_TYPE = 'S'
group by 
a.COMP_NO
,b.SCHOOL_ID 
,case when d.COMP_BRAND_ID in (1,2,9,10,11) then '020000' else '030000' end 
  "
  exec_sql $sql_buff
  
  #进行结果数据检查
  #1.检查chkpkunique
  set tabname "G_I_02033_MONTH"
  set pk   "USER_ID"
  chkpkunique ${tabname} ${pk} ${op_month}
  #
  aidb_runstats bass1.$tabname 3
    
    
    
	return 0
}
