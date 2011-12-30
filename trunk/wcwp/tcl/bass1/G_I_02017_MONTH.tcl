######################################################################################################
#接口名称：中国移动校园区域客户
#接口编码：02017
#接口说明：
#程序名称: G_I_02017_MONTH.tcl
#功能描述:  
#运行粒度: 日
#源    表：
#输入参数:
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：liuzhilong
#编写时间：2010-7-26
#问题记录：西藏没此业务，因此接口暂时送空文件
#修改历史:
#2011-06-20 16:04:23 panzhiwei 根据二经开发的校园市场专题提取.
#xyscv1.3  已删除 该接口
#######################################################################################################


proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

	#当天 yyyymmdd
  set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
  #当天 yyyy-mm-dd
  set optime $op_time

  set op_month [string range $optime_month 0 3][string range $optime_month 5 6]

  #程序名
  global app_name
  set app_name "G_I_02017_MONTH.tcl"
        
  #删除本期数据
	set sql_buff "delete from bass1.G_I_02017_MONTH where time_id=$op_month"
  exec_sql $sql_buff


  set sql_buff "
  insert into G_I_02017_MONTH
  (
         TIME_ID
        ,USER_ID
        ,SCHOOL_ID
        ,STUD_MARK
        ,MARK_TYPE
  )
select distinct 
				$op_month
        ,a.USER_ID
        ,b.school_id
        ,case when upper(a.PHONE_TYPE) = 'S' then '1' else '2' end STUD_MARK    
        ,'1' MARK_TYPE        
from    bass2.DW_XYSC_SCHOOL_REAL_USER_DT_$op_month a
join ( select distinct  school_id,SCHOOL_NAME 
	     from  bass2.Dim_xysc_maintenance_info ) b on a.SCHOOL_NAME = b.SCHOOL_NAME
with ur  
"
  exec_sql $sql_buff
  
  #进行结果数据检查
  #1.检查chkpkunique
  set tabname "G_I_02017_MONTH"
  set pk   "USER_ID"
  chkpkunique ${tabname} ${pk} ${op_month}
  #
  aidb_runstats bass1.$tabname 3
    
           
	return 0
}