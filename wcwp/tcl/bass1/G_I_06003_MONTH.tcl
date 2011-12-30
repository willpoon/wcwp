
######################################################################################################		
#接口名称: 校园信息化覆盖情况                                                               
#接口编码：06003                                                                                          
#接口说明："主要用于了解全国各校园信息化业务的覆盖情况，为集团信息化的科学化管理和统筹管理收集信息"
#程序名称: G_I_06003_MONTH.tcl                                                                            
#功能描述: 生成06003的数据
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
  set op_month [string range $optime_month 0 3][string range $optime_month 5 6]

  #程序名
  global app_name
  set app_name "G_I_06003_MONTH.tcl"
  
##  #删除本期数据
	set sql_buff "
	delete from bass1.G_I_06003_MONTH where time_id=$op_month
	"
  exec_sql $sql_buff 
  
  set sql_buff "
  insert into G_I_06003_MONTH
  (
	 TIME_ID
        ,SCHOOL_ID
        ,ENTERPRISE_ID
        ,IF_CONTRACT
        ,IF_CHNL_COVER
        ,IF_LINE
        ,IF_WLAN
        ,IF_VPMN
        ,IF_MAS
        ,IF_GRP_SMMS
        ,IF_GRP_MAIL
        ,IF_CARDTONG
        ,IF_OTHCOVER
  )
  select 
  distinct 
 $op_month  TIME_ID
 ,a.SCHOOL_ID
 ,min(a.ENTERPRISE_ID ) ENTERPRISE_ID
 ,case  when a.ENTERPRISE_ID  like '8%' then '1' else '0' end IF_CONTRACT
 ,'0' IF_CHNL_COVER
 ,'0' IF_LINE
 ,'0' IF_WLAN
 ,'1' IF_VPMN
 ,'0' IF_MAS
 ,'0' IF_GRP_SMMS
 ,'0' IF_GRP_MAIL
 ,'0' IF_CARDTONG
 ,'1' IF_OTHCOVER
from bass2.Dim_xysc_maintenance_info a
where a.ENTERPRISE_ID  like '8%'
and a.SCHOOL_ID  in (
select distinct SCHOOL_ID from G_I_02032_MONTH where TIME_ID = $op_month
)
group by  
a.SCHOOL_ID
,case  when a.ENTERPRISE_ID  like '8%' then '1' else '0' end
with ur
  "
  exec_sql $sql_buff
  
  #进行结果数据检查
  #1.检查chkpkunique
  set tabname "G_I_06003_MONTH"
  set pk   "SCHOOL_ID"
  chkpkunique ${tabname} ${pk} ${op_month}
  #
  aidb_runstats bass1.$tabname 3
    
  
	return 0
}

