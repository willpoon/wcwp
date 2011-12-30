######################################################################################################
#接口名称：集团客户与目标市场清单客户对应关系
#接口编码：01007
#接口说明： 
#程序名称: G_A_01007_DAY.tcl
#功能描述: 生成01007的数据
#运行粒度: 日
#源    表 
#输入参数: 
#输出参数: 返回值:0 成功;-1 失败  
#编 写 人：liuzhilong
#编写时间：2010-11-9 16:05:01
#问题记录：
#修改历史: 2011.11.30 boss数据源有所完善，增补上报此接口数据
#接口单元名称：集团客户与目标市场清单客户对应关系
#接口单元编码：01007
#接口单元说明：1、本接口上报的是目标市场清单集团客户ID与支撑系统内集团客户标识之间的对应关系信息，即01006接口与01004接口集团间的对应关系。
#              2、本接口以日增量形式进行上传，首次上报全量对应关系。
#              3、省公司上报的清单集团ID与集团客户标识对应关系，只能上报一对一、一对多或者多对一对应关系，不能上报多对多的对应关系。
#
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

    #本月 yyyymm
    set op_month [string range $optime_month 0 3][string range $optime_month 5 6]   
    set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]

    set sql_buff "DELETE FROM bass1.G_A_01007_DAY where time_id=$timestamp"
    exec_sql $sql_buff

#01006	目标市场清单客户	每日增量	每日11点前	2
#01007	集团客户与目标市场清单客户对应关系	每日增量	每日11点前	2
# 插入新建立数据
set sql_buff "
	insert into G_A_01007_DAY
	select  distinct 
	$timestamp TIME_ID
	, ENTERPRISE_ID
	, CUST_ID
	,'1' RELA_STATE
	from (
		select 
		qd_group_id ENTERPRISE_ID
		,zw_group_id CUST_ID
		from bass2.dw_ent_group_relation_$timestamp  a
		except
		select ENTERPRISE_ID,CUST_ID
		from (
		select ENTERPRISE_ID,CUST_ID ,row_number()over(partition by ENTERPRISE_ID,CUST_ID order by time_id desc ) rn 
		from G_A_01007_DAY
		)  t where rn = 1
	    ) o
	with ur
"

#exec_sql $sql_buff

# 插入解除关系数据
# bass2.dw_ent_group_relation_$timestamp 中没有的视为已解除

set sql_buff "
	insert into G_A_01007_DAY
	select  distinct 
	$timestamp TIME_ID
	, ENTERPRISE_ID
	, CUST_ID
	,'2' RELA_STATE
	from (

		select ENTERPRISE_ID,CUST_ID
		from (
		select ENTERPRISE_ID,CUST_ID ,RELA_STATE,row_number()over(partition by ENTERPRISE_ID,CUST_ID order by time_id desc ) rn 
		from G_A_01007_DAY
		)  t where rn = 1 and RELA_STATE = '1'
	except
	select 
		qd_group_id ENTERPRISE_ID
		,zw_group_id CUST_ID
		from bass2.dw_ent_group_relation_$timestamp  a
	    ) o
	with ur
"

#exec_sql $sql_buff



  #进行结果数据检查
  #1.检查chkpkunique
  set tabname "G_A_01007_DAY"
  set pk   "ENTERPRISE_ID||CUST_ID"
        chkpkunique ${tabname} ${pk} ${timestamp}
        #
  aidb_runstats bass1.$tabname 3
  
           
##	set sql_buff "DELETE FROM bass1.G_A_01007_DAY where time_id=$op_month"
##	puts $sql_buff
##    exec_sql $sql_buff
##
##
##
##	set sql_buff "insert into bass1.G_A_01007_DAY
##
##	              "
##
##  puts $sql_buff
##  exec_sql $sql_buff
##
#2011-05-31 19:45:00 一次性 for R194

#set sql_buff "
#insert into G_A_01007_DAY select * from   G_A_01007_DAY_T1
#"
#exec_sql $sql_buff
#

	return 0
}
