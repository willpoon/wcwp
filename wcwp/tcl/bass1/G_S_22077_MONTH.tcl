
######################################################################################################		
#接口名称: 计费收入KPI                                                               
#接口编码：22077                                                                                          
#接口说明：记录计费收入月KPI信息。
#程序名称: G_S_22077_MONTH.tcl                                                                            
#功能描述: 生成22077的数据
#运行粒度: MONTH
#源    表：1.
#输入参数: void
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：panzw
#编写时间：20110922
#问题记录：
#修改历史: 1. panzw 20110922	1.7.5 newly added
#######################################################################################################   
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {
#set optime_month 2011-08
      set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]      
      set op_month [string range $optime_month 0 3][string range $optime_month 5 6]
      puts $op_month
      #set this_month_last_day [string range $op_month 0 5][GetThisMonthDays [string range $op_month 0 5]01] 
      set last_month [GetLastMonth [string range $op_month 0 5]]
      #set curr_month_first_day [string range $timestamp 0 5]01
      #puts $curr_month_first_day
      set ThisMonthFirstDay [string range $op_month 0 3][string range $op_time 4 4][string range $op_month 4 5][string range $op_time 4 4]01
      puts $ThisMonthFirstDay      


  #删除本期数据
	set sql_buff "delete from bass1.G_S_22077_MONTH where time_id=$op_month"
	exec_sql $sql_buff
	
		set sql_buff "
			INSERT INTO  BASS1.G_S_22077_MONTH
		select $op_month
		,'$op_month'
		,(select  char(bigint(sum(BASECALL_FEE+TOLL_FEE+INFO_FEE))) 
			from 
			bass2.DW_CALL_$op_month a
			where roamtype_id in (1,4,6,8)	
			and TOLLTYPE_ID in ( 0,1,2,101,102)
			and CALLTYPE_ID = 0
				)
	,(
			select  char(bigint(sum(BASECALL_FEE+TOLL_FEE+INFO_FEE))) 
		from 
		bass2.DW_CALL_$op_month a
		where roamtype_id in (1,4,6,8)	
		and CALLTYPE_ID = 1
			)
	,'0'
	,'0'
	,
	(
	select char(bigint(sum(BASE_FEE+INFO_FEE+MONTH_FEE+FUNC_FEE))) from  bass2.dw_newbusi_gprs_$op_month 
	where ROAMTYPE_ID in ( 0,1 )
	)
	,(
		select  char(bigint(sum(BASE_FEE+INFO_FEE+MONTH_FEE+FUNC_FEE))) from  bass2.dw_newbusi_gprs_$op_month 
		where ROAMTYPE_ID in ( 4,8 )
		)
	,case when 
	(
	select char(bigint(1.0*sum(CHARGE1+CHARGE2+CHARGE3+CHARGE4)/100)) from  bass2.dw_gprs_l_dm_$op_month 
	where ROAM_TYPE in ( 5,9 )
	) is null then '0' else 
(
	select char(bigint(1.0*sum(CHARGE1+CHARGE2+CHARGE3+CHARGE4)/100)) from  bass2.dw_gprs_l_dm_$op_month 
	where ROAM_TYPE in ( 5,9 )
	) end 
from bass2.dual
with ur
	"
	exec_sql $sql_buff


  #1.检查chkpkunique
	set tabname "G_S_22077_MONTH"
	set pk 			"OP_MONTH"
	chkpkunique ${tabname} ${pk} ${op_month}

  aidb_runstats bass1.G_S_22077_MONTH 3

	
	return 0
}

#	
#	
#	属性编码	属性名称	属性描述	属性类型	备注
#	00	记录行号	唯一标识记录在接口数据文件中的行号。	number(8)	
#	01	月份	格式：YYYYMM	char(6)	主键
#	02	国内漫游出访国内主叫计费收入	指统计周期内省内漫游出访和省际漫游出访状态下国内（含网内网间）主叫产生的计费收入。	number(15)	单位：元
#	03	国内漫游出访被叫计费收入	指统计周期内省内漫游出访和省际漫游出访状态下所有（含网内网间国际）被叫产生的计费收入。	number(15)	单位：元
#	04	IDC-主机托管计费收入	统计周期内IDC-主机托管业务产生的计费收入,IDC-主机托管是指IDC为客户提供一定的“空间”和“带宽”，其中“空间”是参照机架服务器的规格选取固定的1U/2U/4U机位空间，客户将自己的网络设备、服务器托管在租用的空间内。客户拥有对托管设备的所有权和完全控制权限，客户自行安装软件系统和自行维护。	number(15)	单位：元
#	05	IDC-虚拟主机计费收入	统计周期内IDC-虚拟主机业务产生的计费收入,IDC-虚拟主机为客户发布WEB网站提供所需的主机资源及互联网连接。虚拟主机是把一台运行在互联网上的服务器资源（系统资源、网络带宽、存储空间等）按照一定的比例划分成若干台“虚拟”的“小主机”，每一个虚拟主机都具有独立的域名，同一台服务器上的不同虚拟主机是彼此独立的，并可由客户自行管理。	number(15)	单位：元
#	06	GPRS上网-省内计费收入	统计周期内GPRS上网-省内业务产生的计费收入。	number(15)	单位：元
#	07	GPRS上网-省际漫游出访计费收入	统计周期内GPRS上网-省际漫游出访业务产生的计费收入。	number(15)	单位：元
#	08	GPRS上网-国际漫游出访计费收入	统计周期内GPRS上网-国际漫游出访业务产生的计费收入。	number(15)	单位：元
#	
#	
#02	国内漫游出访国内主叫计费收入
##	
##	select (BASE_FEE+TOLL_FEE+INFO_FEE)
##	from 
##	bass2.DW_CALL_$op_month a
##	where roamtype_id in (1,4,6,8)	
##	and TOLLTYPE_ID in ( 0,1,2,101,102)
##	and CALLTYPE_ID = 0
##	
##	
##	
##	03	国内漫游出访被叫计费收入
##	
##	select char((BASE_FEE+TOLL_FEE+INFO_FEE)
##	from 
##	bass2.DW_CALL_$op_month a
##	where roamtype_id in (1,4,6,8)	
##	and CALLTYPE_ID = 1
##	
##	
##	#	04	IDC-主机托管计费收入	统计周期内IDC-主机托管业务产生的计费收入,IDC-主机托管是指IDC为客户提供一定的“空间”和“带宽”，其中“空间”是参照机架服务器的规格选取固定的1U/2U/4U机位空间，客户将自己的网络设备、服务器托管在租用的空间内。客户拥有对托管设备的所有权和完全控制权限，客户自行安装软件系统和自行维护。	number(15)	单位：元
##	0
##	#	05	IDC-虚拟主机计费收入	统计周期内IDC-虚拟主机业务产生的计费收入,IDC-虚拟主机为客户发布WEB网站提供所需的主机资源及互联网连接。虚拟主机是把一台运行在互联网上的服务器资源（系统资源、网络带宽、存储空间等）按照一定的比例划分成若干台“虚拟”的“小主机”，每一个虚拟主机都具有独立的域名，同一台服务器上的不同虚拟主机是彼此独立的，并可由客户自行管理。	number(15)	单位：元
##	0
##	
##	
##	
##	#	06	GPRS上网-省内计费收入	统计周期内GPRS上网-省内业务产生的计费收入。	number(15)	单位：元
##	
##	select char(sum(BASE_FEE+INFO_FEE+MONTH_FEE+FUNC_FEE)) from  bass2.dw_newbusi_gprs_$op_month 
##	where ROAMTYPE_ID in ( 0,1 )
##	#	07	GPRS上网-省际漫游出访计费收入	统计周期内GPRS上网-省际漫游出访业务产生的计费收入。	number(15)	单位：元
##	
##	select char(sum(BASE_FEE+INFO_FEE+MONTH_FEE+FUNC_FEE)) from  bass2.dw_newbusi_gprs_$op_month 
##	where ROAMTYPE_ID in ( 4,8 )
##	
##	#	08	GPRS上网-国际漫游出访计费收入	统计周期内GPRS上网-国际漫游出访业务产生的计费收入。	number(15)	单位：元
##	
##	
##	select char(sum(BASE_FEE+INFO_FEE+MONTH_FEE+FUNC_FEE)) from  bass2.dw_newbusi_gprs_$op_month 
##	where ROAMTYPE_ID in ( 100 )
##	
##	
##	
##	select cast( sum(value(BASE_FEE+INFO_FEE+MONTH_FEE+FUNC_FEE,0)) as decimal(10,2)) from  bass2.dw_newbusi_gprs_$op_month 
##	where ROAMTYPE_ID in ( 100 )
##	
##	select  cast( sum(BASE_FEE+INFO_FEE+MONTH_FEE+FUNC_FEE) as decimal(10,2)) from  bass2.dw_newbusi_gprs_$op_month 
##	where ROAMTYPE_ID in ( 4,8 )
##	
##	
##	select cast( sum(BASE_FEE+INFO_FEE+MONTH_FEE+FUNC_FEE) as decimal(10,2)) from  bass2.dw_newbusi_gprs_$op_month 
##	where ROAMTYPE_ID in ( 0,1 )
##	
##	
##	
##	
##	0	本地
##	1	省内非IP长途
##	2	省际非IP长途
##	101	17951省内IP长途
##	102	17951省际IP长途
##	
##	