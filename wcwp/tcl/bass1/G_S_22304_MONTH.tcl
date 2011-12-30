######################################################################################################
#接口名称：动力100业务收入情况
#接口编码：22304
#接口说明：此接口上报“动力100业务包”中各子业务的收入情况，包括统付、非统付收入。
#程序名称: G_S_22304_MONTH.tcl
#功能描述: 生成22304的数据
#运行粒度: 月
#源    表：
#输入参数: 
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：liuqf
#编写时间：2009-07-19
#问题记录：
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

#        set op_time 2008-06-01
#        set optime_month 2008-05-31
        set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]       
        puts $timestamp
        #----求上月最后一天---#,格式 yyyymmdd
        set last_month_day [GetLastDay [string range $timestamp 0 5]01]
        puts $last_month_day

        set ThisMonthFirstDay [string range $optime_month 0 6][string range $optime_month 4 4]01
        puts $ThisMonthFirstDay

        #本月 yyyymm
        set op_month [string range $optime_month 0 3][string range $optime_month 5 6]          
        set objecttable "G_S_22304_MONTH"
        #set db_user $env(DB_USER)
       
        #删除本期数据
        set handle [aidb_open $conn]
	set sql_buff "delete from bass1.$objecttable where time_id=$op_month"
	puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	
	
	#集团动力100业务收入
	set handle [aidb_open $conn]
	set sql_buff "insert into bass1.$objecttable
									select distinct
									$op_month,
									A.ENTERPRISE_ID,
									C.BASS1_VALUE as ENTERPRISE_BUSI_TYPE,
									D.BASS1_VALUE as SUB_BUSI_TYPE,
									case
									    when upper(B.CONFIG_VALUE)='MAS' then '1'
									    when upper(B.CONFIG_VALUE)='ADC' then '2'
									    else '3'
									end as MANAGE_MODE,
									char(bigint(sum(a.unipay_fee)*100))     as income,
									char(bigint(sum(a.non_unipay_fee)*100)) as no_income
								from  bass2.dw_enterprise_unipay_$op_month a
								inner join (select * from bass2.DIM_SERVICE_CONFIG where CONFIG_ID=1000027)  b on A.SERVICE_ID=B.SERVICE_ID
								inner join (select * from bass1.ALL_DIM_LKP_163 where bass1_tbid='BASS_STD1_0108') c on A.SERVICE_ID=C.XZBAS_VALUE
								inner join (select * from bass1.ALL_DIM_LKP_160 where bass1_tbid='BASS_STD1_0108') d on A.SERVICE_ID=D.XZBAS_VALUE
							where a.enterprise_id not in ('891910006274','891880005002')
								and a.enterprise_id in ('89103001033332')
					    group by A.ENTERPRISE_ID,
									C.BASS1_VALUE,
									D.BASS1_VALUE,
									case
									    when upper(B.CONFIG_VALUE)='MAS' then '1'
									    when upper(B.CONFIG_VALUE)='ADC' then '2'
									    else '3'
									end                   
									"              
  puts $sql_buff                                    
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	puts "动力100业务收入"	
		
	


	return 0
}	