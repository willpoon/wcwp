######################################################################################################
#程序名称：	INT_CHECK_R147R148_DAY.tcl
#校验接口：	01007
#运行粒度: DAY
#输入参数: 
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：PANZHIWEI
#编写时间：2011-05-26 
#问题记录：
#修改历史:  
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {
		##~   set op_time 2012-08-25
        #当天 yyyymmdd
        set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]      
				puts $timestamp
				
        #自然月
				puts $op_time 
        set curr_month [string range $op_time 0 3][string range $op_time 5 6]
				puts $curr_month
	set last_day [GetLastDay [string range $timestamp 0 7]]
#for WriteAlarm
set optime $op_time
        
        #程序名
        set app_name "INT_CHECK_R147R148_DAY.tcl"



########################################################################################################3

 	  set sqlbuf "delete from  BASS1.G_RULE_CHECK where time_id=$timestamp and rule_code in ('R147','R148') "        
	  exec_sql $sqlbuf


##~   R147	日	04_TD业务	使用TD网络客户数在T网上计费时长日变动率	22202 使用TD网络的客户通话情况日汇总	使用TD网络客户数在T网上计费时长日变动率 ≤ 35%	0.05	修改指标校验阈值！

	set sqlbuf "
	select sum(bigint(tnet_bill_duration)) from bass1.g_s_22202_day where time_id=$timestamp with ur"
	set RESULT_VAL1 [get_single $sqlbuf]

	set sqlbuf "
		select sum(bigint(tnet_bill_duration)) from bass1.g_s_22202_day where time_id=$last_day  with ur"
	set RESULT_VAL2 [get_single $sqlbuf]

    set  RESULT_VAL3 [expr 1.000 * ($RESULT_VAL1 - $RESULT_VAL2) / $RESULT_VAL2 ]
    puts  $RESULT_VAL3

	#将校验值插入校验结果表
	set sqlbuf "
	INSERT INTO bass1.G_RULE_CHECK
	values
	(
		$timestamp
		,'R147'
		,$RESULT_VAL1
		,$RESULT_VAL2
		,$RESULT_VAL3
		,0
	)
	"
	  exec_sql $sqlbuf


	if {$RESULT_VAL3>=0.3 || $RESULT_VAL3<=-0.3 } {
		set grade 2
	    set alarmcontent "R147 波动性检查使用TD网络的客户在T网上计费时长超出30%，接近35%"
	    WriteAlarm $app_name $optime $grade ${alarmcontent}
	} 

   puts "R147 end---------------------------------------"

##~   R148	日	04_TD业务	使用TD网络客户数在T网上数据流量日变动率	22203 使用TD网络的客户数据流量日汇总	使用TD网络客户数在T网上数据流量日变动率 ≤ 50%	0.05	修改指标校验阈值！
	set RESULT_VAL1 0
	set RESULT_VAL2 0
	set RESULT_VAL3 0

	set sqlbuf "
	select sum(bigint(td_tnet_data_flux)) from bass1.g_s_22203_day where time_id=$timestamp with ur"
	set RESULT_VAL1 [get_single $sqlbuf]


    #取上个统计日的指标值：使用TD网络的客户数据流量日汇总(tb_sum_td_usd_net_cust_data_d)表中，
    #入库日期为上个统计日的“使用TD网络的客户在T网上的数据流量”。
    
	set sqlbuf "
		select sum(bigint(td_tnet_data_flux)) from bass1.g_s_22203_day where time_id=$last_day with ur"
	set RESULT_VAL2 [get_single $sqlbuf]

    set  RESULT_VAL3 [expr 1.000 * ($RESULT_VAL1 - $RESULT_VAL2) / $RESULT_VAL2 ]
    puts  $RESULT_VAL3


	#将校验值插入校验结果表
	set sqlbuf "
	INSERT INTO bass1.G_RULE_CHECK
	values
	(
		$timestamp
		,'R148'
		,$RESULT_VAL1
		,$RESULT_VAL2
		,$RESULT_VAL3
		,0
	)
	"
	  exec_sql $sqlbuf


	if {$RESULT_VAL3>=0.45 || $RESULT_VAL3<=-0.45 } {
		set grade 2
	    set alarmcontent "R148 波动性检查使用TD网络的客户在T网上的数据流量超出45%，接近50%"
	    WriteAlarm $app_name $timestamp $grade ${alarmcontent}
	} 

   puts "R148 end---------------------------------------"

	return 0
}

