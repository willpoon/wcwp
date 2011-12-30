#***********************************************************************************************************************************************
# ** 程序名称：INT_CHECK_02010_MONTH.tcl
# ** 程序功能：一经校验程序（R013）
# ** 运行示例：int -s INT_CHECK_02010_MONTH.tcl
# ** 创建时间：2009-7-2
# ** 创 建 人：liuzhilong
# ** 规则编号： R012 R014 R013
# ** 规则属性：业务逻辑
# ** 规则类型：月
# ** 指标摘要：实体渠道数量
# ** 规则描述：R013 用户资料表中的在网用户都应该在用户选择资费营销案表中存在
# ** 					 R012 用户选择资费营销案表中的用户标识应存在用户表中
# ** 					 R014 用户选择资费营销案表中的资费营销案标识都应该在资费营销案表中存在
# ** 校验对象：02010 用户选择资费营销案
# **					 02001 资费营销案
# ** 输出参数：返回值:   0 成功; -1 失败
# ** 修改历史: #  20100120 修改在网用户口径usertype_id NOT IN ('2010','2020','2030','9000') 不排除数据卡 sim_code<>'1'
# **           修改日期       修改人        修改内容
# **           -------------------------------------------------------------------------------------------------------
# **
# **	 
# **  Copyright(c) 2004 AsiaInfo Technologies (China), Inc.
# **  All Rights Reserved.
#***********************************************************************************************************************************************
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

	#本月 yyyymm
	set op_month [ string range $optime_month 0 3 ][ string range $optime_month 5 6 ]
	#本月 yyyy-mm
	set opmonth $optime_month
	#获取数据月份1号yyyy-mm-01
	set op_month_01 [ string range $op_month 0 3 ]-[ string range $op_month 4 5 ]-01
	#获取数据月份下月1号yyyy-mm-01
	set nt_month_01 [ clock format [ clock scan "${op_month_01} + 1 months" ] -format "%Y-%m-01" ]
	#获取数据月份本月末日yyyy-mm-31
	set op_month_last_iso [ clock format [ clock scan "${nt_month_01} - 1 days" ] -format "%Y-%m-%d" ]
	#获取数据月份本月末日yyyymm31
	set op_month_last [ string range $op_month_last_iso 0 3 ][ string range $op_month_last_iso 5 6 ][ string range $op_month_last_iso 8 9 ]
	#程序名称
	set app_name "INT_CHECK_02010_MONTH.tcl"

	#删除本期数据
	set handle [ aidb_open $conn ]
	set sqlbuf "delete from bass1.g_rule_check where time_id = $op_month and rule_code in ('R012','R014', 'R013' )"
	puts ${sqlbuf}
	if [ catch { aidb_sql $handle $sqlbuf } errmsg ] {
		WriteTrace "$errmsg" 10000
		return -1
	}
	aidb_commit $conn
	aidb_close $handle

	# R013用户资料表中的在网用户都应该在用户选择资费营销案表中存在
	set handle [ aidb_open $conn ]
	set sqlbuf "
						select  count(*)
						from (select user_id 
												,usertype_id
												,row_number() over(partition by user_id order by time_id desc ) row_id
									from bass1.g_a_02008_day
									where time_id <=$op_month_last ) t
					 inner join (select user_id 
															,sim_code
															,usertype_id
														,row_number() over(partition by user_id order by time_id desc ) row_id
												from bass1.g_a_02004_day
												where time_id <=$op_month_last ) f on t.user_id=f.user_id
						where t.row_id=1 and f.row_id=1 
								and t.usertype_id NOT IN ('2010','2020','2030','9000')
								and f.usertype_id<>'3'
						    and t.user_id not in ( select distinct user_id from bass1.G_I_02010_MONTH where time_id=$op_month  ) "
    puts ${sqlbuf}
	if [ catch { aidb_sql $handle $sqlbuf } errmsg ] {
		WriteTrace $errmsg 10010
		return -1
	}
	while { [ set p_row [ aidb_fetch $handle ] ] != "" } {
		set RESULT_VAL1 [ lindex $p_row 0 ]
	}
	aidb_commit $conn
	aidb_close $handle
	

	#将校验值插入校验结果表
	set handle [ aidb_open $conn ]
	set sqlbuf "
	INSERT INTO bass1.G_RULE_CHECK VALUES
	(
		$op_month
		,'R013'
		,$RESULT_VAL1
		,0
		,0
		,0
	)"
	puts ${sqlbuf}
	if [ catch { aidb_sql $handle $sqlbuf } errmsg ] {
		WriteTrace $errmsg 10030
		return -1
	}
	aidb_commit $conn
	aidb_close $handle

	#判断R013 用户资料表中的在网用户有不存在选择资费营销案表中
	if { $RESULT_VAL1 >0 } {
		set grade 2
		set alarmcontent "R013校验不通过,02004用户资料表中的在网用户应该存在02010的资费营销案表中  "
		WriteAlarm $app_name $opmonth $grade ${alarmcontent}
	}
	

	# R012 用户选择资费营销案表中的用户标识应存在用户表中
	set handle [ aidb_open $conn ]
	set sqlbuf "
						select  count(*)
						from (select distinct user_id from bass1.G_I_02010_MONTH where time_id=$op_month) t
						where  user_id not in ( select distinct user_id
					                            from bass1.g_a_02004_day
												              where time_id <=$op_month_last  ) "
    puts ${sqlbuf}
	if [ catch { aidb_sql $handle $sqlbuf } errmsg ] {
		WriteTrace $errmsg 10010
		return -1
	}
	while { [ set p_row [ aidb_fetch $handle ] ] != "" } {
		set RESULT_VAL1 [ lindex $p_row 0 ]
	}
	aidb_commit $conn
	aidb_close $handle
	

	#将校验值插入校验结果表
	set handle [ aidb_open $conn ]
	set sqlbuf "
	INSERT INTO bass1.G_RULE_CHECK VALUES
	(
		$op_month
		,'R012'
		,$RESULT_VAL1
		,0
		,0
		,0
	)"
	puts ${sqlbuf}
	if [ catch { aidb_sql $handle $sqlbuf } errmsg ] {
		WriteTrace $errmsg 10030
		return -1
	}
	aidb_commit $conn
	aidb_close $handle

	#判断R012：不存在的个数大于0 告警
	if { $RESULT_VAL1 >0 } {
		set grade 2
		set alarmcontent "R012校验不通过,02010用户选择资费营销案表中的用户标识应存在用户表02004中"
		WriteAlarm $app_name $opmonth $grade ${alarmcontent}
	}	
	
	# R014 用户选择资费营销案表中的资费营销案标识都应该在资费营销案表中存在
	set handle [ aidb_open $conn ]
	set sqlbuf "
						select  count(*)
						from (select distinct plan_id from bass1.G_I_02010_MONTH  where time_id=$op_month) t
						where  plan_id not in ( select  plan_id
					                            from bass1.G_I_02001_MONTH
												              where time_id=$op_month  ) "
    puts ${sqlbuf}
	if [ catch { aidb_sql $handle $sqlbuf } errmsg ] {
		WriteTrace $errmsg 10010
		return -1
	}
	while { [ set p_row [ aidb_fetch $handle ] ] != "" } {
		set RESULT_VAL1 [ lindex $p_row 0 ]
	}
	aidb_commit $conn
	aidb_close $handle
	

	#将校验值插入校验结果表
	set handle [ aidb_open $conn ]
	set sqlbuf "
	INSERT INTO bass1.G_RULE_CHECK VALUES
	(
		$op_month
		,'R014'
		,$RESULT_VAL1
		,0
		,0
		,0
	)"
	puts ${sqlbuf}
	if [ catch { aidb_sql $handle $sqlbuf } errmsg ] {
		WriteTrace $errmsg 10030
		return -1
	}
	aidb_commit $conn
	aidb_close $handle

	#判断R014：不存在的个数大于0 告警
	if { $RESULT_VAL1 >0 } {
		set grade 2
		set alarmcontent "R014校验不通过,用户选择资费营销案表中的资费营销案标识都应该在资费营销案表中存在 "
		WriteAlarm $app_name $opmonth $grade ${alarmcontent}
	}	
		
	return 0
}
