#***********************************************************************************************************************************************
# ** 程序名称：INT_CHECK_R023_MONTH.tcl
# ** 程序功能：一经校验程序（R023）
# ** 运行示例：int INT_CHECK_R023_MONTH.tcl
# ** 创建时间：2009-5-31 15:33:20
# ** 创 建 人：xufr
# ** 规则编号：R023
# ** 规则属性：业务逻辑
# ** 规则类型：月
# ** 指标摘要：新增用户数月/日汇总关系
# ** 规则描述：|(月新增用户/∑(日新增用户数）-1) x 100% | ≤ 3%
# ** 校验对象：02004 用户
# ** 输出参数：返回值:   0 成功; -1 失败
# ** 修改历史:
# **           修改日期       修改人        修改内容
# **           -------------------------------------------------------------------------------------------------------
# **
# **  Copyright(c) 2004 AsiaInfo Technologies (China), Inc.
# **  All Rights Reserved.
#***********************************************************************************************************************************************
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

	#本月 yyyymm
	set op_month [ string range $optime_month 0 3 ][ string range $optime_month 5 6 ]
	#本月 yyyymm01
	set op_month_01 [ string range $optime_month 0 3 ][ string range $optime_month 5 6 ]01
	#本月 yyyy-mm
	set opmonth $optime_month
	#获取数据月份1号yyyy-mm-01
	set op_month_01_iso [ string range $op_month 0 3 ]-[ string range $op_month 4 5 ]-01
	#获取数据月份下月1号yyyy-mm-01
	set nt_month_01_iso [ clock format [ clock scan "${op_month_01_iso} + 1 months" ] -format "%Y-%m-01" ]
	#获取数据月份本月末日yyyy-mm-31
	set op_month_last_iso [ clock format [ clock scan "${nt_month_01_iso} - 1 days" ] -format "%Y-%m-%d" ]
	#获取数据月份本月末日yyyymm31
	set op_month_last [ string range $op_month_last_iso 0 3 ][ string range $op_month_last_iso 5 6 ][ string range $op_month_last_iso 8 9 ]
	#程序名称
	set app_name "INT_CHECK_R023_MONTH.tcl"

	#删除本期数据
	set handle [ aidb_open $conn ]
	set sqlbuf "delete from bass1.g_rule_check where time_id = $op_month and rule_code = 'R023'"
	puts ${sqlbuf}
	if [ catch { aidb_sql $handle $sqlbuf } errmsg ] {
		WriteTrace "$errmsg" 10000
		return -1
	}
	aidb_commit $conn
	aidb_close $handle

	#日新增用户数
	set handle [ aidb_open $conn ]
	set sqlbuf "
	select count(*) 
	from bass1.G_A_02004_DAY
	where create_date >= '$op_month_01' 
	and create_date <= '$op_month_last' 
	and time_id / 100 = $op_month"
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
	
	#月新增用户
	set handle [ aidb_open $conn ]
	set sqlbuf "
	select count(*) 
	from bass1.G_A_02004_DAY
	where substr(create_date,1,6) = '$op_month' 
	and time_id / 100 = $op_month"
    puts ${sqlbuf}
	if [ catch { aidb_sql $handle $sqlbuf } errmsg ] {
		WriteTrace $errmsg 10020
		return -1
	}
	while { [ set p_row [ aidb_fetch $handle ] ] != "" } {
		set RESULT_VAL2 [ lindex $p_row 0 ]
	}
	aidb_commit $conn
	aidb_close $handle

	#将校验值插入校验结果表
	set handle [ aidb_open $conn ]
	set sqlbuf "
	INSERT INTO bass1.G_RULE_CHECK VALUES
	(
		$op_month
		,'R023'
		,$RESULT_VAL1
		,$RESULT_VAL2
		,1.000 * $RESULT_VAL2 / $RESULT_VAL1 - 1
		,0
	)"
	puts ${sqlbuf}
	if [ catch { aidb_sql $handle $sqlbuf } errmsg ] {
		WriteTrace $errmsg 10030
		return -1
	}
	aidb_commit $conn
	aidb_close $handle

	#判断R023：判断是否小于等于3%
	if { [ expr 1.000 * $RESULT_VAL2 / $RESULT_VAL1 - 1 ] > 0.3 } {
		set grade 2
		set alarmcontent "R023校验不通过,日/月新增用户数比例超出集团考核范围"
		WriteAlarm $app_name $opmonth $grade ${alarmcontent}
	}
	return 0
}
