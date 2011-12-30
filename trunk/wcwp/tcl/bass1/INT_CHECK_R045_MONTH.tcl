#***********************************************************************************************************************************************
# ** 程序名称：INT_CHECK_R045_MONTH.tcl
# ** 程序功能：一经校验程序（R045）
# ** 运行示例：int INT_CHECK_R045_MONTH.tcl
# ** 创建时间：2009-5-29 9:24:17
# ** 创 建 人：xufr
# ** 规则编号：R045
# ** 规则属性：业务逻辑
# ** 规则类型：月
# ** 指标摘要：实体渠道月汇总接口中单个渠道的新增用户数占比
# ** 规则描述：实体渠道月汇总接口中不应存在单个渠道的新增用户数占当月所有渠道新增用户数汇总值的30%
# ** 校验对象：22054 实体渠道月汇总
# ** 输出参数：返回值:   0 成功; -1 失败
# ** 修改历史:
# **           修改日期       修改人        修改内容
# **           -------------------------------------------------------------------------------------------------------
# ** 20100308 liuqf newcustcount / sumnewcustcount * 1.00  修改成newcustcount* 1.00 / sumnewcustcount  
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
	set app_name "INT_CHECK_R045_MONTH.tcl"

	#删除本期数据
	set handle [ aidb_open $conn ]
	set sqlbuf "delete from bass1.g_rule_check where time_id = $op_month and rule_code = 'R045'"
	puts ${sqlbuf}
	if [ catch { aidb_sql $handle $sqlbuf } errmsg ] {
		WriteTrace "$errmsg" 10000
		return -1
	}
	aidb_commit $conn
	aidb_close $handle

	#实体渠道月汇总接口中存在单个渠道的新增用户数占当月所有渠道新增用户数汇总值的30%以上的记录数
	set handle [ aidb_open $conn ]
	set sqlbuf "
	select 
		count(*)
	from
		(select int(newcustcount) newcustcount from bass1.g_s_22054_month where time_id = $op_month) a
		,(select sum(int(newcustcount)) sumnewcustcount from bass1.g_s_22054_month where time_id = $op_month) b
	where newcustcount * 1.00/ sumnewcustcount >= 0.3"
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
		,'R045'
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

	#判断R045：存在记录则不通过校验
	if { $RESULT_VAL1 > 0 } {
		set grade 2
		set alarmcontent "准确性指标R045超出集团考核范围"
		WriteAlarm $app_name $opmonth $grade ${alarmcontent}
	}
	return 0
}
