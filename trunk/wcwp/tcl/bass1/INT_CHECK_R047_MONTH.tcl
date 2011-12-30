#***********************************************************************************************************************************************
# ** 程序名称：INT_CHECK_R047_MONTH.tcl
# ** 程序功能：一经校验程序（R047）
# ** 运行示例：int INT_CHECK_R047_MONTH.tcl
# ** 创建时间：2009-5-31 9:45:46
# ** 创 建 人：xufr
# ** 规则编号：R047
# ** 规则属性：业务逻辑
# ** 规则类型：月
# ** 指标摘要：自营渠道的分业务办理笔数和分品牌办理笔数的平衡关系
# ** 规则描述：接口规范中要求各指标之间应满足以下关系“新增客户数＋缴费笔数＋增值业务办理笔数＋定制终端销量＋其他杂项业务办理笔数＝全球通用户业务办理笔数＋神州行用户业务办理笔数＋动感地带用户业务办理笔数”
# ** 校验对象：22046 自营渠道业务量情况
# ** 输出参数：返回值:   0 成功; -1 失败
# ** 修改历史:
# **           修改日期       修改人        修改内容
# **           -------------------------------------------------------------------------------------------------------
# **           20100101 liuqf null值补0
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
	set app_name "INT_CHECK_R047_MONTH.tcl"

	#删除本期数据
	set handle [ aidb_open $conn ]
	set sqlbuf "delete from bass1.g_rule_check where time_id = $op_month and rule_code = 'R047'"
	puts ${sqlbuf}
	if [ catch { aidb_sql $handle $sqlbuf } errmsg ] {
		WriteTrace "$errmsg" 10000
		return -1
	}
	aidb_commit $conn
	aidb_close $handle

	#新增客户数＋缴费笔数＋增值业务办理笔数＋定制终端销量＋其他杂项业务办理笔数
	set handle [ aidb_open $conn ]
	set sqlbuf "
	select value(sum(int(NEWCUSTCOUNT) + int(PAYCOUNT) + int(BUSICOUNT) + int(COMINALCOUNT) + int(OTHERCOUNT)),0) from bass1.G_S_22046_MONTH where time_id = $op_month"
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
	
	#全球通用户业务办理笔数＋神州行用户业务办理笔数＋动感地带用户业务办理笔数
	set handle [ aidb_open $conn ]
	set sqlbuf "select value(sum(int(QQTOPERATORCOUNT) + int(SZXOPERATORCOUNT) + int(DGDDOPERATORCOUNT)),0) from bass1.G_S_22046_MONTH where time_id = $op_month"
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
		,'R047'
		,$RESULT_VAL1
		,$RESULT_VAL2
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

	#判断R047：两指标应相等
	if { $RESULT_VAL1 != $RESULT_VAL2 } {
		set grade 2
		set alarmcontent "准确性指标R047超出集团考核范围"
		WriteAlarm $app_name $opmonth $grade ${alarmcontent}
	}
	return 0
}
