#***********************************************************************************************************************************************
# ** 程序名称：INT_CHECK_R053_MONTH.tcl
# ** 程序功能：一经校验程序（R053）
# ** 运行示例：int INT_CHECK_R053_MONTH.tcl
# ** 创建时间：2009-5-31 10:08:05
# ** 创 建 人：xufr
# ** 规则编号：R053
# ** 规则属性：业务逻辑
# ** 规则类型：月
# ** 指标摘要：实体渠道数量
# ** 规则描述：实体渠道接口中渠道数量应该等于自营渠道情况表中和社会渠道情况表中渠道数量之和
# ** 校验对象：22044 自营渠道情况
# **           22045 社会渠道情况
# **           06030 实体渠道
# ** 输出参数：返回值:   0 成功; -1 失败
# ** 修改历史:
# **           修改日期       修改人        修改内容
# **           -------------------------------------------------------------------------------------------------------
# **
# **		2009-07-02	zhaolp2		06030接口增加channel_status='1'
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
	set app_name "INT_CHECK_R053_MONTH.tcl"

	#删除本期数据
	set handle [ aidb_open $conn ]
	set sqlbuf "delete from bass1.g_rule_check where time_id = $op_month and rule_code = 'R053'"
	puts ${sqlbuf}
	if [ catch { aidb_sql $handle $sqlbuf } errmsg ] {
		WriteTrace "$errmsg" 10000
		return -1
	}
	aidb_commit $conn
	aidb_close $handle

	#实体渠道接口中渠道数量
	set handle [ aidb_open $conn ]
	set sqlbuf "
		select count(*) from (
		select channel_id,channel_status,row_number() over(partition by channel_id order by time_id desc ) row_id 
		from  bass1.G_A_06030_DAY
		where time_id<=$op_month_last
		) a 
		where row_id=1 and  channel_status='1' "
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
	
	#自营渠道情况表渠道数量之和
	set handle [ aidb_open $conn ]
	set sqlbuf "select sum(int(COMPANYCOUNT)+int(ENTERPRISECOUNT)+int(SOCIETYCOUNT)) from bass1.G_S_22044_MONTH where time_id = $op_month"
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
	
	#社会渠道情况表渠道数量之和
	set handle [ aidb_open $conn ]
	set sqlbuf "select sum(int(COMPANYCOUNT)+int(ENTERPRISECOUNT)+int(SOCIETYCOUNT)+int(COMPANYCOUNT2)+int(ENTERPRISECOUNT2)+int(SOCIETYCOUNT2)+int(COMPANYCOUNT3)+int(ENTERPRISECOUNT3)+int(SOCIETYCOUNT3)+int(HAVEBOSSCOUNT)+int(HAVENOTBOSSCOUNT)+int(HAVEBOSSCOUNT2)+int(HAVENOTBOSSCOUNT2)+int(HAVEBOSSCOUNT3)+int(HAVENOTBOSSCOUNT3)+int(EXCLUSIVECOUNT)+int(UNEXCLUSIVECOUNT)+int(EXCLUSIVECOUNT2)+int(UNEXCLUSIVECOUNT2)+int(EXCLUSIVECOUNT3)+int(UNEXCLUSIVECOUNT3)) from bass1.G_S_22045_MONTH where time_id = $op_month"
    puts ${sqlbuf}
	if [ catch { aidb_sql $handle $sqlbuf } errmsg ] {
		WriteTrace $errmsg 10030
		return -1
	}
	while { [ set p_row [ aidb_fetch $handle ] ] != "" } {
		set RESULT_VAL3 [ lindex $p_row 0 ]
	}
	aidb_commit $conn
	aidb_close $handle

	#将校验值插入校验结果表
	set handle [ aidb_open $conn ]
	set sqlbuf "
	INSERT INTO bass1.G_RULE_CHECK VALUES
	(
		$op_month
		,'R053'
		,$RESULT_VAL1
		,$RESULT_VAL2
		,$RESULT_VAL3
		,0
	)"
	puts ${sqlbuf}
	if [ catch { aidb_sql $handle $sqlbuf } errmsg ] {
		WriteTrace $errmsg 10030
		return -1
	}
	aidb_commit $conn
	aidb_close $handle

	#判断R053：实体渠道月汇总接口中业务办理笔数 = 自营渠道业务量情况接口品牌业务办理量之和 + 社会渠道业务情况接口品牌业务办理量之和
	if { [ expr $RESULT_VAL2 + $RESULT_VAL3 ] != $RESULT_VAL1 } {
		set grade 2
		set alarmcontent "准确性指标R053超出集团考核范围"
		WriteAlarm $app_name $opmonth $grade ${alarmcontent}
	}
	return 0
}
