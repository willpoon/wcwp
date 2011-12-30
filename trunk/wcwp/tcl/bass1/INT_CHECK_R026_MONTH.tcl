#***********************************************************************************************************************************************
# ** 程序名称：INT_CHECK_R026_MONTH.tcl
# ** 程序功能：一经校验程序（R026）
# ** 运行示例：int INT_CHECK_R026_MONTH.tcl
# ** 创建时间：2009-5-30 10:52:53
# ** 创 建 人：xufr
# ** 规则编号：R026
# ** 规则属性：业务逻辑
# ** 规则类型：月
# ** 指标摘要：使用终端通话用户为非历史离网用户
# ** 规则描述：用户使用终端通话情况表中的用户不应包含用户资料表中的历史离网用户
# ** 校验对象：02047 用户使用终端通话情况
# **           02004 用户
# **           02008 用户状态
# ** 输出参数：返回值:   0 成功; -1 失败
# ** 修改历史: 20100124离网口径修改为 usertype_id in ('2010','2020','2030','9000') 
# **           修改日期       修改人        修改内容
# **           -------------------------------------------------------------------------------------------------------
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
	#获取数据月份上月末日yyyymm31
	set ls_month_last [ clock format [ clock scan "${op_month_01} - 1 days" ] -format "%Y%m%d" ]
	#程序名称
	set app_name "INT_CHECK_R026_MONTH.tcl"

	#删除本期数据
	set handle [ aidb_open $conn ]
	set sqlbuf "delete from bass1.g_rule_check where time_id = $op_month and rule_code = 'R026'"
	puts ${sqlbuf}
	if [ catch { aidb_sql $handle $sqlbuf } errmsg ] {
		WriteTrace "$errmsg" 10000
		return -1
	}
	aidb_commit $conn
	aidb_close $handle

	#判断用户使用终端通话情况表中的用户包含用户资料表中的历史离网用户
	set handle [ aidb_open $conn ]
##	set sqlbuf "
##	select count(*) from bass1.G_S_02047_MONTH a
##	where time_id = $op_month
##	and int(call_cnt) > 0
##	and exists
##	(select 1 from (
##	select
##	    a.user_id
##	from
##	    bass1.G_A_02008_DAY a
##	where
##	    time_id <= ${ls_month_last} and
##	    usertype_id in ('2010','2020','2030','9000') and
##	    exists (select 1 from (select max(time_id) as time_id,user_id as user_id from bass1.G_A_02008_DAY group by user_id) b where a.time_id = b.time_id and a.user_id = b.user_id and b.time_id <= ${ls_month_last})) b
##	    where a.user_id = b.user_id)"
	set sqlbuf "
		select count(*) 
		from bass1.G_S_02047_MONTH a
		inner join (select user_id ,usertype_id ,row_number() over(partition by user_id order by time_id desc ) row_id 
			  	   	  from bass1.G_A_02008_DAY  
					      where time_id <=${ls_month_last}) b on a.user_id=b.user_id
		where a.time_id = $op_month	 
			    and  b.usertype_id in ('2010','2020','2030','9000') 
		      and  b.row_id=1
		with ur"
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
		,'R026'
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

	#判断R026：存在记录则不通过
	if { $RESULT_VAL1 > 0 } {
		set grade 2
		set alarmcontent "准确性指标R026超出集团考核范围"
		WriteAlarm $app_name $opmonth $grade ${alarmcontent}
	}
	return 0
}
