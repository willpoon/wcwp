#***********************************************************************************************************************************************
# ** 程序名称: INT_CHECK_R025_MONTH.tcl
# ** 程序功能: 一经校验程序（R025）
# ** 运行示例: int INT_CHECK_R025_MONTH.tcl
# ** 创建时间: 2009-5-29 9:24:17
# ** 创 建 人: xufr
# ** 规则编号：R025
# ** 规则属性：业务逻辑
# ** 规则类型：月
# ** 指标摘要：开通彩铃功能的在网用户数与彩铃功能的注册用户数之差
# ** 规则描述：用户开通业务功能历史表中开通彩铃功能的在网用户数与新业务注册情况表中彩铃功能的注册用户数相差比例应低于15%
# ** 校验对象：02011 用户开通业务功能历史
# **           22032 新业务注册情况
# ** 输出参数：返回值:   0 成功; -1 失败
# ** 修改历史: 20100126 在网客户口径变动 usertype_id NOT IN ('2010','2020','2030','9000')
# **           修改日期       修改人        修改内容
# **           -------------------------------------------------------------------------------------------------------
# **
# **		20090702	zhaolp2		将业务功能编码由370改为080
# **  Copyright(c) 2004 AsiaInfo Technologies (China), Inc.
# **  All Rights Reserved.
# 修改记录：   liuzhilong 20090827 修改 用户开通业务功能历史表中开通彩铃功能的在网用户数 错误的统计口径
# 修改记录：   panzw 2011-05-26 15:06:49  if {($RESULT>=0.15 || $RESULT<=-0.15) }  ==> if {($RESULT>=0.20 || $RESULT<=-0.20) } 
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
	set app_name "INT_CHECK_R025_MONTH.tcl"

	#删除本期数据
	set handle [ aidb_open $conn ]
	set sqlbuf "delete from bass1.g_rule_check where time_id = $op_month and rule_code = 'R025'"
	puts ${sqlbuf}
	if [ catch { aidb_sql $handle $sqlbuf } errmsg ] {
		WriteTrace "$errmsg" 10000
		return -1
	}
	aidb_commit $conn
	aidb_close $handle

	#用户开通业务功能历史表中开通彩铃功能的在网用户数
	set handle [ aidb_open $conn ]
##	set sqlbuf "
##	select 
##		count(distinct user_id) 
##	from 
##		bass1.g_a_02011_day b
##	where 
##		time_id <= ${op_month_last} and 
##		bigint(invalid_date) > ${op_month_last} and 
##		busi_code = '080' and 
##		exists (
##			select a.user_id 
##			from
##				(
##					select
##					    a.user_id
##					from
##					    bass1.G_A_02008_DAY a
##					where
##					    time_id <= ${op_month_last} and
##					    usertype_id not in ('2010','2020','2030','1040','1021','9000') and
##					    exists (select 1 from (select max(time_id) as time_id,user_id as user_id from bass1.G_A_02008_DAY where time_id <= ${op_month_last} group by user_id) b where a.time_id = b.time_id and a.user_id = b.user_id)
##				) a 
##			where a.user_id = b.user_id
##		)"
##    puts ${sqlbuf}

	set sqlbuf "
	select count( user_id)
	from  (select user_id,busi_code,invalid_date,valid_date,row_number() over(partition by user_id,busi_code order by time_id desc ) row_id
					from  bass1.g_a_02011_day b
					where time_id <= ${op_month_last} ) t			
	where t.row_id=1 
				and bigint(valid_date) <= ${op_month_last} 
				and bigint(invalid_date) >= ${op_month_last} 
				and busi_code = '080'  
				and	user_id  in  ( 	select a.user_id 
														from (  select a.user_id,usertype_id, row_number() over(partition by user_Id  order by time_id desc ) row_id
																		from  bass1.G_A_02008_DAY a
																		where time_id <= ${op_month_last} 			     
																	) a 				
														 where a.usertype_id not in ('2010','2020','2030','9000') and a.row_id=1
						                 )
		 "
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
	
	#新业务注册情况表中彩铃功能的注册用户数
	set handle [ aidb_open $conn ]
	set sqlbuf "select sum(int(register_user_num)) from bass1.g_s_22032_month where time_id = ${op_month} and bus_func_id = '080'"
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
	
	set RESULT [format "%.5f" [expr (${RESULT_VAL1}-${RESULT_VAL2}) / 1.0000/${RESULT_VAL2}]]

	#将校验值插入校验结果表
	set handle [ aidb_open $conn ]
	set sqlbuf "
	INSERT INTO bass1.G_RULE_CHECK VALUES
	(
		$op_month
		,'R025'
		,$RESULT_VAL1
		,$RESULT_VAL2
		,cast ($RESULT as  DECIMAL(18,5))
		,0
	)"
	puts ${sqlbuf}
	if [ catch { aidb_sql $handle $sqlbuf } errmsg ] {
		WriteTrace $errmsg 10030
		return -1
	}
	aidb_commit $conn
	aidb_close $handle

	#判断R025：相差比例应低于20%
	if {($RESULT>=0.20 || $RESULT<=-0.20) } {
		set grade 2
		set alarmcontent "R025校验不通过,彩铃功能在网/注册用户数比例超出集团考核范围"
		WriteAlarm $app_name $opmonth $grade ${alarmcontent}
	}
	return 0
}
