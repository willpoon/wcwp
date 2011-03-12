#/*******************************************************************************
#	源程序名：template.tcl
#	数据流向：cdr_call_yyyymmdd -> dw_call_yyyymmdd
#	创建日期：2007-03-12
#	编写人：  zhoucg
#	简要说明：生成语音业务当日汇总表
#	修改日志：
#*******************************************************************************/

proc deal {p_optime p_timestamp} {

	global env

	global conn

	global handle

	set handle [aidb_open $conn]

	if [catch {set handle [aidb_open $conn]} errmsg] {
	  	trace_sql $errmsg  1000
	  	return -1
	}

	if { [exec_sql $p_optime $p_timestamp] != 0 } {
		aidb_close $handle
		aidb_roll $conn
		return -1
	}

	return 0
}

proc exec_sql {p_optime p_timestamp} {

	global env

	global conn

	global handle

##	求天数的加减
#	aidb_close $handle
#	set handle [aidb_open $conn]
#	puts [ai_adddays $handle ${p_optime} 1]
#
##	求月数的加减
#	aidb_close $handle
#	set handle [aidb_open $conn]
#	puts [ai_addmonths $handle ${p_optime} -1]

#	set sqlbuf "create table bass2.dddddddddddddddddddddddd (a char(1)) in tbs_pso"
#
#  	if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
#		trace_sql "$errmsg" 1001
#	}
#
#	aidb_commit $conn

#	M21、做runstats：
#   exec db2 connect to bassdb user bass2 using bass2
#	exec db2 runstats on table bass2.dw_product_20070701 with distribution and detailed indexes all
#	
#   if [catch {set handle [aidb_open $conn]} errmsg] {
#      trace_sql $errmsg 1302
#      return -1
#   }
#   
#   aidb_commit $conn 

	puts  [GetLastDay 20100301]
	puts  [GetLastDay 20080301]
	puts  [GetLastDay 20120301]
	puts  [GetNextMonth 201002]
	puts  [GetNextMonth 200802]
	puts  [GetNextMonth 201202]
	puts  [GetLastMonth 201002]
	puts  [GetLastMonth 200802]
	puts  [GetLastMonth 201202]
	puts [GetThisMonthDays 20100201]
	puts [GetThisMonthDays 20080201]
	puts [GetThisMonthDays 20120201]
	aidb_close $handle

	return 0
}
