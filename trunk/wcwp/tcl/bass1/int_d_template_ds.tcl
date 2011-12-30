#################################################################
# 程序名称: int_d_template_ds.tcl
# 功能描述: 一经模板d程序
# 编写人：王琦
# 时间：2007-03-22
#
#################################################################

proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

	global env

	set Optime $op_time
	set Timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
	#日接口使用
	set op_month [string range $op_time 0 3][string range $op_time 5 6]
        append op_time_month ${optime_month}-01
        set db_user $env(DB_USER)
        #月接口使用
        set op_month [string range $op_time_month 0 3][string range $op_time_month 5 6]
        #求上个月 格式 yyyymm
        set last_month [GetLastMonth [string range $Timestamp 0 5]]
        #----求上月最后一天---#,格式 yyyymmdd
        set last_month_day [GetLastDay [string range $Timestamp 0 5]01]
        #puts $last_month
        
        #puts $last_day_month
         #----求本月天数-----#
        set thismonthdays [GetThisMonthDays ${op_month}01]
        #puts $thismonthdays
        #----求本年天数-----#
        set thisyeardays [GetThisYearDays ${op_month}01]
        #puts $thisyeardays
        set day [string range $op_time 8 9]
        #--------------------------------------
        
        
        
        set handle [aidb_open $conn]
	set sql_buff "\
		DELETE FROM $db_user. "
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "Error: SQL query failed! The error message is:\n	$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
       
       
       
       
       
       
        set handle [aidb_open $conn]

	set sql_buff ""
        
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "Error: SQL query failed! The error message is:\n	$errmsg" 2020
		puts $errmsg
		aidb_close $handle
		return -1
	}
        
	aidb_commit $conn

	aidb_close $handle

	return 0
}
