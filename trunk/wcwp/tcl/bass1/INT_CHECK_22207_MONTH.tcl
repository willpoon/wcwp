######################################################################################################
#程序名称：INT_CHECK_22204_MONTH.tcl
#校验接口：G_S_22204_MONTH.tcl
#运行粒度: 月
#输入参数: 
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：zhanght
#编写时间：2009-06-22
#问题记录：
#修改历史: 
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

        #本月 yyyymm
        set op_month [string range $optime_month 0 3][string range $optime_month 5 6]
            
        #上月  yyyymm
        set last_month [GetLastMonth [string range $op_month 0 5]]
        
        puts $last_month
        
        #自然月第一天 yyyymmdd
        set timestamp [string range $op_time 0 3][string range $op_time 5 6]01
        
        puts $timestamp
        
        #本月第一天 yyyymmdd
        set l_timestamp [string range $optime_month 0 3][string range $optime_month 5 6]01
        
        puts $l_timestamp
        
        #当天 yyyymmdd
        set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
        
        #当天 yyyy-mm-dd
        set optime $op_time
        
        #前一天 yyyymmdd
        set last_day [GetLastDay [string range $timestamp 0 7]]
        
        #程序名
        set app_name "INT_CHECK_0200402008_MONTH.tcl"

        #本月最后一天 yyyy-mm-dd
        set this_month_last_day [string range $op_month 0 3][string range $op_time 4 4][string range $op_month 4 5][string range $op_time 4 4][GetThisMonthDays [string range $op_month 0 5]01]
        
        puts $this_month_last_day

        #本月最后一天#,格式 yyyymmdd
        set last_month_day [GetLastDay [string range $timestamp 0 5]01]
        
        puts $last_month_day
        
        #上月最后一天 yyyymmdd
        
        set last_month_last_day [GetLastDay [string range $op_month 0 5]01]
        
        puts $last_month_last_day

	set handle [ aidb_open $conn ]
	set sqlbuf "delete from bass1.g_rule_check where time_id = $op_month 
	                    and rule_code in ('R126')"
	puts ${sqlbuf}
	if [ catch { aidb_sql $handle $sqlbuf } errmsg ] {
		WriteTrace "$errmsg" 10000
		return -1
	}
	aidb_commit $conn
	aidb_close $handle

########################################################################################################3
	#--R126:TD手机客户的总收入 ≥ TD手机客户的手机电视业务收入

  set handle [aidb_open $conn]
	set sql_buff "select bigint(TD_MB_CUSTOMER_TOTAL_FEE)-bigint(TD_MB_MOBILETV_INCOME)
                from BASS1.G_S_22207_MONTH
                where time_id=$op_month with ur"

	 if [catch { aidb_sql $handle $sql_buff  } errmsg ] {
		WriteTrace $errmsg 1001
		return -1
	}
	if [catch {set DEC_RESULT_VAL1 [lindex [aidb_fetch $handle] 0]} errmsg ] {
		WriteTrace $errmsg 1002
		return -1
	}
	aidb_commit $conn
	set DEC_RESULT_VAL1 [format "%.3f" [expr ${DEC_RESULT_VAL1} /1.00]]

	puts $DEC_RESULT_VAL1

	#--将校验值插入校验结果表
	set handle [aidb_open $conn]
	set sql_buff "\
		INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R126',$DEC_RESULT_VAL1,0,0,0) "
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace $errmsg 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn

	#--判断
	#--异常
	#--1：TD手机客户的总收入 ≥ TD手机客户的手机电视业务收入


	if {[format %.3f [expr ${DEC_RESULT_VAL1} ]]<0 } {
		set grade 2
	        set alarmcontent " R126 校验不通过,TD手机客户的总收入小于TD手机电视业务收入"
	        WriteAlarm $app_name $optime $grade ${alarmcontent}
		 }

		
	return 0
}

#内部函数部分	
proc exec_sql {MySQL} {

	global env

	global conn

	global handle

	set handle [aidb_open $conn]
	set sql_buff $MySQL
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		puts $errmsg
		exit -1
	}
	aidb_commit $conn
	aidb_close $handle
	return 0
}

proc get_single {MySQL} {

	global env

	global conn

	global handle

	set handle [aidb_open $conn]
	set sql_buff $MySQL
  if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace $errmsg 1001
		puts $errmsg
		exit -1
	}
	if [catch {set result [lindex [aidb_fetch $handle] 0]} errmsg ] {
		WriteTrace $errmsg 1002
		puts $errmsg
		exit -1
	}
	aidb_commit $conn
	aidb_close $handle
	
	
	return $result
}