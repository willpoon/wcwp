######################################################################################################
#程序名称：INT_CHECK_06031_MONTH.tcl
#校验接口：
#运行粒度: 月
#输入参数: 
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：zhanght
#编写时间：2009-06-25
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
        set curr_month [string range $op_time 0 3][string range $op_time 5 6]

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
        set app_name "INT_CHECK_06031_MONTH.tcl"

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
	                    and rule_code in ('R072','R073','R074','R075')"
	puts ${sqlbuf}
	if [ catch { aidb_sql $handle $sqlbuf } errmsg ] {
		WriteTrace "$errmsg" 10000
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
      
        
########################### R072: 7位号段与9位号段的关系 ####################################

  set handle [aidb_open $conn]
	set sql_buff "select count(distinct ltrim(rtrim(MSISDN)))
                  from BASS1.G_I_06031_DAY
                where time_id/100=$op_month
                  and length(ltrim(rtrim(MSISDN)))=7
                  and ltrim(rtrim(MSISDN)) in (
                                               select distinct ltrim(rtrim(MSISDN)) from BASS1.G_I_06031_DAY
                                               where time_id/100=$op_month
                							    and length(ltrim(rtrim(MSISDN)))=9
                							   ) with ur"

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
		INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R072',$DEC_RESULT_VAL1,0,0,0) "
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace $errmsg 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn

	#--判断
	#--异常
	#--1：7位长度的msisdn号段与9位长度的msisdn号段之间不能有前7位相同情况


	if {[format %.3f [expr ${DEC_RESULT_VAL1} ]]>0 } {
		set grade 2
	        set alarmcontent " R072 校验不通过:7位长度的msisdn号段与9位长度的msisdn号段之间不能有前7位相同情况"
	        WriteAlarm $app_name $optime $grade ${alarmcontent}
		 }


########################### R073: 7位号段与8位号段的关系 ####################################

  set handle [aidb_open $conn]
	set sql_buff "select count(distinct ltrim(rtrim(MSISDN)))
                  from BASS1.G_I_06031_DAY
                where time_id/100=$op_month
                  and length(ltrim(rtrim(MSISDN)))=7
                  and ltrim(rtrim(MSISDN)) in (
                                               select distinct ltrim(rtrim(MSISDN)) from BASS1.G_I_06031_DAY
                                               where time_id/100=$op_month
                							    and length(ltrim(rtrim(MSISDN)))=8
                							   ) with ur"

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
		INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R073',$DEC_RESULT_VAL1,0,0,0) "
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace $errmsg 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn

	#--判断
	#--异常
	#--1：7位长度的msisdn号段与9位长度的msisdn号段之间不能有前7位相同情况


	if {[format %.3f [expr ${DEC_RESULT_VAL1} ]]>0 } {
		set grade 2
	        set alarmcontent " R073 校验不通过:7位长度的msisdn号段与8位长度的msisdn号段之间不能有前7位相同情况"
	        WriteAlarm $app_name $optime $grade ${alarmcontent}
		 }




########################### R074: 8位号段与9位号段的关系 ####################################

  set handle [aidb_open $conn]
	set sql_buff "select count(distinct ltrim(rtrim(MSISDN)))
                  from BASS1.G_I_06031_DAY
                where time_id/100=$op_month
                  and length(ltrim(rtrim(MSISDN)))=7
                  and ltrim(rtrim(MSISDN)) in (
                                               select distinct ltrim(rtrim(MSISDN)) from BASS1.G_I_06031_DAY
                                               where time_id/100=$op_month
                							    and length(ltrim(rtrim(MSISDN)))=8
                							   ) with ur"

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
		INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R074',$DEC_RESULT_VAL1,0,0,0) "
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace $errmsg 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn

	#--判断
	#--异常
	#--1：7位长度的msisdn号段与9位长度的msisdn号段之间不能有前7位相同情况


	if {[format %.3f [expr ${DEC_RESULT_VAL1} ]]>0 } {
		set grade 2
	        set alarmcontent " R074 校验不通过:8位长度的msisdn号段与9位长度的msisdn号段之间不能有前7位相同情况"
	        WriteAlarm $app_name $optime $grade ${alarmcontent}
		 }


########################### R075: msisdn号段不能有竞争对手的号段存在 ####################################

  set handle [aidb_open $conn]
	set sql_buff "select count(*)
                  from BASS1.G_I_06031_DAY
                where time_id/100=$op_month
                  and substr(ltrim(rtrim(MSISDN)),1,3) in ('130','131','132','133','153','155','156','180','185','186','189') 
                 with ur"

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
		INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R075',$DEC_RESULT_VAL1,0,0,0) "
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace $errmsg 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn

	#--判断
	#--异常
	#--1：msisdn号段不能有竞争对手的号段存在


	if {[format %.3f [expr ${DEC_RESULT_VAL1} ]]>0 } {
		set grade 2
	        set alarmcontent " R075 校验不通过: msisdn号段不能有竞争对手的号段存在"
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

