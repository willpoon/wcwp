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
        set app_name "INT_CHECK_22204_MONTH.tcl"

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
	                    and rule_code in ('R117','R118','R119','R121','R122','R123')"
	puts ${sqlbuf}
	if [ catch { aidb_sql $handle $sqlbuf } errmsg ] {
		WriteTrace "$errmsg" 10000
		return -1
	}
	aidb_commit $conn
	aidb_close $handle

########################################################################################################3
	#--R117:TD手机客户数=188号段的TD手机客户+157号段的TD手机客户+两网融合的TD手机客户

  set handle [aidb_open $conn]
	set sql_buff "select bigint(TD_MOBILE_CUSTOMER_CNT)-bigint(TD_188_CNT)-bigint(TD_157_CNT)-bigint(TD_GSM_CNT)
                from BASS1.G_S_22204_MONTH
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
		INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R117',$DEC_RESULT_VAL1,0,0,0) "
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace $errmsg 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn

	#--判断
	#--异常
	#--1：TD手机客户数=188号段的TD手机客户+157号段的TD手机客户+两网融合的TD手机客户


	if {[format %.3f [expr ${DEC_RESULT_VAL1} ]]!=0 } {
		set grade 2
	        set alarmcontent " R117 校验不通过"
	        WriteAlarm $app_name $optime $grade ${alarmcontent}
		 }



########################################################################################################3
	#--R118:TD数据卡客户数=157号段的TD数据卡客户+147号段的TD数据卡客户+两网融合的数据卡客户

  set handle [aidb_open $conn]
	set sql_buff "select bigint(TD_DATACARD_CNT)-bigint(TD_157_DATACARD_CNT)-bigint(TD_147_DATACARD_CNT)-bigint(TD_GSM_DATACARD_CNT)
                from BASS1.G_S_22204_MONTH
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
		INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R118',$DEC_RESULT_VAL1,0,0,0) "
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace $errmsg 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn

	#--判断
	#--异常
	#--1：TD数据卡客户数=157号段的TD数据卡客户+147号段的TD数据卡客户+两网融合的数据卡客户


	if {[format %.3f [expr ${DEC_RESULT_VAL1} ]]!=0 } {
		set grade 2
	        set alarmcontent " R118 校验不通过"
	        WriteAlarm $app_name $optime $grade ${alarmcontent}
		 }


########################################################################################################3
	#--R119:TD手机客户数=全球通TD手机客户数+神州行TD手机客户数+动感地带TD手机客户数

  set handle [aidb_open $conn]
	set sql_buff "select bigint(TD_MOBILE_CUSTOMER_CNT)-bigint(TD_QQT_CNT)-bigint(TD_SZX_CNT)-bigint(TD_DGDD_CNT)
                from BASS1.G_S_22204_MONTH
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
		INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R119',$DEC_RESULT_VAL1,0,0,0) "
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace $errmsg 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn

	#--判断
	#--异常
	#--1：TD手机客户数=全球通TD手机客户数+神州行TD手机客户数+动感地带TD手机客户数


	if {[format %.3f [expr ${DEC_RESULT_VAL1} ]]!=0 } {
		set grade 2
	        set alarmcontent " R119 校验不通过"
	        WriteAlarm $app_name $optime $grade ${alarmcontent}
		 }



########################################################################################################3
	#--R121:使用过TD网络的TD客户数 ≤ TD客户总数

  set handle [aidb_open $conn]
	set sql_buff "select bigint(TD_TNET_USE_CNT)-bigint(TD_CUSTOMER_CNT)
               from BASS1.G_S_22204_MONTH
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
		INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R121',$DEC_RESULT_VAL1,0,0,0) "
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace $errmsg 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn

	#--判断
	#--异常
	#--1：使用过TD网络的TD客户数 ≤ TD客户总数


	if {[format %.3f [expr ${DEC_RESULT_VAL1} ]]>0 } {
		set grade 2
	        set alarmcontent " R121 校验不通过"
	        WriteAlarm $app_name $optime $grade ${alarmcontent}
		 }




########################################################################################################3
	#--R122:使用过TD网络的手机客户数 ≤ TD手机客户数

  set handle [aidb_open $conn]
	set sql_buff "select bigint(TNET_USE_CNT)-bigint(TD_MOBILE_CUSTOMER_CNT)
                from BASS1.G_S_22204_MONTH
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
		INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R122',$DEC_RESULT_VAL1,0,0,0) "
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace $errmsg 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn

	#--判断
	#--异常
	#--1：使用过TD网络的手机客户数 ≤ TD手机客户数


	if {[format %.3f [expr ${DEC_RESULT_VAL1} ]]>0 } {
		set grade 2
	        set alarmcontent " R122 校验不通过"
	        WriteAlarm $app_name $optime $grade ${alarmcontent}
		 }



########################################################################################################3
	#--R123:使用过TD网络的数据卡客户数 ≤ TD数据卡客户数

  set handle [aidb_open $conn]
	set sql_buff "select bigint(TNET_USE_DATACARD_CNT)-bigint(TD_DATACARD_CNT)
                from BASS1.G_S_22204_MONTH
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
		INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R123',$DEC_RESULT_VAL1,0,0,0) "
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace $errmsg 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn

	#--判断
	#--异常
	#--1：使用过TD网络的数据卡客户数 ≤ TD数据卡客户数


	if {[format %.3f [expr ${DEC_RESULT_VAL1} ]]>0 } {
		set grade 2
	        set alarmcontent " R123 校验不通过"
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