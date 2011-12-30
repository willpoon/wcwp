######################################################################################################
#程序名称：INT_CHECK_22073_DAY.tcl
#校验接口：G_S_22073_DAY.tcl
#运行粒度: 日
#输入参数: 
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：zhanght
#编写时间：2009-05-30
#问题记录：
#修改历史: 20091201以前的月接口在1.6.4版本修改为日校验接口。
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

        #本月 yyyymm
        set op_month [string range $optime_month 0 3][string range $optime_month 5 6]
            
        #上月  yyyymm
        set last_month [GetLastMonth [string range $op_month 0 5]]
        
        #自然月第一天 yyyymmdd
        set timestamp [string range $op_time 0 3][string range $op_time 5 6]01
        
        #本月第一天 yyyymmdd
        set l_timestamp [string range $optime_month 0 3][string range $optime_month 5 6]01
        
        #当天 yyyymmdd
        set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
        #当天 yyyy-mm-dd
        set optime $op_time
        #前一天 yyyymmdd
        set last_day [GetLastDay [string range $timestamp 0 7]]
        #程序名
        set app_name "INT_CHECK_22073_DAY.tcl"

	set handle [ aidb_open $conn ]
	set sqlbuf "delete from bass1.g_rule_check where time_id = $timestamp 
	                    and rule_code in ('R063','R064','R065','R066','R067','R068','R069','R070','R071')"
	puts ${sqlbuf}
	if [ catch { aidb_sql $handle $sqlbuf } errmsg ] {
		WriteTrace "$errmsg" 10000
		return -1
	}
	aidb_commit $conn
	aidb_close $handle


	#--R063:联通GSM156号段新增客户数小于等于联通GSM新增客户数

  set handle [aidb_open $conn]
	set sql_buff "select count(*)
  from bass1.G_S_22073_DAY
  where int(UNION_156_NEW_ADD_CNT)>int(UNION_MOBILE_NEW_ADD_CNT)
  and time_id=$timestamp
  with ur"

	 puts ${sql_buff}
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
		INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R063',$DEC_RESULT_VAL1,0,0,0) "
		
	puts ${sql_buff}
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace $errmsg 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn

	#--判断
	#--异常
	#--1：联通GSM156号段新增客户数小于等于联通GSM新增客户数


	if {[format %.3f [expr ${DEC_RESULT_VAL1} ]]>0 } {
		set grade 2
	        set alarmcontent " R063校验不通过：联通GSM156号段新增客户数大于联通GSM新增客户数"
	        WriteAlarm $app_name $optime $grade ${alarmcontent}
		 }




	#--R064:联通GSM156号段客户到达数小于等于联通GSM客户到达数

  set handle [aidb_open $conn]
	set sql_buff "select count(*)
                from bass1.G_S_22073_DAY
                where int(UNION_156_ARRIVE_CNT)>int(UNION_MOBILE_ARRIVE_CNT)
                and time_id=$timestamp
                with ur"
   puts ${sql_buff}
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

	#puts $DEC_RESULT_VAL1

	#--将校验值插入校验结果表
	set handle [aidb_open $conn]
	set sql_buff "\
		INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R064',$DEC_RESULT_VAL1,0,0,0) "
		
	puts ${sql_buff}
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace $errmsg 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn

	#--判断
	#--异常
	#--1：联通GSM156号段客户到达数小于等于联通GSM客户到达数


	if {[format %.3f [expr ${DEC_RESULT_VAL1} ]]>0 } {
		set grade 2
	        set alarmcontent " R064校验不通过：联通GSM156号段客户到达数大于联通GSM客户到达数"
	        WriteAlarm $app_name $optime $grade ${alarmcontent}
		 }
		 
		 

	#--R065:联通GSM156号段离网客户数小于等于联通GSM离网客户数

  set handle [aidb_open $conn]
	set sql_buff "select count(*)
                from bass1.G_S_22073_DAY
                where int(UNION_156_LOST_CNT)>int(UNION_MOBILE_LOST_CNT)
                and time_id=$timestamp
                with ur"

   puts ${sql_buff}
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

	#puts $DEC_RESULT_VAL1

	#--将校验值插入校验结果表
	set handle [aidb_open $conn]
	set sql_buff "\
		INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R065',$DEC_RESULT_VAL1,0,0,0) "
		
	puts ${sql_buff}
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace $errmsg 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn

	#--判断
	#--异常
	#--1：联通GSM156号段离网客户数小于等于联通GSM离网客户数


	if {[format %.3f [expr ${DEC_RESULT_VAL1} ]]>0 } {
		set grade 2
	        set alarmcontent " R065校验不通过：联通GSM156号段离网客户数大于联通GSM离网客户数"
	        WriteAlarm $app_name $optime $grade ${alarmcontent}
		 }
		 
		 
	#R066

  set handle [aidb_open $conn]
	set sql_buff "select count(*)
									from bass1.G_S_22073_DAY
									where int(TEL_133_NEW_ADD_CNT)>int(TEL_MOBILE_NEW_ADD_CNT)
									and time_id=$timestamp
									with ur"
   
   puts ${sql_buff}
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

	#puts $DEC_RESULT_VAL1

	#--将校验值插入校验结果表
	set handle [aidb_open $conn]
	set sql_buff "\
		INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R066',$DEC_RESULT_VAL1,0,0,0) "
		
	puts ${sql_buff}
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace $errmsg 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn

	#--判断
	#--异常
	#--1


	if {[format %.3f [expr ${DEC_RESULT_VAL1} ]]>0 } {
		set grade 2
	        set alarmcontent " R066校验不通过"
	        WriteAlarm $app_name $optime $grade ${alarmcontent}
		 }
		 
		 

	#R067

  set handle [aidb_open $conn]
	set sql_buff "select count(*)
									from bass1.G_S_22073_DAY
									where int(TEL_189_NEW_ADD_CNT)>int(TEL_MOBILE_NEW_ADD_CNT)
									and time_id=$timestamp
									with ur"
									
   puts ${sql_buff}
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

	#puts $DEC_RESULT_VAL1

	#--将校验值插入校验结果表
	set handle [aidb_open $conn]
	set sql_buff "\
		INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R067',$DEC_RESULT_VAL1,0,0,0) "
		
	puts ${sql_buff}
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace $errmsg 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn

	#--判断
	#--异常
	#--1


	if {[format %.3f [expr ${DEC_RESULT_VAL1} ]]>0 } {
		set grade 2
	        set alarmcontent " R067校验不通过"
	        WriteAlarm $app_name $optime $grade ${alarmcontent}
		 }
		 
		 

	#R068

  set handle [aidb_open $conn]
	set sql_buff "select count(*)
									from bass1.G_S_22073_DAY
									where int(TEL_133_ARRIVE_CNT)>int(TEL_MOBILE_ARRIVE_CNT)
									and time_id=$timestamp
									with ur"
   
   puts ${sql_buff}
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

	#puts $DEC_RESULT_VAL1

	#--将校验值插入校验结果表
	set handle [aidb_open $conn]
	set sql_buff "\
		INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R068',$DEC_RESULT_VAL1,0,0,0) "
		
	puts ${sql_buff}
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace $errmsg 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn

	#--判断
	#--异常
	#--1


	if {[format %.3f [expr ${DEC_RESULT_VAL1} ]]>0 } {
		set grade 2
	        set alarmcontent " R068校验不通过"
	        WriteAlarm $app_name $optime $grade ${alarmcontent}
		 }
		 
		 
		 
	#R069

  set handle [aidb_open $conn]
	set sql_buff "select count(*)
									from bass1.G_S_22073_DAY
									where int(TEL_189_ARRIVE_CNT)>int(TEL_MOBILE_ARRIVE_CNT)
									and time_id=$timestamp
									with ur"
   
   puts ${sql_buff}
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

	#puts $DEC_RESULT_VAL1

	#--将校验值插入校验结果表
	set handle [aidb_open $conn]
	set sql_buff "\
		INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R069',$DEC_RESULT_VAL1,0,0,0) "
		
	puts ${sql_buff}
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace $errmsg 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn

	#--判断
	#--异常
	#--1


	if {[format %.3f [expr ${DEC_RESULT_VAL1} ]]>0 } {
		set grade 2
	        set alarmcontent " R069校验不通过"
	        WriteAlarm $app_name $optime $grade ${alarmcontent}
		 }
		 
		 
		 
	#R070

  set handle [aidb_open $conn]
	set sql_buff "select count(*)
									from bass1.G_S_22073_DAY
									where int(TEL_133_LOST_CNT)>int(TEL_MOBILE_LOST_CNT)
									and time_id=$timestamp
									with ur"
   
   puts ${sql_buff}
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

	#puts $DEC_RESULT_VAL1

	#--将校验值插入校验结果表
	set handle [aidb_open $conn]
	set sql_buff "\
		INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R070',$DEC_RESULT_VAL1,0,0,0) "
		
	puts ${sql_buff}
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace $errmsg 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn

	#--判断
	#--异常
	#--1


	if {[format %.3f [expr ${DEC_RESULT_VAL1} ]]>0 } {
		set grade 2
	        set alarmcontent " R070校验不通过"
	        WriteAlarm $app_name $optime $grade ${alarmcontent}
		 }
		 
		 

	#R071

  set handle [aidb_open $conn]
	set sql_buff "select count(*)
									from bass1.G_S_22073_DAY
									where int(TEL_189_LOST_CNT)>int(TEL_MOBILE_LOST_CNT)
									and time_id=$timestamp
									with ur"
   
   puts ${sql_buff}
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

	#puts $DEC_RESULT_VAL1

	#--将校验值插入校验结果表
	set handle [aidb_open $conn]
	set sql_buff "\
		INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'R071',$DEC_RESULT_VAL1,0,0,0) "
		
	puts ${sql_buff}
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace $errmsg 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn

	#--判断
	#--异常
	#--1


	if {[format %.3f [expr ${DEC_RESULT_VAL1} ]]>0 } {
		set grade 2
	        set alarmcontent " R071校验不通过"
	        WriteAlarm $app_name $optime $grade ${alarmcontent}
		 }		 		 		 		 		 		 
		 		 
		
	return 0
}
