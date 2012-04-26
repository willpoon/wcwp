#!/bassapp/tcl/aiomnivision/aitools/bin/tclsh

#command \
exec tclsh8.4 "$0" "$@"
if {$tcl_version >8.0} {
        encoding system identity
        fconfigure stdout -encoding identity
}

#############################################################################################
#	程序名：	int
#	功能描述： 	一经接口数据计算程序的主程序。
#	命令行参数：	-s <tcl程序名> -d <数据库连接串> [-u 调度号] [-v 任务号]
#	创建日期： 	2004-03-15
#	copyright (c) 2003 AsiaInfo. All Rights Reserved.
#############################################################################################

# 函数定义
#################################################################
# 函数名称: GetThisMonthDays
# 功能描述: 求本月天数
# 输入参数:
#	data_time		格式为“yyyymm”
# 输出参数:
# 返回值:   本月天数,格式为"dd".如果出错,返回-1
# 参考贵州程序 添加 BY TYM
#################################################################
proc GetThisMonthDays { data_time } {
	if { [string length $data_time] != 8 || [string range $data_time 0 3] < "1980" || [string range $data_time 0 3] > "2020" || [string range $data_time 4 5] > "12" || [string range $data_time 6 6] > "31" } {
		WriteTrace "The data_time($data_time) is not correct!" 0110
		return -1
	}

	if { [string range $data_time 6 7] == "01" } {
		switch [string range $data_time 4 5] {
			01 {return 31}
			03 {return 31}
			02 {
				if { [expr [string range $data_time 0 3] % 4] == 0 && [expr [string range $data_time 0 3] % 100] != 0 } {
					return 29
				} elseif { [expr [string range $data_time 0 3] % 100] == 0 && [expr [string range $data_time 0 3] % 400] == 0 } {
				        return 29
				} else {
					return 28
				}
			   }
			04 {return 30}
			05 {return 31}
			06 {return 30}
			07 {return 31}
			08 {return 31}
			09 {return 30}
			10 {return 31}
			11 {return 30}
			12 {return 31}
			default {
				WriteTrace "The data_time($data_time) is not correct!" 0111
				return -1
			   }
		}
	} else {
		return 31 ]
	}
}
#################################################################
# 函数名称: GetThisYearDays
# 功能描述: 求本年天数
# 输入参数:
#	data_time		格式为“yyyymm”
# 输出参数:
# 返回值:   本年天数,格式为"ddd".如果出错,返回-1
# 参考贵州程序 添加 BY TYM
#################################################################
proc GetThisYearDays { data_time } {
	if { [string length $data_time] != 8 || [string range $data_time 0 3] < "1980" || [string range $data_time 0 3] > "2020" || [string range $data_time 4 5] > "12" || [string range $data_time 6 6] > "31" } {
		WriteTrace "The data_time($data_time) is not correct!" 0110
		return -1
	}

        if { [expr [string range $data_time 0 3] % 4] == 0 && [expr [string range $data_time 0 3] % 100] != 0 } {
		return 366
	} elseif { [expr [string range $data_time 0 3] % 100] == 0 && [expr [string range $data_time 0 3] % 400] == 0 } {
		return 366
	} else {
		return 365
	}
}

#################################################################
# 函数名称: GetLastMonth
# 功能描述: 求上个月
# 输入参数:
#	data_time		格式为“yyyymm”
# 输出参数:
# 返回值:   上个月,格式为"yyyymm".如果出错,返回-1
#################################################################
proc GetLastMonth { data_time } {
	if { [string length $data_time] != 6 || [string range $data_time 0 3] < "1980" || [string range $data_time 0 3] > "2020" || [string range $data_time 4 5] < "01" || [string range $data_time 4 5] > "12" } {
		WriteTrace "The data_time($data_time) is not correct!" 0101
		return -1
	}

	if { [string range $data_time 4 5] == "01" } {
		return [expr [string range $data_time 0 3] - 1]12
	} else {
		return [string range $data_time 0 3][format "%02d" [expr [string trimleft [string range $data_time 4 5] 0] - 1] ]
	}
}


#################################################################
# 函数名称: GetNextMonth
# 功能描述: 求下个月
# 输入参数:
#	data_time		格式为“yyyymm”
# 输出参数:
# 返回值:   下个月,格式为"yyyymm".如果出错,返回-1
#################################################################
proc GetNextMonth { data_time } {
	if { [string length $data_time] != 6 || [string range $data_time 0 3] < "1980" || [string range $data_time 0 3] > "2020" || [string range $data_time 4 5] < "01" || [string range $data_time 4 5] > "12" } {
		WriteTrace "The data_time($data_time) is not correct!" 0101
		return -1
	}

	if { [string range $data_time 4 5] == "12" } {
		return [expr [string range $data_time 0 3] + 1]01
	} else {
		return [string range $data_time 0 3][format "%02d" [expr [string trimleft [string range $data_time 4 5] 0] + 1] ]
	}
}



#################################################################
# 函数名称: GetLastDay
# 功能描述: 求前一天
# 输入参数:
#	data_time		格式为“yyyymmdd”
# 输出参数:
# 返回值:   前一天,格式为"yyyymmdd".如果出错,返回-1
#################################################################
proc GetLastDay { data_time } {
	if { [string length $data_time] != 8 || [string range $data_time 0 3] < "1980" || [string range $data_time 0 3] > "2020" || [string range $data_time 4 5] > "12" || [string range $data_time 6 6] > "31" } {
		WriteTrace "The data_time($data_time) is not correct!" 0110
		return -1
	}

	if { [string range $data_time 6 7] == "01" } {
		switch [string range $data_time 4 5] {
			01 {return [expr [string range $data_time 0 3] - 1]1231}
			02 {return [string range $data_time 0 3]0131}
			03 {
				if { [expr [string range $data_time 0 3] % 4] == 0 && [expr [string range $data_time 0 3] % 400] != 0 } {
					return [string range $data_time 0 3]0229
				} else {
					return [string range $data_time 0 3]0228
				}
			   }
			04 {return [string range $data_time 0 3]0331}
			05 {return [string range $data_time 0 3]0430}
			06 {return [string range $data_time 0 3]0531}
			07 {return [string range $data_time 0 3]0630}
			08 {return [string range $data_time 0 3]0731}
			09 {return [string range $data_time 0 3]0831}
			10 {return [string range $data_time 0 3]0930}
			11 {return [string range $data_time 0 3]1031}
			12 {return [string range $data_time 0 3]1130}
			default {
				WriteTrace "The data_time($data_time) is not correct!" 0111
				return -1
			   }
		}
	} else {
		return [string range $data_time 0 3][string range $data_time 4 5][format "%02d" [expr [string trimleft [string range $data_time 6 7] 0] - 1] ]
	}
}





#################################################################
# 函数名称: Initialize
# 功能描述: 读int_program_data，取该程序对应的src_data和obj_data; 读一经接口程序专用控制表（USYS_INT_CONTROL），并且检查参数的正确性
# 输入参数:	均是全局变量
#	trace_fd	trace文件描述符
#	script		deal过程所在的tcl程序名，去掉tcl扩展名的。
#	program_name	deal过程所在的tcl程序名，包含tcl扩展名的。
# 输出参数:	均是全局变量
#	province_id	省份编码，格式为两位数字字符
#	op_time		数据时间（批次）,“yyyy-mm-dd”
#	data_time	含义同op_time，格式为“yyyymmdd”
#	optime_month	数据月份,供月处理程序使用,格式为"yyyy-mm"
#	redo_number	重传序号,两位数字字符。当重传次数超过99次时，重传次数为99。
#	is_normal_proc	是否正常处理（本次处理是否正常处理）。"0"表示正常处理，这种情况下，全部数据处理程序都将会运行一遍，生成最终数据文件；"1"表示异常处理（重传），这种情况说明校验报告分析程序发现上一周期的数据发生错误，人工调整数据源之后，本次程序运行将重新产生之前出错的数据文件，文件名里的重传序号加一。每次程序执行之前，需要确认该标志配置正确。
#	src_data	数据源，数据库名或文件名，某些程序是没有源
#	obj_data	数据目标，数据名或文件名，某些程序是没有目的
# 返回值:   0 成功； -1 数据库查询失败; -2 参数格式错误
#################################################################

proc Initialize {  } {
	global trace_fd
	global script
	global program_name
	global src_data
	global obj_data
	global final_data
	global province_id
	global op_time
	global optime_month
	global data_time
	global redo_number
	global is_normal_proc
	global modify_counts

	global conn
	set handle [aidb_open $conn]
	
#	取src_data、obj_data和final_data
	set sql_buff "SELECT source_data,objective_data,final_data FROM bass1.int_program_data WHERE program_name='$program_name'"

	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		  WriteTrace $errmsg 0032
		  aidb_close $handle
		  return -111
	}
	aidb_commit $conn

	if [catch {
			set sof_data [aidb_fetch $handle]
			set src_data [lindex $sof_data 0]
			set obj_data [lindex $sof_data 1]
			set final_data [lindex $sof_data 2]

			if { $src_data == "" } {
				WriteTrace "Warning: the value of src_data is NULL" 0081
			}
			if { ![info exists obj_data] } {
				WriteTrace "Warning: the value of obj_data is NULL" 0082
			}
			if { $final_data == "" } {
				puts "Can not find the $program_name in bass1.int_program_data"
				error "sql=$sql_buff,结果集为空"
			}
		} errmsg ] {
			WriteTrace $errmsg 0033
			aidb_close $handle
			return -2
	}

#	if { [string length $src_data] > 30 } {
#		WriteTrace "Warning: The src_data's length exceed 30 bytes. The string has been cut." 0034
#	}
#	set src_data [format %-30s [string range $src_data 0 29]]
#
#	if { [string length $obj_data] > 30 } {
#		WriteTrace "Warning: The obj_data's length exceed 30 bytes. The string has been cut." 0035
#	}
#	set obj_data [format %-30s [string range $obj_data 0 29]]
#
#	if { [string length $final_data] > 30 } {
#		WriteTrace "Warning: The obj_data's length exceed 30 bytes. The string has been cut." 0035
#	}
#	set final_data [format %-30s [string range $final_data 0 29]]

	aidb_close $handle
	set handle [aidb_open $conn]

#	取省市编码
#	set sql_buff "SELECT para_value FROM bass1.usys_int_control WHERE para_name='province_id'"
#
#	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
#		  WriteTrace $errmsg 0002
#		  aidb_close $handle
#		  return -3
#	}
#	aidb_commit $conn
#	if [catch {
#			set province_id [lindex [aidb_fetch $handle] 0]
#			if { $province_id == ""} {
#				puts "取参数province_id时发生错误。"
#				error "sql=$sql_buff,结果集为空"
#			}
#		} errmsg ] {
#			WriteTrace $errmsg 0003
#			aidb_close $handle
#			return -4
#	}
#	if { [string length $province_id] != 5 } {
#		WriteTrace "Error: The province_id($province_id) is not correct!" 0004
#		aidb_close $handle
#		return -5
#	}
#	aidb_close $handle
#	set handle [aidb_open $conn]
        
        set province_id "13100"

#	取数据时间（批次）
	set sql_buff "\
	      SELECT 
	        case 
	          when para_value='0' then char(current date - 1 days)
	          else para_value
	        end
	      FROM bass1.usys_int_control 
	      WHERE para_name='op_time'"
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		  WriteTrace $errmsg 0005
		  aidb_close $handle
		  return -6
	}
	aidb_commit $conn
	if [catch {
			set op_time [lindex [aidb_fetch $handle] 0]
			if { $op_time == ""} {
				puts "取参数op_time时发生错误。"
				error "sql=$sql_buff,结果集为空"
			}
		} errmsg ] {
			WriteTrace $errmsg 0006
			aidb_close $handle
			return -7
	}
	if { [string length $op_time] != 10 || [string range $op_time 4 4] != "-" || [string range $op_time 0 3] < "1980" || [string range $op_time 0 3] > "2020" || [string range $op_time 5 6] > "12" || [string range $op_time 8 9] > "31" } {
		WriteTrace "The op_time($op_time) is not correct!" 0007
		aidb_close $handle
		return -8
	}
	aidb_close $handle
	set handle [aidb_open $conn]

#	将op_time转为data_time
	set data_time [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]

#	取数据月份
	set sql_buff "\
	       SELECT 
	         case
	           when para_value='0' then  substr(char(current date - 1 months),1,7)
	           else para_value
	         end
	       FROM 
	         bass1.usys_int_control 
	       WHERE 
	         para_name='op_time_month'"
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		  WriteTrace $errmsg 0205
		  aidb_close $handle
		  return -9
	}
	aidb_commit $conn

	if [catch {
			set optime_month [lindex [aidb_fetch $handle] 0]
			if { $optime_month == ""} {
				puts "取参数op_time_month时发生错误。"
				error "sql=$sql_buff,结果集为空"
			}
		} errmsg ] {
			WriteTrace $errmsg 0206
			aidb_close $handle
			return -10
	}
	if { [string length $optime_month] != 7 || [string range $optime_month 4 4] != "-" || [string range $optime_month 0 3] < "1980" || [string range $optime_month 0 3] > "2020" || [string range $optime_month 5 6] > "12" || [string range $optime_month 5 6] < "01" } {
		WriteTrace "The optime_month($optime_month) is not correct!" 0207
		aidb_close $handle
		return -11
	}
	aidb_close $handle
	set handle [aidb_open $conn]

#	取is_normal_proc(本次处理是否正常处理)
	set sql_buff "SELECT para_value FROM bass1.usys_int_control WHERE para_name='is_normal_proc'"
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		  WriteTrace $errmsg 0008
		  aidb_close $handle
		  return -12
	}
	aidb_commit $conn

	if [catch {
			set is_normal_proc [lindex [aidb_fetch $handle] 0]
			if { $is_normal_proc == ""} {
				puts "取参数is_normal_proc时发生错误。"
				error "sql=$sql_buff,结果集为空"
			}
		} errmsg ] {
			WriteTrace $errmsg 0009
			aidb_close $handle
			return -13
	}
	puts $trace_fd "The is_normal_proc is: $is_normal_proc."
	aidb_close $handle
	set handle [aidb_open $conn]

##       	取redo_number(重传序号)
#	set sql_buff "\
#	SELECT					\
#		MAX(redo_number)		\
#			,MAX(A.modify_counts)		\
#		FROM					\
#			bass1.int_all_job_log A,	\
#			(SELECT				\
#				MAX(modify_counts) AS modify_counts	\
#			 FROM				\
#				bass1.int_all_job_log	\
#			 WHERE				\
#			 	op_time='$op_time'	\
#			) B				\
#		WHERE					\
#			A.modify_counts=B.modify_counts	\
#			AND A.op_time='$op_time'"
#	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
#		  WriteTrace $errmsg 0011
#		  aidb_close $handle
#		  return -14
#	}
#	aidb_commit $conn
#
#	if [catch {
#			set res [aidb_fetch $handle]
#			set redo_number [lindex $res 0]
#			set modify_counts [lindex $res 1]
#			if { $redo_number == "" } {
#				puts "取参数redo_number时发生错误。"
#				error "sql=$sql_buff,结果集为空"
#			}
#		} errmsg ] {
#			WriteTrace $errmsg 0012
#			aidb_close $handle
#			return -15
#	}
#	if { [string is integer [string trimleft $redo_number 0]] == 0} {
#		WriteTrace "Error: The redo_number('$redo_number') is not correct!" 0013
#		aidb_close $handle
#		return -2
#	}
        set redo_number "00"
        set modify_counts "00"
	puts $trace_fd "The redo_number is: $redo_number."

	aidb_close $handle


	return 0
}

#################################################################
# 函数名称: Decide
# 功能描述: 决定是否继续运行program_name这个程序
# 输入参数:	均是全局变量
#	trace_fd	trace文件描述符
#	script		deal过程所在的tcl程序名，去掉tcl扩展名的。
#	data_time		数据时间（批次）,“yyyymmdd”
#	is_normal_proc	是否正常处理（本次处理是否正常处理）。"0"表示正常处理，这种情况下，全部数据处理程序都将会运行一遍，生成最终数据文件；"1"表示异常处理（重传），这种情况说明校验报告分析程序发现上一周期的数据发生错误，人工调整数据源之后，本次程序运行将重新产生之前出错的数据文件，文件名里的重传序号加一。每次程序执行之前，需要确认该标志配置正确。
# 输出参数:
# 返回值:   0 程序可以继续运行下去；1 程序无需继续运行下去； -1 数据库查询失败
#################################################################

proc Decide {  } {
	global trace_fd
	global script
	global program_name
	global data_time
	global is_normal_proc

	global conn

#	决定是否继续运行该程序
	if { $is_normal_proc == 1 || $is_normal_proc == 9 || $is_normal_proc == 3 } {
		return 0
	} else {
		#如果该程序对应的最终接口单元数据文件出现在错误文件列表中,则返回1.否则返回空
		set sql_buff "\
			SELECT	'1'				\
			FROM	bass1.int_program_data A,	\
				bass1.int_verf_err_list B	\
			WHERE	A.program_name='$program_name'	\
				AND A.final_data=B.data_file_name"

		set handle [aidb_open $conn]
		if [catch { aidb_sql $handle $sql_buff } errmsg ] {
			WriteTrace $errmsg 0041
			aidb_close $handle
			return -1
		}
		set is_process [lindex [aidb_fetch $handle] 0]
		if { $is_process == ""} {
			WriteTrace "The program $program_name should not be processed."  0042
			aidb_close $handle
			return 1
		} else {
			WriteTrace "The program $program_name should be processed. The data_time is: $data_time."  0043
			aidb_close $handle
			return 0
		}

	}

}


####设计有变动.重传序号不从日志获得,改为从bass1.int_all_job_log获得.在函数Initialize()中实现.
#####################################################################
##### 函数名称: ReadLog
##### 功能描述: 读日志文件中的记录，确定重传序号
##### 输入参数:	均是全局变量
#####	trace_fd	trace文件描述符
#####	script		deal过程所在的tcl程序名，去掉tcl扩展名的。
#####	data_time	数据时间（批次）,“yyyymmdd”
##### 输出参数:	均是全局变量
#####	redo_number	重传序号,两位数字字符。当重传次数超过99次时，重传次数为99。
#####
##### 返回值:   0 成功； -1 取环境变量BASS1LOGDIR失败; -2 打开日志文件错误
#####################################################################
####
####proc ReadLog {  } {
####	global env
####	global trace_fd
####	global script
####	global data_time
####	global redo_number
####
####	if {! [info exists env(BASS1LOGDIR)]} {
####		WriteTrace "Error: cannot get env varialbe BASS1LOGDIR" 0051
####		return "-1"
####	}
####	set log_dir $env(BASS1LOGDIR)
####
####	if {[file exists $log_dir] == 0 } {
####		file mkdir $log_dir
####	}
####
####	set len [string length $log_dir]
####	if { [string index $log_dir [expr $len - 1]] != "/" } {
####		set log_dir "$log_dir/"
####	}
####
####	set log_name "$log_dir$script.log"
####
####	if {[file exists $log_name] == 0 } {
####		set log_fd [open $log_name w]
####		close $log_fd
####	}
####
####
####	if [catch {open $log_name r} log_fd] {
####		WriteTrace "Can not open the log file '$log_name' !" 0052
####		return -2
####	} else {
#####		读该程序对应的log文件，取得相应批次（数据时间）对应的最后一条记录，即重传序号最大的记录；
#####		分解最后一条记录，读取成功标志和重传序号；
#####		根据该条记录的成功标志和重传序号确定本次生成数据文件的重传序号，原则是：如果没有找到相应批次的记录（表明这是第一次生成数据），则重传序号为"00"；如果成功（标志为"0"），则将重传序号加1；如果失败（标志为"1"），则重传序号不变；
####
####		set redo_number "00"
####
####		while { ! [eof $log_fd] } {
####			set row [gets $log_fd]
####			if { [string range $row 5 12] == $data_time } {
####				set pre_redo_num [string range $row 2 3]
####				if { $pre_redo_num >= $redo_number && [string range $row 0 0] == 0 } {
####					if { [string range $row 0 0] == 0 } {
####						set redo_number [format "%02d" [expr [string trimleft $pre_redo_num 0] + 1] ]
####					} else {
####						set redo_number $pre_redo_num
####					}
####				}
####			}
####		}
####
####		close $log_fd
####	}
####
####	if { [string compare $redo_number "100"] == 0 } {
####		set redo_number 99
####	}
####	WriteTrace "Has read the log file, the data_time and redo_number are:$data_time --- $redo_number.\n" 0053
####	return 0
####}



#################################################################
# 函数名称: GenLog
# 功能描述: 生成日志文件中的记录
# 输入参数:	均是全局变量
#	trace_fd	trace文件描述符
#	success_code	0 表示程序成功完成；1 表示连接数据库失败；4 表示程序失败完成
#	redo_number	重传序号,两位数字字符
#	script		deal过程所在的tcl程序名，去掉tcl扩展名的。
#	start_time	程序开始时间,“yyyymmdd hh:mm:ss”
#	end_time	程序结束时间,“yyyymmdd hh:mm:ss”
#	data_time	数据时间（批次）,“yyyymmdd”
#	src_data	数据源,源表或源文件名
#	obj_data	数据目标,目的表或目的文件名
# 输出参数:
# 返回值:   0 成功； -1 取环境变量BASS1LOGDIR失败
#################################################################

proc GenLog {  } {
	global env
	global trace_fd
	global success_code
	global redo_number
	global script
	global start_time
	global end_time
	global data_time
	global src_data
	global obj_data

	switch $success_code {
		0 {
			set  suc_code 0
		}
		default {
			set  suc_code 1
		}
	}


	if {! [info exists env(BASS1LOGDIR)]} {
		puts "Error: cannot get env varialbe BASS1LOGDIR"
		return "-1"
	}
	set log_dir $env(BASS1LOGDIR)

	if {[file exists $log_dir] == 0 } {
		file mkdir $log_dir
	}

	set len [string length $log_dir]
	if { [string index $log_dir [expr $len - 1]] != "/" } {
		set log_dir "$log_dir/"
	}

	set log_name "$log_dir$script.log"

	set buff "$suc_code,$redo_number,$data_time,$start_time,$end_time,$script,$src_data,$obj_data"
	set log_fd [open $log_name a]
	puts $log_fd $buff
	close $log_fd

	puts $trace_fd "log = $buff"

	return 0

}


#################################################################
# 函数名称: GenSchServerLog
# 功能描述: 生成schedule server所需的日志文件
# 输入参数: 均是全局变量
#	ddh		调度号；
#	rwh		任务号；
#	success_code	程序运行结果代码:
#		0 表示程序成功完成
#		1 表示打开trace文件失败
#		2 表示连接数据库失败
#		4 表示程序失败完成
# 输出参数:
# 返回值:   0 成功； -1 取环境变量BASS1LOGDIR失败
#################################################################

proc GenSchServerLog {  } {
	global env
	global script
	global trace_fd
	global ddh
	global rwh
	global success_code

	switch $success_code {
		0 {
			set  sys_clzt_dm 03
			set  s_notes "SUCCEED!!"
		}
		1 {
			set  sys_clzt_dm 04
			set  s_notes "FAIL TO OPEN THE TRACE FILE!!"
		}
		2 {
			set  sys_clzt_dm 04
			set  s_notes "FAIL TO CONNECT TO THE DATABASE!!"
		}
		3 {
			set  sys_clzt_dm 04
			set  s_notes "FAIL TO INITIALIZE THE PARAMETERS!!"
		}
		4 {
			set  sys_clzt_dm 04
			set  s_notes "FAIL!!"
		}
		default {
			set  sys_clzt_dm 04
			set  s_notes "FAIL!!"
		}
	}

	if {! [info exists env(BASS1LOGDIR)]} {
		puts "Error: cannot get env varialbe BASS1LOGDIR"
		return "-1"
	}
	set log_dir $env(BASS1LOGDIR)

	if {[file exists $log_dir] == 0 } {
		file mkdir $log_dir
	}

	set len [string length $log_dir]
	if { [string index $log_dir [expr $len - 1]] != "/" } {
		set log_dir "$log_dir/"
	}

	set log_name "$log_dir$ddh.log"
	set tmp_file "$log_dir$ddh.log.tmp"

	set log_fd [open $log_name w]
	set buff "update SYS_TASK_RUNLOG set PROGRAM_NAME=\"$script\",SYS_CLZT_DM=\"$sys_clzt_dm\",NOTES=\"$s_notes\" where DDH=$ddh\n"
	set buff "$sys_clzt_dm$buff"
	puts $log_fd $buff
	close $log_fd

	set tmp_fd [open $tmp_file w]
	puts $tmp_fd $buff
	close $tmp_fd

	puts $trace_fd "schedule server log = $buff"

	return "0"
}

#################################################################
# 函数名称: OpenTrace
# 功能描述: 打开trace文件，当trace文件超过64KB时，清空以前的记录。
# 输入参数: taskname tcl脚本名,去掉tcl扩展名的
# 输出参数:
# 返回值:   -1 打开或创建文件失败; 文件描述字符串  表示成功打开文件或创建文件
#################################################################

proc OpenTrace { taskname } {
	global env


	if {! [info exists env(BASS1TRACEDIR)]} {
		puts "Error: cannot get env varialbe BASS1TRACEDIR"
		return "-1"
	}
	set trace_dir $env(BASS1TRACEDIR)

	set len [string length $trace_dir]

	if { [string index $trace_dir [expr $len - 1]] != "/" } {
		set trace_dir "$trace_dir/"
	}
	set bass1 "bass1"
	set trace_dir "$trace_dir"

	if {[file exists $trace_dir] == 0 } {
		file mkdir $trace_dir
	}

	set file_name "$trace_dir/$taskname.trace"

	if {[file exists $file_name] == 0 } {
		return [open $file_name w]
	}

	if { [file size $file_name] > 65535 } {
		return [open $file_name w]
	}

	return [open $file_name a]
}

#################################################################
# 函数名称: WriteTrace
# 功能描述: 向已经打开的trace文件写一条记录
# 输入参数: errmsg 欲写入trace文件的字符串; line 行号或者是检查点的标记
# 输出参数:
# 返回值:   无
#################################################################

proc WriteTrace { msg line } {
	global trace_fd
	global script

	global program_name
	global data_time

	global conn

	set handle2 [aidb_open $conn]
   	set sqlbuf "select module,program_name,control_code from app.sch_control_map where program_name='$program_name' and module=2"

   	if [catch { aidb_sql $handle2 $sqlbuf } errmsg ] {
		puts $errmsg
		trace_sql $errmsg 1001
		return -1
	}

	aidb_commit $conn

	if [catch {
		set sof_data [aidb_fetch $handle2]
		set control_code [lindex $sof_data 2]

		if { $control_code == "" } {
			puts "Warning: You have not set the schedule name in table app.sch_control_map"
			return -1
		}
	} errmsg ] {
			aidb_close $handle2
			return -1
	}
	aidb_close $handle2
        
	set op_time [string range $data_time 0 3]-[string range $data_time 4 5]-[string range $data_time 6 7]
	if {$line=="1000"} {
		set grade 1
	} else {
		set grade 2
	}
#	#对错误信息进行简化处理(屏蔽[IBM][CLI Driver][DB2/SUN64]这些SQL错误)
#	set Strlen [string length ${msg}]
#	set newmsg  [string range ${msg} 29 $Strlen]
#	set msg ${newmsg}
        #去掉对line错误的读入
#       set sql_buff "insert into app.sch_control_alarm (control_code,cmd_line,grade,content,alarmtime,flag) values ('$control_code','int -s $program_name',$grade,'Error line:${line}; Error info:${msg}',current timestamp,-1)"
	if { [ string last "'" $msg ] != -1 } {
		set msg_list [ split $msg "'" ]
		set msg_list_length [ llength $msg_list ]
		for { set i 0 } { $i < $msg_list_length } { incr i } {
			set msg_list_content($i) [ lindex $msg_list $i ]
			append msg_content $msg_list_content($i)
		}
		set msg $msg_content
	}
	
	set handle3 [aidb_open $conn]
	set sql_buff "insert into app.sch_control_alarm (control_code,cmd_line,grade,content,alarmtime,flag) values ('$control_code','int -s $program_name',$grade,' Error info:${msg}',current timestamp,-1)"

	if [ catch { aidb_sql $handle3 $sql_buff } errmsg ] {
		puts $errmsg
		aidb_close $handle3
		return -1
	}
	aidb_commit $conn

	puts $trace_fd "file=$script,line=$line,msg=$msg \n"
	flush $trace_fd
}
#################################################################
# 函数名称: WriteAlarm
# 功能描述: 向告警表插入一条告警记录
# 输入参数: app_name 核查文件名 p_optime 当前时间(yyyy-mm-dd) grade 告警级别 alarmcontent 告警内容
# 输出参数:
# 返回值:   无
#################################################################
proc WriteAlarm {app_name p_optime grade alarmcontent} {
	
	global program_name
	global conn

#	set handle2 [aidb_open $conn]
#   	set sqlbuf "\
#   	          select 
#   	            module,program_name,control_code 
#   	          from 
#   	            app.sch_control_map 
#   	          where 
#   	            program_name='$program_name' and module=2"
#   	if [catch { aidb_sql $handle2 $sqlbuf } errmsg ] {
#		puts $errmsg
#		trace_sql $errmsg 1001
#		return -1
#	}
#	aidb_commit $conn
#	if [ catch {
#		set sof_data [aidb_fetch $handle2]
#		set control_code [lindex $sof_data 2]
#
#		if { $control_code == "" } {
#			puts "Warning: You have not set the schedule name in table app.sch_control_map"
#			return -1
#		}
#	} errmsg ] {
#			aidb_close $handle2
#			return -1
#	}
#	aidb_close $handle2
	if { [ string last "'" $alarmcontent ] != -1 } {
		set msg_list [ split $alarmcontent "'" ]
		set msg_list_length [ llength $msg_list ]
		for { set i 0 } { $i < $msg_list_length } { incr i } {
			set msg_list_content($i) [ lindex $msg_list $i ]
			append msg_content $msg_list_content($i)
		}
		set alarmcontent $msg_content
	}

	set control_code "BASS1_${program_name}"
	set handle [aidb_open $conn]
	set sql_buff "\
	      insert into app.sch_control_alarm (control_code,cmd_line,grade,content,alarmtime,flag) 
	      values 
	      (
	        '$control_code',
	        'int -s $program_name',
	        $grade,
	        '${alarmcontent}',
	        current timestamp,
	        -1
	      )"
	if [ catch { aidb_sql $handle $sql_buff } errmsg ] {
		puts $errmsg
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	

	
}
#################################################################
# 函数名称: MakeFexpScript
# 功能描述: f程序专用。teradata生成teradata的fastexport脚本
# 输入参数: script_filename 脚本文件名(含绝对路径,不含扩展名.ctl)
#           out_file		导出的数据文件名(含绝对路径)
#			db_name         数据库名
#			username        用户名
#			passwd          用户密码
#			column_list     要抽取的字段列表(以空格分隔)
#			from_str		sql语句的from子句(包括where,order by,join....所有from之后的语句）
#							格式为：from table_name ......
#			separator 		分隔符(缺省为tab键)
#			str_len			字段值转换为char型后的最大长度(缺省为255,可根据具体情况调整）
#
# 输出参数:
# 返回值:    0 生成脚本文件成功
#			-1 生成脚本文件失败
#################################################################

proc MakeFexpScript { script_file out_file db_name username passwd column_list from_str separator str_len } {
	set exp_fd [open $script_file.ctl w]
    puts $exp_fd ".logtable fastexp_log;"
    puts $exp_fd ".logon $db_name/$username,$passwd;"
    puts $exp_fd ".begin export;"
    puts $exp_fd ".export outfile $out_file Format text mode record;"
	#若未定义分隔符号，缺省为tab键
	if {$separator==""} {
		set separator "	"
	}
	#若未定义str_len，缺省为255
	if {$str_len==""} {
		set str_len 30
	}
	set select_str "SELECT"
	set list_len [llength $column_list]
	set i 1
	foreach col_str $column_list {
		if {$i < $list_len} {
			set col_str "cast($col_str as char($str_len)),cast('$separator' as char(1)),"
		} else {
			set col_str "cast($col_str as char($str_len))"
		}
		set select_str "$select_str $col_str"
		set i [expr $i + 1]
	}
	set sql_str "$select_str $from_str"
    puts $exp_fd "$sql_str;"
    puts $exp_fd ".end export;"
    puts $exp_fd ".logoff;"
    close $exp_fd
	if {[file exists $script_file] == 0 } {
		return 0
	} else {
		return -1
	}
}

#################################################################
# 函数名称: MoveAndVerify
# 功能描述: f程序专用函数。功能是将一经编码定长文件(.dat)复制到finaldata目录下，并且生成校验文件(.verf)
# 输入参数:
# 输出参数:
# 返回值:   0成功；-1失败
#################################################################

proc MoveAndVerify { temp_data_dir semi_data_dir final_data_dir obj_file src_file_name conn data_time } {
	global optime_month
	set op_month [string range $optime_month 0 3][string range $optime_month 5 6]

	set fnl_file_list ""
	set fnl_att_file_list ""
	set tmp_file_list [split [exec ls -t $temp_data_dir] "\n"]
	if [catch { set verf_fd [open $temp_data_dir$obj_file.verf w] } errmsg] {
		WriteTrace "Error: failed to open the file '$temp_data_dir$obj_file.verf'!" 2130
		return -1
	}
	#读tempdata目录下的dat和att文件
	foreach file_name_aft $tmp_file_list {
		if [ string match $obj_file*.att $file_name_aft ] {
			if [catch { set att_fd [open $temp_data_dir$file_name_aft r] } errmsg] {
				WriteTrace "Error: failed to open the file '$temp_data_dir$file_name_aft'!" 2131
				return -1
			}
			set verf_name ""
			set verf_value ""
			scan [gets $att_fd] {%s %s} verf_name verf_value
			while {![eof $att_fd]} {

				switch $verf_name {
					size	{
							if { $verf_value == "" || [string length $verf_value] > 20 || [string is integer $verf_value] != 1 } {
								WriteTrace "Error:  The value of file_size is $verf_value, its format is not correct! in file '$temp_data_dir$obj_file.att'!" 2141
								return -1
							} else {
								 set file_size $verf_value
							}
						}
					row	{
							if { $verf_value == "" || [string length $verf_value] > 20 || [string is integer $verf_value] != 1 } {
								WriteTrace "Error:  The value of row_num is $verf_value, its format is not correct! in file '$temp_data_dir$obj_file.att'!" 2142
								return -1
							} else {
								 set row_num $verf_value
							}
						}
					time	{
							if { $verf_value == "" || [string length $verf_value] != 14} {
								WriteTrace "Error:  The value of end_time is $verf_value, its format is not correct! in file '$temp_data_dir$obj_file.att'!" 2143
								return -1
							} else {
								 set end_time $verf_value
							}
						}
					default	{
							WriteTrace "Error:  The format is not correct! in file '$temp_data_dir$obj_file.att'!" 2144
							return -1
						}
				}
				set verf_name ""
				set verf_value ""
				scan [gets $att_fd] {%s %s} verf_name verf_value
			}

			close $att_fd

			if { [string range $obj_file 0 1] == "p_" } {
				if { [string length $obj_file] == 27 } {
					set data_date $data_time
				} elseif { [string length $obj_file] == 25 } {
					set data_date "$op_month  "
				} else {
					WriteTrace "Error: the final data file name '$obj_file' is not correct!" 2150
					return -1
				}
			} else {
				if { [string length $obj_file] == 25 } {
					set data_date $data_time
				} elseif { [string length $obj_file] == 23 } {
					set data_date "$op_month  "
				} else {
					WriteTrace "Error: the final data file name '$obj_file' is not correct!" 2151
					return -1
				}
			}

			set file_verify_result ""
			set file_verify_result $file_verify_result[format %-40s [string range $file_name_aft 0 end-4].dat]
			set file_verify_result $file_verify_result[format %-20s $file_size]
			set file_verify_result $file_verify_result[format %-20s $row_num]
			set file_verify_result $file_verify_result[format %-8s $data_date]
			set file_verify_result $file_verify_result[format %-14s $end_time]

			puts $verf_fd "$file_verify_result\r"
			flush $verf_fd
			lappend att_file_list $file_name_aft
		} elseif [ string match $obj_file* $file_name_aft ] {
			lappend fnl_file_list $file_name_aft
		}
	}
	close $verf_fd
	#检查数据文件.dat与.att数量是否一致
	if { [llength $fnl_file_list] != [llength $att_file_list] } {
		WriteTrace "Error: the number of data file and the number of att file are not equal!" 2160
		return -1
	}
	#将数据文件和校验文件move到finaldata目录下
	if [catch { file rename -force $temp_data_dir$obj_file.verf $final_data_dir$obj_file.verf } err_msg] {
		WriteTrace "Error: failed to move file!\n	from: $temp_data_dir$obj_file.verf\n	to: $final_data_dir$obj_file.verf" 2165
		return -1
	}
	foreach final_data_file_name $fnl_file_list {
		if [catch { file rename -force $temp_data_dir$final_data_file_name $final_data_dir$final_data_file_name.dat } errmsg] {
			WriteTrace "Error: failed to move file!\n	from: $temp_data_dir$final_data_file_name\n	to: $final_data_dir$final_data_file_name.dat" 2150
			return -1
		}
	}
	#文件名入bass1.int_send_file_list
	foreach final_data_file_name $fnl_file_list {
		set handle [aidb_open $conn]
		set sql_buff "\
			INSERT INTO bass1.int_send_file_list VALUES('$final_data_file_name.dat','$data_time','0')"
		if [catch { aidb_sql $handle $sql_buff } errmsg ] {
			WriteTrace $errmsg 2171
			aidb_close $handle
			return -1
		}
		aidb_close $handle
	}

	set handle [aidb_open $conn]
	set sql_buff "\
		INSERT INTO bass1.int_send_file_list VALUES('$obj_file.verf','$data_time','0')"
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace $errmsg 2172
		aidb_close $handle
		return -1
		}
	aidb_close $handle

	foreach att_file_name $att_file_list {
		if [catch { exec rm $temp_data_dir$att_file_name } errmsg] {
			WriteTrace "Warning: failed to delete file $att_file_name" 2152
		}
	}

	return 0
}


#################################################################
# 函数名称: exec_sql
# 功能描述: 执行sql
# 输入参数: MySQL : sql串
# 输出参数: 
# 返回值:   0成功；-1失败
#################################################################
proc exec_sql {MySQL} {

	global env

	global conn

	global handle

	set handle [aidb_open $conn]
	set sql_buff $MySQL
	puts $sql_buff
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

#################################################################
# 函数名称: get_single
# 功能描述: 返回一条查询结果
# 输入参数: MySQL : sql串
# 输出参数: 
# 返回值:   0成功；-1失败
#################################################################
proc get_single {MySQL} {

	global env

	global conn

	global handle

	set handle [aidb_open $conn]
	set sql_buff $MySQL
	puts $sql_buff
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
	
	puts $result
	return $result
}


#################################################################
# 函数名称: chkpkunique
# 功能描述: 返回一条查询结果
# 输入参数: tabname pk
# 输出参数: 
# 返回值:   0成功；-1失败
#################################################################
proc chkpkunique {in_tabname in_pk in_timestamp} {
	global env

	global conn

	global handle	
	
	set tabname $in_tabname
	set pk $in_pk	
	set timestamp $in_timestamp	
	set sql_buf " select count(0) dup_cnt
	from (
		select count(0)  cnt
		from bass1.$tabname
		where time_id =$timestamp
		group by $pk having count(0) > 1
		) t 
		"
	set CHECK_VAL1 [get_single $sql_buf ]
	puts $CHECK_VAL1
	set CHECK_VAL1 [format "%.3f" [expr ${CHECK_VAL1} /1.00]]
	puts $CHECK_VAL1
	
	if {[format %.3f [expr ${CHECK_VAL1} ]]>0 } {
		set grade 2
		set app_name "BASS1_$in_tabname.tcl"
	        set alarmcontent "PK-CHECK not passed!"
	        WriteAlarm $app_name $timestamp $grade ${alarmcontent}
    return -1	        
	}
		puts "PK-CHECK check OK!"
	return 0
}



#################################################################
# 函数名称: aidb_runstats
# 功能描述: 返回一条查询结果
# 输入参数: tabname 
# 输出参数: 
# 返回值:   0成功；-1失败
#################################################################
proc aidb_runstats {tablename  flag} {

switch $flag {
            1 {set  sqlbuf "runstats on table $tablename on all columns and indexes all " }
            2 {set  sqlbuf "runstats on table $tablename  on key columns and indexes all " }
            3 {set  sqlbuf "runstats on table $tablename with distribution and detailed indexes all " }
            default {
               set  sqlbuf "runstats on table $tablename on all columns and indexes all " 
             }
 }

exec db2 connect to bassdb user bass1 using bass1  > /dev/null
puts "$sqlbuf"
exec db2 $sqlbuf
exec db2 terminate > /dev/null
}


#################################################################
# 函数名称: chkzero
# 功能描述: 非零值告警
# 输入参数: MySQL : sql串
# 输出参数: 
# 返回值:   0成功；-1失败
#################################################################
proc chkzero {MySQL ind} {
	
	global app_name
	global op_time

	puts "SQL-STATEMENT NON-ZERO CHECK:"	
	
  set DEC_RESULT_VAL1 [get_single $MySQL]
	set DEC_RESULT_VAL1 [format "%.3f" [expr ${DEC_RESULT_VAL1} /1.00]]
	puts $DEC_RESULT_VAL1

	if {[format %.3f [expr ${DEC_RESULT_VAL1} ]] != 0 } {
		set grade 2
	        set alarmcontent "exception:$ind:$DEC_RESULT_VAL1"
	        WriteAlarm $app_name $op_time $grade ${alarmcontent}
	   			return -1
	} else {
	 	puts "chkzero pass!"	
	 	return 0
	}

}



#################################################################
# 函数名称: chkzero2
# 功能描述: 非零值告警
# 输入参数: MySQL : sql串
# 输出参数: 
# 返回值:   0成功；-1失败
#################################################################
proc chkzero2 {MySQL msg} {
	
	global app_name
	global op_time

	puts "SQL-STATEMENT NON-ZERO CHECK:"	
	
  set DEC_RESULT_VAL1 [get_single $MySQL]
	set DEC_RESULT_VAL1 [format "%.3f" [expr ${DEC_RESULT_VAL1} /1.00]]
	puts $DEC_RESULT_VAL1

	if {[format %.3f [expr ${DEC_RESULT_VAL1} ]] != 0 } {
		set grade 2
	        set alarmcontent "exception:$msg:$DEC_RESULT_VAL1"
	        WriteAlarm $app_name $op_time $grade ${alarmcontent}
	   			return -1
	} else {
	 	puts "chkzero pass!"	
	 	return 0
	}

}



#################################################################
# 函数名称: lezeroalarm
# 功能描述: 小于零值告警
# 输入参数: MySQL : sql串
# 输出参数: 
# 返回值:   0成功；-1失败
#################################################################
proc lezeroalarm {MySQL msg} {
	
	global app_name
	global op_time

	puts "SQL-STATEMENT NON-ZERO CHECK:"	
	
  set DEC_RESULT_VAL1 [get_single $MySQL]
	set DEC_RESULT_VAL1 [format "%.3f" [expr ${DEC_RESULT_VAL1} /1.00]]
	puts $DEC_RESULT_VAL1

	if {[format %.3f [expr ${DEC_RESULT_VAL1} ]] < 0 } {
		set grade 2
	        set alarmcontent "exception:$msg:$DEC_RESULT_VAL1"
	        WriteAlarm $app_name $op_time $grade ${alarmcontent}
	   			return -1
	} else {
	 	puts "chkzero pass!"	
	 	return 0
	}

}

#################################################################
# 函数名称: get_row
# 功能描述: 获得一行
# 输入参数: MySQL : sql串
# 输出参数: 
# 返回值:   0成功；-1失败
#################################################################
proc get_row {MySQL} {

        global env

        global conn

        global handle

        set handle [aidb_open $conn]
        set sql_buff $MySQL
        puts $sql_buff
        puts "----------------------------------------------------------------------------------- "
        if [catch { aidb_sql $handle $sql_buff } errmsg ] {
                WriteTrace"$errmsg" 2005
                aidb_close $handle
                puts $errmsg
                exit -1
        }
        set p_row [aidb_fetch $handle]
        aidb_commit $conn
        aidb_close $handle
        return $p_row
}


#################################################################
# 函数名称: get_rows
# 功能描述: 获得n行
# 输入参数: MySQL : sql串
# 输出参数: 
# 返回值:   0成功；-1失败
#################################################################
proc get_rows {MySQL n} {

        global env

        global conn

        global handle

        set handle [aidb_open $conn]
        set i 0
        set sql_buff $MySQL
        if [catch { aidb_sql $handle $sql_buff } errmsg ] {
                WriteTrace"$errmsg" 2005
                aidb_close $handle
                puts $errmsg
                exit -1
        }
        while {[set p_row [aidb_fetch $handle ]] != "" } {
        set res($i) $p_row
        incr i
    		}
        #set p_row [aidb_fetch $handle]
        aidb_commit $conn
        aidb_close $handle
        
        return $res($n)
}


#################################################################
# 函数名称: createtb
# 功能描述: 建表
# 输入参数: table_template part_key space_tb space_ind data_time -> 模板表 分区键盘(格式："a,b,c") 表空间 索引空间 数据日期
# 输出参数: 无
# 返回值:   0成功；-1失败
#example: createtb INT_02004_02008_YYYYMM "OP_TIME,USER_ID" TBS_APP_BASS1 TBS_INDEX 201108
#################################################################
proc createtb {table_template part_key space_tb space_ind data_time} {
	#获得带数据日期的表$real_tabname
	if { [ string length $data_time ] == 8  } {
		set len_str [ string length $table_template ]
		set str_range [ expr $len_str - 8 -1 ]
		set tabname_nodt [ string range $table_template 0 $str_range ]
		set real_tabname [ string range $table_template 0 $str_range ]$data_time
	} elseif { [ string length $data_time ] == 6  } { 
	
			set len_str [ string length $table_template ]
			set str_range [ expr $len_str - 6 -1 ]
			set tabname_nodt [ string range $table_template 0 $str_range ]
			set real_tabname [ string range $table_template 0 $str_range ]$data_time
		} else {
			puts "invalid data_time!"
		}
        #判断表是否已创建
	set sql_buff "
		select count(0) from syscat.tables where tabname = '$real_tabname'
	"
	set RESULT_VAL [get_single $sql_buff]
	#create
	if { $RESULT_VAL <= 0 } {
		#the create statement
		set sql_buff "
		CREATE TABLE ${real_tabname} LIKE ${table_template} 
		DISTRIBUTE BY HASH(${part_key})  IN ${space_tb} INDEX IN ${space_ind} 
		"
		exec_sql $sql_buff
		#runstat
		aidb_runstats bass1.${real_tabname}  3
		
		puts "send msg..."
		set sqlbuf "insert into APP.SMS_SEND_INFO(MESSAGE_CONTENT,MOBILE_NUM) 
				select '${real_tabname} created!',phone_id 
				from BASS2.ETL_SEND_MESSAGE where MODULE='BASS1'"
		exec_sql $sqlbuf
		puts "send complete!"
		return 0
	} else {
		puts "send msg..."
		set sqlbuf "insert into APP.SMS_SEND_INFO(MESSAGE_CONTENT,MOBILE_NUM) 
				select '${real_tabname} already exists!',phone_id 
				from BASS2.ETL_SEND_MESSAGE where MODULE='BASS1'"
		exec_sql $sqlbuf
		puts "send complete!"
		return 1
	}

}



#################################################################
# 函数名称: ChnRatio
# 功能描述: 指标环比异常告警
# 输入参数: MySQL : sql串1 sql串2
# 输出参数: 
# 返回值:   0成功；-1失败
#################################################################
proc ChnRatio {MySQL1 MySQL2} {
	
	global app_name
	global op_time

	puts "SQL-STATEMENT  ChnRatio:"	
	
	
  set DEC_RESULT_VAL1 [get_single $MySQL1]
  set DEC_RESULT_VAL2 [get_single $MySQL2]
	set DEC_RESULT_VAL1 [format "%.3f" [expr ${DEC_RESULT_VAL1} /1.00]]
	set DEC_RESULT_VAL2 [format "%.3f" [expr ${DEC_RESULT_VAL2} /1.00]]
	
	puts $DEC_RESULT_VAL1
	puts $DEC_RESULT_VAL2
	
	set DEC_RESULT_VAL3 [format %.3f [expr abs(${DEC_RESULT_VAL1} / ${DEC_RESULT_VAL2} - 1) ]]
	puts [format %.3f [expr (${DEC_RESULT_VAL1} / ${DEC_RESULT_VAL2} - 1) ]]
	puts $DEC_RESULT_VAL3
	if {  ${DEC_RESULT_VAL3} > 0.30 } {
		set grade 2
	        set alarmcontent "exception:指标环比异常"
	        WriteAlarm $app_name $op_time $grade ${alarmcontent}
	   			return -1
	} else {
	 	puts "ChnRatio pass!"	
	 	return 0
	}

}

################self defined functions end#################################################


#################################################################
#	主程序部分
#################################################################

#解析命令行参数
if {$argc < 2} {
	puts "Usage : int -s \[program_name\] -u \[ddh\] -v \[rwh\]"
	#输出失败给调度程序
	puts "FAILED"
	exit -1
}

set arg(-s)  ""
set arg(-u)  ""
set arg(-v)  ""


for { set i 0 } {$i < [expr $argc - 1 ]} {incr i} {
	set flag [lindex $argv $i]
	set value [lindex $argv [expr $i + 1] ]
	if { [string index $flag 0] == "-" } {
		set arg($flag) $value
	}
}

set program_name $arg(-s)
set ddh $arg(-u)
set rwh $arg(-v)

set script [string range $program_name 0 [expr [string length $program_name] - 5]]

#检查$program_name是否存在
if {! [file exists $env(HOME)/tcl/${program_name}]} {
	puts "Error: can not find the program $program_name"
	#输出失败给调度程序
	puts "FAILED"
	exit -1
} else {
	source $env(HOME)/tcl/${program_name}
	#source "$program_name"
}

# 初始化
set format "+\"%Y-%m-%d %H:%M:%S\""
set start_time [string range [exec date $format] 1 19]
set redo_number "00"

if {! [info exists env(DATABASE)]} {
	puts "Error: can not get env variable DATABASE"
	#输出失败给调度程序
	puts "FAILED"
	exit -1
}

if {! [info exists env(AITOOLSPATH)]} {
	puts "Error: can not get env variable AITOOLSPATH"
	#输出失败给调度程序
	puts "FAILED"
	exit -1
} else {
	set aidbtclpath $env(AITOOLSPATH)
}
if { $env(DATABASE) == "DB2" } {
	source "$aidbtclpath/bin/aidb_db2.tcl"
	load "$aidbtclpath/lib/libdb2tcl.so"
} elseif { $env(DATABASE) == "oracle" } {
	load "$aidbtclpath//lib//libOratcl2.5.so"
	source "$aidbtclpath//bin//aidb_ora.tcl"
} elseif { $env(DATABASE) == "TDB" } {
	load "$aidbtclpath//lib//libtdb.so"
	source "$aidbtclpath//bin//aidb_tdb.tcl"
} else {
	puts "we only support database teradata db2 and oracle now !\n\t请检查环境变量 DATABASE!"
	#输出失败给调度程序
	puts "FAILED"
	exit -1
}


#取环境变量BASS1FILEDIR，即数据文件的存放路径
if {! [info exists env(BASS1FILEDIR)]} {
	puts "Error: can not get env variable BASS1FILEDIR"
	#输出失败给调度程序
	puts "FAILED"
	exit -1
}
set data_dir $env(BASS1FILEDIR)
set len [string length $data_dir]
if { [string index $data_dir [expr $len - 1]] != "/" } {
	set data_dir "$data_dir/"
}
if {[file exists $data_dir] == 0 } {
	file mkdir $data_dir
}
#临时数据存放路径
set tempdata "tempdata/"
set temp_data_dir "$data_dir$tempdata"
if {[file exists $temp_data_dir] == 0 } {
	file mkdir $temp_data_dir
}
#中间数据存放路径
set semidata "semidata/"
set semi_data_dir "$data_dir$semidata"
if {[file exists $semi_data_dir] == 0 } {
	file mkdir $semi_data_dir
}
#最终数据存放路径
set finaldata "finaldata/"
set final_data_dir "$data_dir$finaldata"
if {[file exists $final_data_dir] == 0 } {
	file mkdir $final_data_dir
}

#取环境变量BASS1DIR，即一经接口程序的安装路径
if {! [info exists env(BASS1DIR)]} {
	puts "Error: can not get env variable BASS1DIR"
	#输出失败给调度程序
	puts "FAILED"
	exit -1
}
set bass1_dir $env(BASS1DIR)
if { [string index $bass1_dir [expr [string length $bass1_dir] - 1]] != "/" } {
	set bass1_dir "$bass1_dir/"
}
if { ![file exists $bass1_dir] } {
	file mkdir $bass1_dir
}

#打开trace文件
#puts "begin to openTrace"

set trace_fd [OpenTrace $script]
if {$trace_fd == "-1"} {
	puts "\nError: Open trace file failed! The program has terminated!\n"
	set success_code 1
	GenSchServerLog
	#输出失败给调度程序
	puts "FAILED"
	exit -1
}
puts $trace_fd "\n------------------------------------------\nStart: The program $program_name started at $start_time\n"

##读配置文件int_connect_str.cfg，获得两个连接串
#if {! [file exists $bass1_dir/tcl/int_connect_str.cfg]} {
#	WriteTrace "Error:can not find file int_connect_str.cfg in directory $bass1_dir/tcl/!" 5000
#	#输出失败给调度程序
#	puts "FAILED"
#	exit -1
#} else {
#	source "$bass1_dir/tcl/int_connect_str.cfg"
#}
#if {! [info exists dw_conn_str]} {
#	WriteTrace "Error:variable dw_conn_str in $bass1_dir/tcl/int_connect_str.cfg not exist!" 5001
#	#输出失败给调度程序
#	puts "FAILED"
#	exit -1
#}

set pwd_str "0312004232"
set encode_pwd_str [exec /bassapp/backapp/src/C_FUNCTION/decode ${pwd_str}]
set dw_conn_str "bass1/${encode_pwd_str}@bassdb"

set pos1 [string first "/" $dw_conn_str]
set pos2 [string first "@" $dw_conn_str]
set username [string range $dw_conn_str 0 [expr $pos1 - 1] ]
set passwd [string range $dw_conn_str [expr $pos1 + 1] [expr $pos2 -1 ]]
set db [string range $dw_conn_str [expr $pos2 + 1] end ]

#连接数据库
if [catch {set conn [aidb_connect $db $username $passwd] } errmsg ] {
	WriteTrace "Error:conn failed! The error message is:\n\"$errmsg\"\n" 5003
	set success_code 2
	GenSchServerLog
	#输出失败给调度程序
	puts "FAILED"
	exit -1
}
puts $trace_fd "Connected to database : $db ."

if { [info exists control_db_conn_str]} {
	set pos1 [string first "/" $control_db_conn_str]
	set pos2 [string first "@" $control_db_conn_str]
	set username_ctl [string range $control_db_conn_str 0 [expr $pos1 - 1] ]
	set passwd_ctl [string range $control_db_conn_str [expr $pos1 + 1] [expr $pos2 -1 ]]
	set db_ctl [string range $control_db_conn_str [expr $pos2 + 1] end ]
	#连接另外一个数据库，用于存放控制表的。
	if [catch {set conn_ctl [aidb_connect $db_ctl $username_ctl $passwd_ctl] } errmsg ] {
		WriteTrace "Error:conn failed! The error message is:\n\"$errmsg\"\n" 5004
		set success_code 2
		GenSchServerLog
		#输出失败给调度程序
		puts "FAILED"
		exit -1
	}
	puts $trace_fd "Connected to database : $db_ctl ."
#added by shensp
} else {
	set conn_ctl $conn
}

#初始化以下参数
#	province_id	省份编码，格式为两位数字字符
#	op_time		数据时间（批次）,“yyyy-mm-dd”
#	data_time	含义同op_time，格式为“yyyymmdd”
#	is_normal_proc	是否正常处理（本次处理是否正常处理）。"0"表示正常处理，这种情况下，全部数据处理程序都将会运行一遍，生成最终数据文件；"1"表示异常处理（重传），这种情况说明校验报告分析程序发现上一周期的数据发生错误，人工调整数据源之后，本次程序运行将重新产生之前出错的数据文件，文件名里的重传序号加一。每次程序执行之前，需要确认该标志配置正确。
#	src_data	数据源，数据库名或文件名，某些程序是没有源
#	obj_data	数据目标，数据名或文件名，某些程序是没有目的
if {[Initialize] != 0} {
	WriteTrace "Error: Initialization failed!!\n" 0102
	set success_code 3
	GenSchServerLog
	#输出失败给调度程序
	puts "FAILED"
	exit -1
}

#决定是否继续运行程序
set decision [Decide]
if {$decision == 1} {
	WriteTrace "THE PROGRAM SHOULD NOT RUN!!" 0103
	set success_code 0
	GenSchServerLog
	#输出成功给调度程序
	puts "SUCCESSFUL"
	exit 0
}
if {$decision == -1} {
	WriteTrace "DATABASE QUERY FAILED!!" 0103
	set success_code 4
	GenSchServerLog
	#输出失败给调度程序
	puts "FAILED"
	exit -1
}

#####读日志文件，确定重传序号
####if {[ReadLog] != 0} {
####	WriteTrace "READ LOG FILE FAILED!!" 0104
####	set success_code 4
####	GenSchServerLog
####	#输出失败给调度程序
####	puts "FAILED"
####	exit -1
####}

#运行核心的deal过程
if {[Deal $op_time $optime_month $province_id $redo_number $trace_fd $bass1_dir $temp_data_dir $semi_data_dir $final_data_dir $conn $conn_ctl $src_data $obj_data $final_data] != 0 } {
	#WriteTrace "EXECUTE FAILED!" 0103
	catch {aidb_disconnect $conn} errmsg

	set success_code 4
	set end_time [string range [exec date $format] 1 19]
	if { [GenLog] != 0 } {
		WriteTrace "ERROR: Can not write the log file!" 0104
	}
	GenSchServerLog
	#输出失败给调度程序
	puts "FAILED"
	exit -1
}

#WriteTrace "EXECUTE SUCCESSFULLY!" 0105
set success_code 0
set end_time [string range [exec date $format] 1 19]
if { [GenLog] != 0 } {
	WriteTrace "ERROR: Can not write the log file!" 0104
	set success_code 4
	GenSchServerLog
	#输出失败给调度程序
	puts "FAILED"
	exit -1
}
GenSchServerLog
#输出成功给调度程序
puts "SUCCESSFUL"
exit 0
