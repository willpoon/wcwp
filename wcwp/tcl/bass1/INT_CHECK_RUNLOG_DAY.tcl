######################################################################################################
#程序名称：INT_CHECK_RUNLOG_DAY.tcl
#校验接口：每天7点统计生成的接口文件数量
#运行粒度: 日
#输入参数: 
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：liuzhilong
#编写时间：2009-09-14
#问题记录：
#修改历史: 2010.11.01 废除06030 9点的日接口，同时废除22053，总共日接口55个，9点的接口8个
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

        
        #当天 yyyymmdd
        set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
        #当天 yyyy-mm-dd
        set optime $op_time
        #前一天 yyyymmdd
        set last_day [GetLastDay [string range $timestamp 0 7]]
        #程序名
        set app_name "INT_CHECK_RUNLOG_DAY.tcl"

    puts " 删除旧数据"
 	  set sqlbuf "delete from  BASS1.G_RULE_CHECK where time_id=$timestamp and rule_code in ('RUNLOG') "        
	  exec_sql $sqlbuf     

   
   puts "每天7点统计生成的接口文件数量 val3(9点钟接口的数量)"
   set sqlbuf " 
						select count(distinct a.control_code) val1
						      ,value(count(distinct b.control_code),0) val2
						      ,value(count(distinct case  
						      									when substr(b.control_code,15,5) in ('01002','01004','02004','02008','02011','02053','06031','06032') 
						      									then b.control_code 
						      								 end  ),0) val3
						from app.sch_control_task a
						left join ( select control_code 
						            from app.sch_control_runlog 
						            where date(endtime)=date(current date)
						                  and flag=0 ) b on a.control_code=b.control_code
						where a.control_code like 'BASS1_EXP%DAY%' 
						      and a.cc_flag=1
    "
   set p_row [get_row $sqlbuf]
   set RESULT_VAL1 [lindex $p_row 0]
	 set RESULT_VAL2 [lindex $p_row 1]
	 set RESULT_VAL3 [lindex $p_row 2]  

	puts " 将校验值插入校验表里 "   
	set sqlbuf "INSERT INTO BASS1.G_RULE_CHECK VALUES ($timestamp,'RUNLOG',$RESULT_VAL1,$RESULT_VAL2,$RESULT_VAL3,0) "        
	exec_sql $sqlbuf  
	puts "将校验值插入校验表里成功"
  
  puts "插入到短信信息表里头"
	set sqlbuf "insert into APP.SMS_SEND_INFO(MESSAGE_CONTENT,MOBILE_NUM) 
							select '数据日期${timestamp}报告------一经接口总数:${RESULT_VAL1}个,生成9点接口数:${RESULT_VAL3}个,今日总共生成的接口数:${RESULT_VAL2}个。',
							phone_id 
							from BASS2.ETL_SEND_MESSAGE where MODULE='BASS1'"
	exec_sql $sqlbuf
  puts "插入到短信信息表里头成功"

	return 0
}




#------------------------内部函数部分--------------------------#	
#  get_row 返回 SQL的行徝
proc get_row {MySQL} {

	global env

	global conn

	global handle

	set handle [aidb_open $conn]
	set sql_buff $MySQL
	puts $sql_buff
	puts "----------------------------------------------------------------------------------- "
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		puts $errmsg
		exit -1
	}
	set p_row [aidb_fetch $handle]
	aidb_commit $conn
	aidb_close $handle
	return $p_row
}

#   exec_sql 执行SQL
proc exec_sql {MySQL} {

	global env

	global conn

	global handle

	set handle [aidb_open $conn]
	set sql_buff $MySQL
	puts $sql_buff
	puts "----------------------------------------------------------------------------------- "
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

