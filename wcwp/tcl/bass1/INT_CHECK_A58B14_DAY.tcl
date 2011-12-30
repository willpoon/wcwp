#################################################################
#程序名称: INT_CHECK_A58B14_DAY.tcl
#功能描述:
#规则编号：A5,A8,B1,B4
#规则属性：交叉验证
#规则类型：日
#指标摘要：A5：智能网VPMN日时段计费时长/智能网VPMN业务使用计费时长
#          A8：智能网VPMN日时段通话次数/智能网VPMN业务使用通话次数
#          B1：智能网VPMN日时段通话时长/智能网VPMN业务使用通话时长
#          B4: 智能网VPMN日时段总费用/智能网VPMN业务使用总费用
#规则描述：A5：|智能网VPMN日时段计费时长/智能网VPMN业务使用计费时长 - 1| ≤ 0.1%
#          A8：|智能网VPMN日时段通话次数/智能网VPMN业务使用通话次数 - 1| ≤ 0.1%
#          B1：|智能网VPMN日时段通话时长/智能网VPMN业务使用通话时长 - 1| ≤ 0.1%
#          B4: |智能网VPMN日时段总费用/智能网VPMN业务使用总费用 - 1| ≤ 0.1%
#校验对象：
#          BASS1.G_S_21009_DAY     智能网VPMN业务日使用
#          BASS1.G_S_21016_DAY     智能网VPMN业务日时段使用
#输出参数:
# 返回值:   0 成功; -1 失败
#################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

        #当天 yyyymmdd
        set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
        #当天 yyyy-mm-dd
        set optime $op_time
        #程序名称
        set app_name "INT_CHECK_A58B14_DAY.tcl"


#删除本期数据
        set handle [aidb_open $conn]
	set sql_buff "
		delete from bass1.g_rule_check where time_id=$timestamp
    	              and rule_code in ('A5','A8','B1','B4') "
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
##################################A5：智能网VPMN日时段计费时长/智能网VPMN业务使用计费时长#######
	#智能网VPMN日时段计费时长
	set handle [aidb_open $conn]
	set sqlbuf "
            	SELECT 
            	   SUM(BIGINT(BASE_BILL_DURATION))
                FROM bass1.G_S_21009_DAY
                where time_id=$timestamp"

	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
		WriteTrace $errmsg 001
		return -1
	}
	while { [set p_row [aidb_fetch $handle]] != "" } {
		set CHECK_VAL1 [lindex $p_row 0]
	}
	aidb_commit $conn
	aidb_close $handle
	
	set RESULT_VAL1 [format "%.2f" [expr ${CHECK_VAL1} /1.00]]	
     
        #智能网VPMN业务使用计费时长
	set handle [aidb_open $conn]
	set sqlbuf "
            	SELECT 
            	   SUM(BIGINT(BASE_BILL_DURATION))
                FROM bass1.G_S_21016_DAY
                where time_id=$timestamp"

	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
		WriteTrace $errmsg 001
		return -1
	}
	while { [set p_row [aidb_fetch $handle]] != "" } {
		set CHECK_VAL2 [lindex $p_row 0]
	}
	aidb_commit $conn
	aidb_close $handle
	
	set RESULT_VAL2 [format "%.2f" [expr ${CHECK_VAL2} /1.00]]

    #将校验值插入校验结果表
	set handle [aidb_open $conn]
	set sqlbuf "INSERT INTO bass1.G_RULE_CHECK VALUES
			($timestamp ,
			'A5',
			cast ($RESULT_VAL1 as  DECIMAL(18, 5) ),
			cast ($RESULT_VAL2 as  DECIMAL(18, 5) ),
			0,
			0)"
	if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
		WriteTrace $errmsg 003
		return -1
	}
	aidb_commit $conn
	aidb_close $handle

	#判断A5：|智能网VPMN日时段计费时长/智能网VPMN业务使用计费时长 - 1| ≤ 0.1%超标
	if { $RESULT_VAL1 ==0 ||$RESULT_VAL2 ==0 ||$RESULT_VAL1 != $RESULT_VAL2 } {
		set grade 2
	        set alarmcontent "准确性A5超出集团考核范围"
	        WriteAlarm $app_name $optime $grade ${alarmcontent}	
            } 
            puts "A5结束"       
##################################A8：智能网VPMN日时段通话次数/智能网VPMN业务使用通话次数#######
	#智能网VPMN日时段通话次数
	set handle [aidb_open $conn]
	set sqlbuf "
            	SELECT 
            	   SUM(BIGINT(CALL_COUNTS))
                FROM bass1.G_S_21009_DAY
                where time_id=$timestamp"

	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
		WriteTrace $errmsg 001
		return -1
	}
	while { [set p_row [aidb_fetch $handle]] != "" } {
		set CHECK_VAL1 [lindex $p_row 0]
	}
	aidb_commit $conn
	aidb_close $handle
	
	set RESULT_VAL1 [format "%.2f" [expr ${CHECK_VAL1} /1.00]]	
     
        #智能网VPMN业务使用通话次数
	set handle [aidb_open $conn]
	set sqlbuf "
            	SELECT 
            	   SUM(BIGINT(CALL_COUNTS))
                FROM bass1.G_S_21016_DAY
                where time_id=$timestamp"

	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
		WriteTrace $errmsg 001
		return -1
	}
	while { [set p_row [aidb_fetch $handle]] != "" } {
		set CHECK_VAL2 [lindex $p_row 0]
	}
	aidb_commit $conn
	aidb_close $handle
	
	set RESULT_VAL2 [format "%.2f" [expr ${CHECK_VAL2} /1.00]]

    #将校验值插入校验结果表
	set handle [aidb_open $conn]
	set sqlbuf "INSERT INTO bass1.G_RULE_CHECK VALUES
			($timestamp ,
			'A8',
			cast ($RESULT_VAL1 as  DECIMAL(18, 5) ),
			cast ($RESULT_VAL2 as  DECIMAL(18, 5) ),
			0,
			0)"
	if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
		WriteTrace $errmsg 003
		return -1
	}
	aidb_commit $conn
	aidb_close $handle

	#判断A8：|智能网VPMN日时段通话次数/智能网VPMN业务使用通话次数 - 1| ≤ 0.1%超标
	if { $RESULT_VAL1 ==0 ||$RESULT_VAL2 ==0 ||$RESULT_VAL1 != $RESULT_VAL2 } {	
		set grade 2
	        set alarmcontent "准确性A8超出集团考核范围"
	        WriteAlarm $app_name $optime $grade ${alarmcontent}	
		aidb_close $handle
            } 
            puts "A8结束"             
##################################B1：智能网VPMN日时段通话时长/智能网VPMN业务使用通话时长#######
	#智能网VPMN日时段通话时长
	set handle [aidb_open $conn]
	set sqlbuf "
            	SELECT 
            	   SUM(BIGINT(CALL_DURATION))
                FROM bass1.G_S_21009_DAY
                where time_id=$timestamp"

	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
		WriteTrace $errmsg 001
		return -1
	}
	while { [set p_row [aidb_fetch $handle]] != "" } {
		set CHECK_VAL1 [lindex $p_row 0]
	}
	aidb_commit $conn
	aidb_close $handle
	
	set RESULT_VAL1 [format "%.2f" [expr ${CHECK_VAL1} /1.00]]	
     
        #智能网VPMN业务使用通话时长
	set handle [aidb_open $conn]
	set sqlbuf "
            	SELECT 
            	   SUM(BIGINT(CALL_DURATION))
                FROM bass1.G_S_21016_DAY
                where time_id=$timestamp"

	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
		WriteTrace $errmsg 001
		return -1
	}
	while { [set p_row [aidb_fetch $handle]] != "" } {
		set CHECK_VAL2 [lindex $p_row 0]
	}
	aidb_commit $conn
	aidb_close $handle
	
	set RESULT_VAL2 [format "%.2f" [expr ${CHECK_VAL2} /1.00]]

    #将校验值插入校验结果表
	set handle [aidb_open $conn]
	set sqlbuf "INSERT INTO bass1.G_RULE_CHECK VALUES
			($timestamp ,
			'B1',
			cast ($RESULT_VAL1 as  DECIMAL(18, 5) ),
			cast ($RESULT_VAL2 as  DECIMAL(18, 5) ),
			0,
			0)"
	if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
		WriteTrace $errmsg 003
		return -1
	}
	aidb_commit $conn
	aidb_close $handle

	#判断B1：|智能网VPMN日时段通话时长/智能网VPMN业务使用通话时长 - 1| ≤ 0.1%超标
	if { $RESULT_VAL1 ==0 ||$RESULT_VAL2 ==0 ||$RESULT_VAL1 != $RESULT_VAL2 } {	
		set grade 2
	        set alarmcontent "准确性B1超出集团考核范围"
	        WriteAlarm $app_name $optime $grade ${alarmcontent}	
            } 
          puts "B1结束"           
##################################B4: 智能网VPMN日时段总费用/智能网VPMN业务使用总费用#######
	#智能网VPMN日时段总费用
	set handle [aidb_open $conn]
	set sqlbuf "
            	SELECT 
            	   SUM(BIGINT(FAVOURED_CALL_FEE))
                FROM bass1.G_S_21009_DAY
                where time_id=$timestamp"

	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
		WriteTrace $errmsg 001
		return -1
	}
	while { [set p_row [aidb_fetch $handle]] != "" } {
		set CHECK_VAL1 [lindex $p_row 0]
	}
	aidb_commit $conn
	aidb_close $handle
	
	set RESULT_VAL1 [format "%.2f" [expr ${CHECK_VAL1} /1.00]]	
     
        #智能网VPMN业务使用总费用
	set handle [aidb_open $conn]
	set sqlbuf "
            	SELECT 
            	   SUM(BIGINT(FAVOURED_CALL_FEE))
                FROM bass1.G_S_21016_DAY
                where time_id=$timestamp"

	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
		WriteTrace $errmsg 001
		return -1
	}
	while { [set p_row [aidb_fetch $handle]] != "" } {
		set CHECK_VAL2 [lindex $p_row 0]
	}
	aidb_commit $conn
	aidb_close $handle
	
	set RESULT_VAL2 [format "%.2f" [expr ${CHECK_VAL2} /1.00]]

    #将校验值插入校验结果表
	set handle [aidb_open $conn]
	set sqlbuf "INSERT INTO bass1.G_RULE_CHECK VALUES
			($timestamp ,
			'B4',
			cast ($RESULT_VAL1 as  DECIMAL(18, 5) ),
			cast ($RESULT_VAL2 as  DECIMAL(18, 5) ),
			0,
			0)"
	if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
		WriteTrace $errmsg 003
		return -1
	}
	aidb_commit $conn
	aidb_close $handle

	#判断 B4: |智能网VPMN日时段总费用/智能网VPMN业务使用总费用 - 1| ≤ 0.1%超标
	if { $RESULT_VAL1 ==0 ||$RESULT_VAL2 ==0 ||$RESULT_VAL1 != $RESULT_VAL2 } {	
		set grade 2
	        set alarmcontent "准确性B4超出集团考核范围"
	        WriteAlarm $app_name $optime $grade ${alarmcontent}	
            } 
            puts "B4结束"                     
##################################END#######################
	return 0
}