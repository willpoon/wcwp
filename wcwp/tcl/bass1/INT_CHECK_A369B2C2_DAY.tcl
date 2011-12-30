#################################################################
#程序名称: INT_CHECK_A369B2C2_DAY.tcl
#功能描述:
#输入参数:
#	    op_time		数据时间（批次）,“yyyy-mm-dd”
#	    province_id	        省份编码
#	    redo_number	        重传序号。只有最终的一经编码定长数据文件需要重传序号
#	    trace_fd	        trace文件句柄
#	    temp_data_dir	临时数据文件的存放位置，$BASS1FILEDIR/tempdata/，用于保存e_transform生成的数据文件
#	    semi_data_dir	中间数据文件的存放位置，$BASS1FILEDIR/semidata/，用于保存.bass1、.bass2、.sample等文件
#	    final_data_dir	最终数据文件的存放位置，$BASS1FILEDIR/finaldata/，用于保存.dat，即一经编码定长数据文件
#	    conn		数据库连接
#	    src_data	        数据源
#	    obj_data	        数据目的
#
#规则编号：A3,A6,A9,B2,C2
#规则属性：交叉验证
#规则类型：日
#指标摘要：A3：GSM日时段计费时长/GSM日汇总计费时长
#          A6：GSM日时段通话次数/GSM日汇总通话次数
#          A9：GSM日时段通话时长/GSM日汇总通话时长
#          B2: GSM日时段总费用/GSM日汇总总费用
#          C2: 全球通日时段计费时长/全球通日汇总计费时长
#规则描述：A3：|GSM日时段计费时长/GSM日汇总计费时长 - 1| ≤ 0.1%
#          A6：|GSM日时段通话次数/GSM日汇总通话次数 - 1| ≤ 0.1%
#          A9：|GSM日时段通话时长/GSM日汇总通话时长 - 1| ≤ 0.1%
#          B2: |GSM日时段总费用/GSM日汇总总费用 - 1| ≤ 0.1%
#          C2: |全球通日时段计费时长/全球通日汇总计费时长 - 1| ≤ 0.1%
#校验对象：
#          BASS1.G_S_21001_DAY     GSM普通语音业务日使用
#          BASS1.G_S_21002_DAY     GSM普通语音业务日时段使用
#输出参数:
# 返回值:   0 成功; -1 失败
#################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

        #当天 yyyymmdd
        set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
        #当天 yyyy-mm-dd
        set optime $op_time
        #程序名称
        set app_name "INT_CHECK_A369B2C2_DAY.tcl"


        #删除本期数据
        set handle [aidb_open $conn]
	set sql_buff "\
		delete from bass1.g_rule_check where time_id=$timestamp
    	              and rule_code in ('A3','A6','A9','B2','C2') "
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
	
###################################A3：|GSM日时段计费时长/GSM日汇总计费时长 - 1| ≤ 0.1%##########################	
	#GSM日时段计费时长
	set handle [aidb_open $conn]
	set sqlbuf "
            	SELECT 
            	   SUM(BIGINT(BASE_BILL_DURATION))
                FROM bass1.G_S_21001_DAY
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
     
        #GSM日汇总计费时长
	set handle [aidb_open $conn]
	set sqlbuf "
            	SELECT 
            	   SUM(BIGINT(BASE_BILL_DURATION))
                FROM bass1.G_S_21002_DAY
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
	
#数据清零
     set CHECK_VAL1 "0.00"
     set CHECK_VAL2 "0.00"	


    #将校验值插入校验结果表
	set handle [aidb_open $conn]
	set sqlbuf "INSERT INTO bass1.G_RULE_CHECK VALUES
			($timestamp ,
			'A3',
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

	#判断A3：|GSM日时段计费时长/GSM日汇总计费时长 - 1| ≤ 0.1%超标
	if { $RESULT_VAL1 != $RESULT_VAL2 } {	
		set grade 2
	        set alarmcontent "准确性A3超出集团考核范围"
	        WriteAlarm $app_name $optime $grade ${alarmcontent}
            }
###################################A6：|GSM日时段通话次数/GSM日汇总通话次数 - 1| ≤ 0.1%##########################        
	#GSM日时段通话次数
	set handle [aidb_open $conn]
	set sqlbuf "
            	SELECT 
            	   SUM(BIGINT(CALL_COUNTS))
                FROM bass1.G_S_21001_DAY
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
     
        #GSM日汇总通话次数
	set handle [aidb_open $conn]
	set sqlbuf "
            	SELECT 
            	   SUM(BIGINT(CALL_COUNTS))
                FROM bass1.G_S_21002_DAY
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
	
#数据清零
     set CHECK_VAL1 "0.00"
     set CHECK_VAL2 "0.00"	


    #将校验值插入校验结果表
	set handle [aidb_open $conn]
	set sqlbuf "INSERT INTO bass1.G_RULE_CHECK VALUES
			($timestamp ,
			'A6',
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

	#判断A6：|GSM日时段通话次数/GSM日汇总通话次数 - 1| ≤ 0.1%超标
	if { $RESULT_VAL1 != $RESULT_VAL2 } {	
		set grade 2
	        set alarmcontent "准确性A6超出集团考核范围"
	        WriteAlarm $app_name $optime $grade ${alarmcontent}
            }
##################################A9：|GSM日时段通话时长/GSM日汇总通话时长 - 1| ≤ 0.1%###############
	#GSM日时段通话时长
	set handle [aidb_open $conn]
	set sqlbuf "
            	SELECT 
            	   SUM(BIGINT(CALL_DURATION))
                FROM bass1.G_S_21001_DAY
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
     
        #GSM日汇总通话时长
	set handle [aidb_open $conn]
	set sqlbuf "
            	SELECT 
            	   SUM(BIGINT(CALL_DURATION))
                FROM bass1.G_S_21002_DAY
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
	
#数据清零
     set CHECK_VAL1 "0.00"
     set CHECK_VAL2 "0.00"	


    #将校验值插入校验结果表
	set handle [aidb_open $conn]
	set sqlbuf "INSERT INTO bass1.G_RULE_CHECK VALUES
			($timestamp ,
			'A9',
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

	#判断A9：|GSM日时段通话时长/GSM日汇总通话时长 - 1| ≤ 0.1%超标
	if { $RESULT_VAL1 != $RESULT_VAL2 } {	
		set grade 2
	        set alarmcontent "准确性A9超出集团考核范围"
	        WriteAlarm $app_name $optime $grade ${alarmcontent}
            }
##################################B2: |GSM日时段总费用/GSM日汇总总费用 - 1| ≤ 0.1%###########
	#GSM日时段总费用
	set handle [aidb_open $conn]
	set sqlbuf "
            	SELECT 
            	   SUM(BIGINT(FAVOURED_CALL_FEE))
                FROM bass1.G_S_21001_DAY
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
     
        #GSM日汇总总费用
	set handle [aidb_open $conn]
	set sqlbuf "
            	SELECT 
            	   SUM(BIGINT(FAVOURED_CALL_FEE))
                FROM bass1.G_S_21002_DAY
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
	
#数据清零
     set CHECK_VAL1 "0.00"
     set CHECK_VAL2 "0.00"	


    #将校验值插入校验结果表
	set handle [aidb_open $conn]
	set sqlbuf "INSERT INTO bass1.G_RULE_CHECK VALUES
			($timestamp ,
			'B2',
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

	#判断B2：|GSM日时段总费用/GSM日汇总总费用 - 1| ≤ 0.1%超标
	if { $RESULT_VAL1 != $RESULT_VAL2 } {	
		set grade 2
	        set alarmcontent "准确性B2超出集团考核范围"
	        WriteAlarm $app_name $optime $grade ${alarmcontent}
            }        
##################################C2: |全球通日时段计费时长/全球通日汇总计费时长 - 1| ≤ 0.1%##########
	#全球通日时段计费时长
	set handle [aidb_open $conn]
	set sqlbuf "
            	SELECT 
            	   SUM(BIGINT(BASE_BILL_DURATION))
                FROM bass1.G_S_21001_DAY
                where time_id=$timestamp AND BRAND_ID='1'"

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
     
        #全球通日汇总计费时长
	set handle [aidb_open $conn]
	set sqlbuf "
            	SELECT 
            	   SUM(BIGINT(BASE_BILL_DURATION))
                FROM bass1.G_S_21002_DAY
                where time_id=$timestamp AND BRAND_ID='1'"

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
			'C2',
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

	#判断C2：|全球通日时段计费时长/全球通日汇总计费时长 - 1| ≤ 0.1%超标
	if { $RESULT_VAL1 != $RESULT_VAL2 } {	
		set grade 2
	        set alarmcontent "准确性C2超出集团考核范围"
	        WriteAlarm $app_name $optime $grade ${alarmcontent}
            }        
##################################END#######################
	return 0
}