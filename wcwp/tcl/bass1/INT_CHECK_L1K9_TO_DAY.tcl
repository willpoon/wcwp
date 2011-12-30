#################################################################
# 程序名称: INT_CHECK_L1K9_TO_DAY.tcl
# 功能描述:
# 输入参数:
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
#规则编号：L1,K9
#规则属性：业务逻辑
#规则类型：日
#指标摘要：L1：点对点短信业务量占比
#          K9：梦网短信业务量占比
#规则描述：L1：70% ≤ 点对点短信计费量/短信计费量 ≤ 90%
#          K9：8% ≤ 梦网短信计费量/短信计费量 ≤ 28%
#校验对象：
#          BASS1.G_S_04014_DAY       音信互动短信话单
#          BASS1.G_S_04005_DAY       梦网短信话单
#          BASS1.G_S_21008_TO_DAY    普通短信业务月使用(日使用中间表)
#输出参数:
# 返回值:   0 成功; -1 失败
##################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

	global env
	set p_optime $op_time
	set p_timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
	set this_month [string range $op_time 0 3][string range $op_time 5 6]
	set db_user $env(DB_USER)
	#求上个月 格式 yyyymm
        set last_month [GetLastMonth [string range $p_timestamp 0 5]]
        #puts $last_month
        #----求上月最后一天---#,格式 yyyymmdd
        set last_day_month [GetLastDay [string range $p_timestamp 0 5]01]
        puts $last_day_month
        
        #上月的第一天
        set last_month_one [string range $last_day_month 0 3][string range $last_day_month 4 5]01
        puts $last_month_one
        
         ##--求昨天，格式yyyymmdd--##
        set yesterday [GetLastDay ${p_timestamp}]
        #puts $yesterday
        
        ##今天的日期，格式dd(例：输入20070411 返回11)
        set today_dd [format "%.0f" [string range $p_timestamp 6 7]]
        #puts $today_dd

        #程序名称
        set app_name "INT_CHECK_L1K9_TO_DAY.tcl"
###
###
####删除本期数据
###        set handle [aidb_open $conn]
###	set sql_buff "
###		DELETE FROM $db_user.G_RULE_CHECK WHERE TIME_ID=$p_timestamp
###    	              AND RULE_CODE IN ('L1','K9') "
###	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
###		WriteTrace "$errmsg" 2005
###		aidb_close $handle
###		return -1
###	}
###	aidb_commit $conn
###	aidb_close $handle
###
#############################L1：点对点短信业务量占比################
###       if {$today_dd>=32} {
####点对点短信计费量
###          set handle [aidb_open $conn]
###	  set sql_buff "
###                    SELECT 
###                 	SUM(BIGINT(SMS_COUNTS))
###                    FROM  bass1.G_S_21008_TO_DAY 
###                    WHERE TIME_ID/100=$this_month
###                       AND END_STATUS ='0'
###                       AND CDR_TYPE_ID IN ('00','10')
###                       AND SVC_TYPE_ID IN ('11','12','13')"
###	if [catch {aidb_sql $handle $sql_buff } errmsg] {
###		WriteTrace $errmsg 001
###		return -1
###	}
###	while { [set p_row [aidb_fetch $handle]] != "" } {
###		set CHECK_VAL1 [lindex $p_row 0]
###	}
###	aidb_commit $conn
###	aidb_close $handle
###       
###       set RESULT_VAL1 [format "%.5f" [expr ${CHECK_VAL1} /1.00]]
###       
####短信计费量
####K9指标将要用到该指标
###          set handle [aidb_open $conn]
###	  set sql_buff "
###                     SELECT 
###	               SUM(T.CNT)
###                     FROM (
###	                   SELECT SUM(BIGINT(SMS_COUNTS)) AS CNT FROM bass1.G_S_21008_TO_DAY
###                           WHERE TIME_ID/100 =$this_month
###                             AND END_STATUS = '0' AND CDR_TYPE_ID IN ('00','10') AND SVC_TYPE_ID IN ('11','12','13')
###                           UNION ALL
###		                   SELECT COUNT(*) AS CNT FROM bass1.G_S_04005_DAY
###                           WHERE TIME_ID/100 =$this_month
###                             AND SMS_STATUS = '0' AND CALLTYPE_ID IN ('00','01','10','11')
###                           UNION ALL
###                           SELECT COUNT(*) AS CNT FROM bass1.G_S_04014_DAY
###                           WHERE TIME_ID/100 =$this_month
###                             AND SMS_SEND_STATE = '0' AND SMS_BILL_TYPE IN ('00','01','10','11')
###                      ) T"
###	if [catch {aidb_sql $handle $sql_buff } errmsg] {
###		WriteTrace $errmsg 001
###		return -1
###	}
###	while { [set p_row [aidb_fetch $handle]] != "" } {
###		set CHECK_VAL2 [lindex $p_row 0]
###	}
###	aidb_commit $conn
###	aidb_close $handle
###       
###       set RESULT_VAL2 [format "%.5f" [expr ${CHECK_VAL2} /1.00]]
###       
###       set RESULT [format "%.5f" [expr (${RESULT_VAL1}/1.000/${RESULT_VAL2})]]
###
####数据清零
###       set CHECK_VAL1 0.00
###       
###    #将校验值插入校验结果表
###	set handle [aidb_open $conn]
###	set sqlbuf "INSERT INTO bass1.G_RULE_CHECK VALUES
###			($p_timestamp ,
###			'L1',
###			cast ($RESULT_VAL1 as  DECIMAL(18, 5) ),
###			cast ($RESULT_VAL2 as  DECIMAL(18, 5) ),
###			cast ($RESULT as  DECIMAL(18, 5) ),
###			0)"
###	if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
###		WriteTrace $errmsg 003
###		return -1
###	}
###	aidb_commit $conn
###	aidb_close $handle
###	
###	#判断L1：70% ≤ 点对点短信计费量/短信计费量 ≤ 90%超标
###	if { $RESULT<0.7 || $RESULT>0.9 } {
###		set grade 2
###	        set alarmcontent "准确性指标L1超出集团考核范围"
###	        WriteAlarm $app_name $p_optime $grade ${alarmcontent}		
###		} elseif {$RESULT<=0.72 || $RESULT>=0.88 } {
###		set grade 3
###	        set alarmcontent "准确性指标L1接近集团考核范围0.7~0.9,达到 ${RESULT}"
###	        WriteAlarm $app_name $p_optime $grade ${alarmcontent}	
###		        	
###        } 
#####################################K9：8% ≤ 梦网短信计费量/短信计费量 ≤ 28%
#### 梦网短信计费量
###          set handle [aidb_open $conn]
###	  set sql_buff "
###                  SELECT 
###                    COUNT(*)
###                  FROM  BASS1.G_S_04005_DAY 
###                  WHERE TIME_ID/100=$this_month
###                       AND SMS_STATUS = '0'
###                       AND  CALLTYPE_ID IN ('00','01','10','11')"
###	if [catch {aidb_sql $handle $sql_buff } errmsg] {
###		WriteTrace $errmsg 001
###		return -1
###	}
###	while { [set p_row [aidb_fetch $handle]] != "" } {
###		set CHECK_VAL1 [lindex $p_row 0]
###	}
###	aidb_commit $conn
###	aidb_close $handle
###       
###       set RESULT_VAL1 [format "%.5f" [expr ${CHECK_VAL1} /1.00]]
###       
####短信计费量
####直接取自L1的CHECK_VAL2值
###       set RESULT_VAL2 [format "%.5f" [expr ${CHECK_VAL2} /1.00]]
###       
###       set RESULT [format "%.5f" [expr (${RESULT_VAL1}/1.000/${RESULT_VAL2})]]
###
####数据清零
###       set CHECK_VAL1 0.00
###       
###    #将校验值插入校验结果表
###	set handle [aidb_open $conn]
###	set sqlbuf "INSERT INTO bass1.G_RULE_CHECK VALUES
###			($p_timestamp ,
###			'K9',
###			cast ($RESULT_VAL1 as  DECIMAL(18, 5) ),
###			cast ($RESULT_VAL2 as  DECIMAL(18, 5) ),
###			cast ($RESULT as  DECIMAL(18, 5) ),
###			0)"
###	if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
###		WriteTrace $errmsg 003
###		return -1
###	}
###	aidb_commit $conn
###	aidb_close $handle
###	
###	#判断K9：8% ≤ 梦网短信计费量/短信计费量 ≤ 28%超标
###	if { $RESULT<0.08 || $RESULT>0.28 } {
###		set grade 2
###	        set alarmcontent "准确性指标K9超出集团考核范围"
###	        WriteAlarm $app_name $p_optime $grade ${alarmcontent}	
###	        
###        } elseif {$RESULT<=0.09 || $RESULT>=0.27 } {
###        	set grade 3
###	        set alarmcontent "准确性指标K9接近集团考核范围0.08~0.28,达到 ${RESULT}"
###	        WriteAlarm $app_name $p_optime $grade ${alarmcontent}	
###	        
###		       	
###        } 
###}
##################################END#######################
	return 0
}    