#################################################################
# 程序名称: INT_CHECK_L2_TO_DAY.tcl
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
#规则编号：L2
#规则属性：业务逻辑
#规则类型：日
#指标摘要：L2：集团客户月变动率
#规则描述：L2：|本月集团客户数/上月集团客户数 - 1| ≤ 10%，且客户数大于零
#校验对象：
#          BASS1.G_A_01001_DAY     客户
#          BASS1.G_A_01004_DAY     集团客户
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
        #puts $last_day_month
         ##--求昨天，格式yyyymmdd--##
        set yesterday [GetLastDay ${p_timestamp}]
        #puts $yesterday
        
        ##今天的日期，格式dd(例：输入20070411 返回11)
        set today_dd [string range $p_timestamp 6 7]
        #puts $today_dd
        
        #这个月的天数，格式dd例：输入20070411 返回30)
        set DaysOfThisMonth [GetThisMonthDays ${this_month}01]
        puts $DaysOfThisMonth
        
#        #今年的天数，格式ddd
#        set DaysOfThisYear [GetThisYearDays ${this_month}01]
#        puts $DaysOfThisYear
        
        #程序名称
        set app_name "INT_CHECK_L2_TO_DAY.tcl"

       
#####删除本期数据
#####删除本期数据
####        set handle [aidb_open $conn]
####	set sql_buff "
####		DELETE FROM $db_user.G_RULE_CHECK WHERE TIME_ID=$p_timestamp
####    	              AND RULE_CODE='L2' "
####	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
####		WriteTrace "$errmsg" 2005
####		aidb_close $handle
####		return -1
####	}
####	aidb_commit $conn
####	aidb_close $handle
#############################################
####      if {$today_dd>19} {
#####截止今日，集团客户到达数
####          set handle [aidb_open $conn]
####	  set sql_buff "
####                 SELECT
####                   COUNT(DISTINCT T.ENTERPRISE_ID)
####                 FROM (
####                       SELECT A.ENTERPRISE_ID,A.CUST_STATU_TYP_ID FROM bass1.G_A_01004_DAY A,
####                       (
####                        SELECT ENTERPRISE_ID, MAX(TIME_ID) AS TIME_ID FROM bass1.G_A_01004_DAY
####                        WHERE TIME_ID <=$p_timestamp  GROUP BY ENTERPRISE_ID 
####                        )B
####                        WHERE   A.TIME_ID=B.TIME_ID AND A.ENTERPRISE_ID=B.ENTERPRISE_ID  
####                        AND A.CUST_STATU_TYP_ID ='20' 
####                      )T,
####                      (
####                        SELECT  A.CUST_ID AS CUST_ID FROM bass1.G_A_01001_DAY A,
####                        (
####                         SELECT CUST_ID,MAX(TIME_ID) AS TIME_ID FROM bass1.G_A_01001_DAY
####                         WHERE TIME_ID <=$p_timestamp GROUP BY CUST_ID 
####                        )B
####                        WHERE A.TIME_ID=B.TIME_ID AND A.CUST_ID = B.CUST_ID  
####                        AND A.ORG_TYPE_ID ='2'
####                      )P 
####                  WHERE T.ENTERPRISE_ID=P.CUST_ID "
####	if [catch {aidb_sql $handle $sql_buff } errmsg] {
####		WriteTrace $errmsg 001
####		return -1
####	}
####	while { [set p_row [aidb_fetch $handle]] != "" } {
####		set CHECK_VAL1 [lindex $p_row 0]
####	}
####	aidb_commit $conn
####	aidb_close $handle
####       
####       set RESULT_VAL1 [format "%.5f" [expr ${CHECK_VAL1} /1.00]]
####       
#####上月集团客户到达数
####          set handle [aidb_open $conn]
####	  set sql_buff "
####                 SELECT
####                   COUNT(DISTINCT T.ENTERPRISE_ID)
####                 FROM (
####                       SELECT A.ENTERPRISE_ID,A.CUST_STATU_TYP_ID FROM bass1.G_A_01004_DAY A,
####                       (
####                        SELECT ENTERPRISE_ID, MAX(TIME_ID) AS TIME_ID FROM bass1.G_A_01004_DAY
####                        WHERE TIME_ID <=$last_day_month  GROUP BY ENTERPRISE_ID 
####                        )B
####                        WHERE   A.TIME_ID=B.TIME_ID AND A.ENTERPRISE_ID=B.ENTERPRISE_ID  
####                        AND A.CUST_STATU_TYP_ID ='20' 
####                      )T,
####                      (
####                        SELECT  A.CUST_ID AS CUST_ID FROM bass1.G_A_01001_DAY A,
####                        (
####                         SELECT CUST_ID,MAX(TIME_ID) AS TIME_ID FROM bass1.G_A_01001_DAY
####                         WHERE TIME_ID <=$last_day_month GROUP BY CUST_ID 
####                        )B
####                        WHERE A.TIME_ID=B.TIME_ID AND A.CUST_ID = B.CUST_ID  
####                        AND A.ORG_TYPE_ID ='2'
####                      )P 
####                  WHERE T.ENTERPRISE_ID=P.CUST_ID "
####	if [catch {aidb_sql $handle $sql_buff } errmsg] {
####		WriteTrace $errmsg 001
####		return -1
####	}
####	while { [set p_row [aidb_fetch $handle]] != "" } {
####		set CHECK_VAL2 [lindex $p_row 0]
####	}
####	aidb_commit $conn
####	aidb_close $handle
####       
####      set RESULT_VAL2 [format "%.5f" [expr ${CHECK_VAL2} /1.00]]
####       
####       set RESULT [format "%.5f" [expr (${RESULT_VAL1}/1.000/${RESULT_VAL2} - 1)]]
####       
####       puts $RESULT_VAL1
####       puts $RESULT_VAL2
####       puts $RESULT
####       
####      
####    #将校验值插入校验结果表
####	set handle [aidb_open $conn]
####	set sqlbuf "INSERT INTO bass1.G_RULE_CHECK VALUES
####			($p_timestamp ,
####			'L2',
####			cast ($RESULT_VAL1 as  DECIMAL(18, 5) ),
####			cast ($RESULT_VAL2 as  DECIMAL(18, 5) ),
####			cast ($RESULT as  DECIMAL(18, 5) ),
####			0)"
####	if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
####		WriteTrace $errmsg 003
####		return -1
####	}
####	aidb_commit $conn
####	aidb_close $handle
####
#####系数 
####      set coefficient [format "%.5f" [expr (${today_dd}/1.000/${DaysOfThisMonth})]]
####      puts $coefficient
####      set coefficient 1.00
####       set target1 [format [expr (0.100*$coefficient)]]
####       set target2 [format [expr (-0.100*$coefficient)]]
####       set target3 [format [expr (0.095*$coefficient)]]
####       set target4 [format [expr (-0.095*$coefficient)]]  
####       puts $target1
####       puts $target2
####       puts $target3
####       puts $target4
####          
####	#判断L2：|本月集团客户数/上月集团客户数 - 1| ≤ 10%，且客户数大于零超标
####	if { $RESULT<$target2 || $RESULT>$target1 } {
####		set grade 2
####	        set alarmcontent "准确性指标L2超出集团考核范围"
####	        WriteAlarm $app_name $p_optime $grade ${alarmcontent}	
####		} elseif { $RESULT<$target4 || $RESULT>$target3 } {
####		set grade 3
####	        set alarmcontent "准确性指标L2接近集团考核范围10%,达到${RESULT}"
####	        WriteAlarm $app_name $p_optime $grade ${alarmcontent}			
####		        	
####        }     
####}
##################################END#######################

##   INT_CHECK_L2_TO_DAY.tcl
source /bassapp/bass1/tcl/INT_FIX_TMP.tcl
Trans91003 $op_time $optime_month
	return 0
}    