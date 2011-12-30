#################################################################
# 程序名称: INT_CHECK_G56_MONTH.tcl
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
#规则编号：G5,G6
#规则属性：业务逻辑
#规则类型：月
#指标摘要：G5: 集团客户统一付费收入月变动率
#          G6: 集团信息化收入月变动率
#规则描述：G5: |本月集团客户统一付费收入/上月集团客户统一付费收入 - 1| ≤ 15%
#          G6: |本月集团信息化收入/上月集团信息化收入 - 1| ≤ 15%
#校验对象：
#          BASS1.G_S_03013_MONTH   集团客户统付收入
#输出参数:
# 返回值:   0 成功; -1 失败 
##################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

        #本月 yyyymm
        set op_month [string range $optime_month 0 3][string range $optime_month 5 6]
        #本月 yyyy-mm
        set opmonth $optime_month
        #上月  yyyymm
        set last_month [GetLastMonth [string range $op_month 0 5]]
        #程序名称
        set app_name "INT_CHECK_G56_MONTH.tcl"

#删除本期数据
        set handle [aidb_open $conn]
	set sql_buff "\
		delete from bass1.g_rule_check where time_id=$op_month
    	              and rule_code  in ('G5','G6')"
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle   

##############################G5: 集团客户统一付费收入月变动率#########
        #本月集团客户统一付费收入
        set handle [aidb_open $conn]
	set sqlbuf "
               SELECT 
             	 SUM(BIGINT(INCOME))/100.0
               FROM  bass1.G_S_03013_MONTH
               WHERE TIME_ID=$op_month"
	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
		WriteTrace $errmsg 001
		return -1
	}
	while { [set p_row [aidb_fetch $handle]] != "" } {
		set RESULT_VAL1 [lindex $p_row 0]
	}
	aidb_commit $conn
	aidb_close $handle
	
	#上月集团客户统一付费收入
        set handle [aidb_open $conn]
	set sqlbuf "
               SELECT 
             	 SUM(BIGINT(INCOME))/100.0
               FROM  bass1.G_S_03013_MONTH
               WHERE TIME_ID=$last_month"
	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
		WriteTrace $errmsg 001
		return -1
	}
	while { [set p_row [aidb_fetch $handle]] != "" } {
		set RESULT_VAL2 [lindex $p_row 0]
	}
	aidb_commit $conn
	aidb_close $handle

        #将校验值插入校验结果表
	set handle [aidb_open $conn]
	set sqlbuf "INSERT INTO bass1.G_RULE_CHECK VALUES
			($op_month ,
			'G5',
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
        
        set RESULT [format "%.2f" [expr (${RESULT_VAL1}/1.00000/ ${RESULT_VAL2}-1.0000)]]
	#判断G5：|本月集团客户统一付费收入/上月集团客户统一付费收入 - 1| ≤ 15%超标
	if { $RESULT_VAL1<=0 || $RESULT<-0.15 ||$RESULT>0.15  } {	
	     	set grade 2
	        set alarmcontent "准确性指标G5超出集团考核范围"
	        WriteAlarm $app_name $opmonth $grade ${alarmcontent}
        }

##############################G6: 集团信息化收入月变动率#########
        #本月集团信息化收入
        set handle [aidb_open $conn]
	set sqlbuf "
             SELECT 
             	SUM(BIGINT(INCOME))/100.0
             FROM  bass1.G_S_03013_MONTH
             WHERE TIME_ID=$op_month
             	AND (
                       (ENT_PROD_ID IN ('1001') AND BIGINT(ACCOUNT_ITEM) IN (1,2,3)) OR 
                       (ENT_PROD_ID IN ('1008') AND BIGINT(ACCOUNT_ITEM) IN (2,3)) OR 
                       (ENT_PROD_ID IN ('1009') AND BIGINT(ACCOUNT_ITEM) IN (2)) OR 
                       (ENT_PROD_ID IN ('2002') AND BIGINT(ACCOUNT_ITEM) IN (1,2,4,6)) OR 
                       (ENT_PROD_ID IN ('2003') AND BIGINT(ACCOUNT_ITEM) IN (1,2,4,6)) OR 
                       (ENT_PROD_ID IN ('2004') AND BIGINT(ACCOUNT_ITEM) IN (1,2,4,6)) OR 
                       (ENT_PROD_ID IN ('2001') AND BIGINT(ACCOUNT_ITEM) IN (1,2,4,6)) OR 
                       (ENT_PROD_ID IN ('2005') AND BIGINT(ACCOUNT_ITEM) IN (1,2,3,4)) OR 
                       (ENT_PROD_ID IN ('1005') AND BIGINT(ACCOUNT_ITEM) IN (2,6,7)) OR 
                       (ENT_PROD_ID IN ('2006') AND BIGINT(ACCOUNT_ITEM) IN (6)) OR 
                       (ENT_PROD_ID IN ('1004') AND BIGINT(ACCOUNT_ITEM) IN (2,3)) OR 
                       (ENT_PROD_ID IN ('1006') AND BIGINT(ACCOUNT_ITEM) IN (2,3)) OR 
                       (ENT_PROD_ID IN ('1007') AND BIGINT(ACCOUNT_ITEM) IN (2,6)) OR 
                       (ENT_PROD_ID IN ('2012') AND BIGINT(ACCOUNT_ITEM) IN (2,6)) OR 
                       (ENT_PROD_ID IN ('2007') AND BIGINT(ACCOUNT_ITEM) IN (2,6)) OR 
                       (ENT_PROD_ID IN ('2008') AND BIGINT(ACCOUNT_ITEM) IN (3,5)) OR 
                       (ENT_PROD_ID IN ('2009') AND BIGINT(ACCOUNT_ITEM) IN (3,4)) OR 
                       (ENT_PROD_ID IN ('2010') AND BIGINT(ACCOUNT_ITEM) IN (3,6,7)) OR 
                       (ENT_PROD_ID IN ('2011') AND BIGINT(ACCOUNT_ITEM) IN (1,2)) OR 
                       (ENT_PROD_ID IN ('2014') AND BIGINT(ACCOUNT_ITEM) IN (2,6)) OR 
                       (ENT_PROD_ID IN ('2015') AND BIGINT(ACCOUNT_ITEM) IN (2,6)) OR 
                       (ENT_PROD_ID IN ('2013') AND BIGINT(ACCOUNT_ITEM) IN (6,7)) OR 
                       (ENT_PROD_ID IN ('3000') AND BIGINT(ACCOUNT_ITEM) IN (9))
                    )"
               
	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
		WriteTrace $errmsg 001
		return -1
	}
	while { [set p_row [aidb_fetch $handle]] != "" } {
		set RESULT_VAL1 [lindex $p_row 0]
	}
	aidb_commit $conn
	aidb_close $handle
	
	#上月集团信息化收入
        set handle [aidb_open $conn]
	set sqlbuf "
             SELECT 
             	SUM(BIGINT(INCOME))/100.0
             FROM  bass1.G_S_03013_MONTH
             WHERE TIME_ID=$last_month
             	AND (
                       (ENT_PROD_ID IN ('1001') AND BIGINT(ACCOUNT_ITEM) IN (1,2,3)) OR 
                       (ENT_PROD_ID IN ('1008') AND BIGINT(ACCOUNT_ITEM) IN (2,3)) OR 
                       (ENT_PROD_ID IN ('1009') AND BIGINT(ACCOUNT_ITEM) IN (2)) OR 
                       (ENT_PROD_ID IN ('2002') AND BIGINT(ACCOUNT_ITEM) IN (1,2,4,6)) OR 
                       (ENT_PROD_ID IN ('2003') AND BIGINT(ACCOUNT_ITEM) IN (1,2,4,6)) OR 
                       (ENT_PROD_ID IN ('2004') AND BIGINT(ACCOUNT_ITEM) IN (1,2,4,6)) OR 
                       (ENT_PROD_ID IN ('2001') AND BIGINT(ACCOUNT_ITEM) IN (1,2,4,6)) OR 
                       (ENT_PROD_ID IN ('2005') AND BIGINT(ACCOUNT_ITEM) IN (1,2,3,4)) OR 
                       (ENT_PROD_ID IN ('1005') AND BIGINT(ACCOUNT_ITEM) IN (2,6,7)) OR 
                       (ENT_PROD_ID IN ('2006') AND BIGINT(ACCOUNT_ITEM) IN (6)) OR 
                       (ENT_PROD_ID IN ('1004') AND BIGINT(ACCOUNT_ITEM) IN (2,3)) OR 
                       (ENT_PROD_ID IN ('1006') AND BIGINT(ACCOUNT_ITEM) IN (2,3)) OR 
                       (ENT_PROD_ID IN ('1007') AND BIGINT(ACCOUNT_ITEM) IN (2,6)) OR 
                       (ENT_PROD_ID IN ('2012') AND BIGINT(ACCOUNT_ITEM) IN (2,6)) OR 
                       (ENT_PROD_ID IN ('2007') AND BIGINT(ACCOUNT_ITEM) IN (2,6)) OR 
                       (ENT_PROD_ID IN ('2008') AND BIGINT(ACCOUNT_ITEM) IN (3,5)) OR 
                       (ENT_PROD_ID IN ('2009') AND BIGINT(ACCOUNT_ITEM) IN (3,4)) OR 
                       (ENT_PROD_ID IN ('2010') AND BIGINT(ACCOUNT_ITEM) IN (3,6,7)) OR 
                       (ENT_PROD_ID IN ('2011') AND BIGINT(ACCOUNT_ITEM) IN (1,2)) OR 
                       (ENT_PROD_ID IN ('2014') AND BIGINT(ACCOUNT_ITEM) IN (2,6)) OR 
                       (ENT_PROD_ID IN ('2015') AND BIGINT(ACCOUNT_ITEM) IN (2,6)) OR 
                       (ENT_PROD_ID IN ('2013') AND BIGINT(ACCOUNT_ITEM) IN (6,7)) OR 
                       (ENT_PROD_ID IN ('3000') AND BIGINT(ACCOUNT_ITEM) IN (9))
                    )"

	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
		WriteTrace $errmsg 001
		return -1
	}
	while { [set p_row [aidb_fetch $handle]] != "" } {
		set RESULT_VAL2 [lindex $p_row 0]
	}
	aidb_commit $conn
	aidb_close $handle

        #将校验值插入校验结果表
	set handle [aidb_open $conn]
	set sqlbuf "INSERT INTO bass1.G_RULE_CHECK VALUES
	               (
			$op_month,
			'G6',
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
        
        set RESULT [format "%.2f" [expr (${RESULT_VAL1}/1.00000/ ${RESULT_VAL2}-1.0000)]]
        
	#判断G6: |本月集团信息化收入/上月集团信息化收入 - 1| ≤ 15%超标
	if { $RESULT_VAL1<=0 || $RESULT<-0.15 ||$RESULT>0.15  } {	
	     	set grade 2
	        set alarmcontent "准确性指标G6超出集团考核范围"
	        WriteAlarm $app_name $opmonth $grade ${alarmcontent}
        }

##################################END#######################
	return 0
}