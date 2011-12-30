#################################################################
#程序名称: INT_CHECK_H01234_MONTH.tcl
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
#规则编号：H0,H1,H2,H3,H4
#规则属性：业务逻辑
#规则类型：月
#指标摘要：H0: 基本资费营销案所属客户品牌
#          H1: 资费营销案费率记录
#          H2: 资费营销案固定费用记录
#          H3: 资费营销案策略记录
#          H4: 资费营销案设计背景记录
#规则描述：H0: 基本资费营销案所属客户品牌标识不能为空
#          H1: “资费营销案费率”接口中资费营销案应在“资费营销案”接口中
#          H2: “资费营销案固定费”接口中资费营销案应在“资费营销案”接口中
#          H3: “资费营销案策略”接口中资费营销案应在“资费营销案”接口中
#          H4: “资费营销案设计背景”接口中资费营销案应在“资费营销案”接口中
#校验对象：
#          BASS1.G_I_02001_MONTH   资费营销案
#          BASS1.G_I_02022_MONTH   资费营销案固定费用
#          BASS1.G_I_02021_MONTH   资费营销案费率
#          BASS1.G_I_02024_MONTH   资费营销案策略
#          BASS1.G_I_02030_MONTH   资费营销案设计背景
#输出参数:
# 返回值:   0 成功; -1 失败 
##################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

        #本月 yyyymm
        set op_month [string range $optime_month 0 3][string range $optime_month 5 6]
        #本月 yyyy-mm
        set opmonth $optime_month
        #本月最后一天 yyyymmdd
        set this_month_last_day [string range $op_month 0 5][GetThisMonthDays [string range $op_month 0 5]01]        
        #程序名称
        set app_name "INT_CHECK_H01234_MONTH.tcl"

#删除本期数据
#####        set handle [aidb_open $conn]
#####	set sql_buff "\
#####		delete from bass1.g_rule_check where time_id=$op_month
#####    	              and rule_code  in ('H0','H1','H2','H3','H4')"
#####	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
#####		WriteTrace "$errmsg" 2005
#####		aidb_close $handle
#####		return -1
#####	}
#####	aidb_commit $conn
#####	aidb_close $handle    
#####
##################################H0: 基本资费营销案所属客户品牌#############
#####        #基本资费营销案所属客户品牌标识是空的记录数
##### 	set handle [aidb_open $conn]
#####	set sqlbuf "
#####              SELECT 
#####              	COUNT(*)
#####              FROM  BASS1.G_I_02001_MONTH 
#####              WHERE TIME_ID=$op_month
#####                   AND BRAND_ID NOT IN ('1','2','3')"
#####
#####	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
#####		WriteTrace $errmsg 001
#####		return -1
#####	}
#####	while { [set p_row [aidb_fetch $handle]] != "" } {
#####		set RESULT_VAL1 [lindex $p_row 0]
#####	}
#####	aidb_commit $conn
#####	aidb_close $handle
#####
#####    #将校验值插入校验结果表
#####	set handle [aidb_open $conn]
#####	set sqlbuf "INSERT INTO bass1.G_RULE_CHECK VALUES
#####			($op_month,
#####			'H0',
#####			cast ($RESULT_VAL1 as  DECIMAL(18, 5) ),
#####			0,
#####			0,
#####			0)"
#####	if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
#####		WriteTrace $errmsg 003
#####		return -1
#####	}
#####	aidb_commit $conn
#####	aidb_close $handle
#####
#####	#判断H0：基本资费营销案所属客户品牌标识不能为空超标
#####	if { $RESULT_VAL1>0 } {	
#####	     	set grade 2
#####	        set alarmcontent "准确性指标H0超出集团考核范围"
#####	        WriteAlarm $app_name $opmonth $grade ${alarmcontent}
#####        }
#####
##################################H1: 资费营销案费率记录#############
#####       #H1: 资费营销案费率记录
##### 	set handle [aidb_open $conn]
#####	set sqlbuf "
#####              SELECT 
#####              	COUNT(*)
#####              FROM  BASS1.G_I_02021_MONTH 
#####              WHERE TIME_ID=$op_month"
#####
#####	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
#####		WriteTrace $errmsg 001
#####		return -1
#####	}
#####	while { [set p_row [aidb_fetch $handle]] != "" } {
#####		set RESULT_VAL1 [lindex $p_row 0]
#####	}
#####	aidb_commit $conn
#####	aidb_close $handle
#####	
#####       #将校验值插入校验结果表
#####	set handle [aidb_open $conn]
#####	set sqlbuf "INSERT INTO bass1.G_RULE_CHECK VALUES
#####			($op_month,
#####			'H1',
#####			cast ($RESULT_VAL1 as  DECIMAL(18, 5) ),
#####			0,
#####			0,
#####			0)"
#####	if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
#####		WriteTrace $errmsg 003
#####		return -1
#####	}
#####	aidb_commit $conn
#####	aidb_close $handle
#####
#####	#判断H1：因为现在02021接口还没有数据，所以只要判断该接口有数据，若有，则告警
#####	if { $RESULT_VAL1>0 } {	
#####	     	set grade 2
#####	        set alarmcontent "准确性指标H1超出集团考核范围"
#####	        WriteAlarm $app_name $opmonth $grade ${alarmcontent}
#####        }
#####        
##################################H2: 资费营销案固定费用记录#############
#####       #资费营销案固定费接口中且在资费营销案接口中的资费营销案个数
##### 	set handle [aidb_open $conn]
#####	set sqlbuf "
#####            SELECT 
#####		COUNT(DISTINCT A.PLAN_ID ) 
#####	    FROM 
#####		BASS1.G_I_02022_MONTH A ,
#####		BASS1.G_I_02001_MONTH B
#####             WHERE A.TIME_ID=$op_month 
#####                  AND B.TIME_ID=$op_month
#####                  AND A.PLAN_ID=B.PLAN_ID
#####                  AND B.START_DATE<='$this_month_last_day'
#####                  AND B.END_DATE>'$this_month_last_day' "
#####
#####	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
#####		WriteTrace $errmsg 001
#####		return -1
#####	}
#####	while { [set p_row [aidb_fetch $handle]] != "" } {
#####		set RESULT_VAL1 [lindex $p_row 0]
#####	}
#####	aidb_commit $conn
#####	aidb_close $handle
#####      
#####      #资费营销案接口中的资费营销案个数
##### 	set handle [aidb_open $conn]
#####	set sqlbuf "
#####              SELECT 
#####              	COUNT(DISTINCT PLAN_ID)
#####              FROM  BASS1.G_I_02022_MONTH
#####              WHERE TIME_ID=$op_month"
#####
#####	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
#####		WriteTrace $errmsg 001
#####		return -1
#####	}
#####	while { [set p_row [aidb_fetch $handle]] != "" } {
#####		set RESULT_VAL2 [lindex $p_row 0]
#####	}
#####	aidb_commit $conn
#####	aidb_close $handle
#####
#####        #将校验值插入校验结果表
#####	set handle [aidb_open $conn]
#####	set sqlbuf "INSERT INTO bass1.G_RULE_CHECK VALUES
#####			($op_month,
#####			'H2',
#####			cast ($RESULT_VAL1 as  DECIMAL(18, 5) ),
#####			cast ($RESULT_VAL2 as  DECIMAL(18, 5) ),
#####			0,
#####			0)"
#####	if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
#####		WriteTrace $errmsg 003
#####		return -1
#####	}
#####	aidb_commit $conn
#####	aidb_close $handle
#####        
#####        
#####	#判断H2："资费营销案固定费"接口中资费营销案应在"资费营销案"接口中超标
#####	if { $RESULT_VAL1 != $RESULT_VAL2 } {	
#####	     	set grade 2
#####	        set alarmcontent "准确性指标H2超出集团考核范围"
#####	        WriteAlarm $app_name $opmonth $grade ${alarmcontent}
#####        }
#####
##################################H3: 资费营销案策略记录#############
#####       #H3: 资费营销案策略记录
##### 	set handle [aidb_open $conn]
#####	set sqlbuf "
#####              SELECT 
#####              	COUNT(*)
#####              FROM  BASS1.G_I_02024_MONTH 
#####              WHERE TIME_ID=$op_month"
#####
#####	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
#####		WriteTrace $errmsg 001
#####		return -1
#####	}
#####	while { [set p_row [aidb_fetch $handle]] != "" } {
#####		set RESULT_VAL1 [lindex $p_row 0]
#####	}
#####	aidb_commit $conn
#####	aidb_close $handle
#####	
#####       #将校验值插入校验结果表
#####	set handle [aidb_open $conn]
#####	set sqlbuf "INSERT INTO bass1.G_RULE_CHECK VALUES
#####			($op_month,
#####			'H3',
#####			cast ($RESULT_VAL1 as  DECIMAL(18, 5) ),
#####			0,
#####			0,
#####			0)"
#####	if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
#####		WriteTrace $errmsg 003
#####		return -1
#####	}
#####	aidb_commit $conn
#####	aidb_close $handle
#####
#####	#判断H3：因为现在02024接口还没有数据，所以只要判断该接口有数据，若有，则告警
#####	if { $RESULT_VAL1>0 } {	
#####	     	set grade 2
#####	        set alarmcontent "准确性指标H3超出集团考核范围"
#####	        WriteAlarm $app_name $opmonth $grade ${alarmcontent}
#####        }
#####
##################################H4: 资费营销案设计背景记录#############
#####       #H4: 资费营销案设计背景记录
##### 	set handle [aidb_open $conn]
#####	set sqlbuf "
#####              SELECT 
#####              	COUNT(*)
#####              FROM  BASS1.G_I_02030_MONTH 
#####              WHERE TIME_ID=$op_month"
#####
#####	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
#####		WriteTrace $errmsg 001
#####		return -1
#####	}
#####	while { [set p_row [aidb_fetch $handle]] != "" } {
#####		set RESULT_VAL1 [lindex $p_row 0]
#####	}
#####	aidb_commit $conn
#####	aidb_close $handle
#####	
#####       #将校验值插入校验结果表
#####	set handle [aidb_open $conn]
#####	set sqlbuf "INSERT INTO bass1.G_RULE_CHECK VALUES
#####			($op_month,
#####			'H4',
#####			cast ($RESULT_VAL1 as  DECIMAL(18, 5) ),
#####			0,
#####			0,
#####			0)"
#####	if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
#####		WriteTrace $errmsg 003
#####		return -1
#####	}
#####	aidb_commit $conn
#####	aidb_close $handle
#####
#####	#判断H4：因为现在0230接口还没有数据，所以只要判断该接口有数据，若有，则告警
#####	if { $RESULT_VAL1>0 } {	
#####	     	set grade 2
#####	        set alarmcontent "准确性指标H4超出集团考核范围"
#####	        WriteAlarm $app_name $opmonth $grade ${alarmcontent}
#####        }
#####
##################################END#######################
	return 0
}