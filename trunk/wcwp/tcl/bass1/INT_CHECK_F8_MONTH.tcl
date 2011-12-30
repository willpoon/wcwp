#################################################################
# 程序名称: INT_CHECK_F8_MONTH.tcl
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
#规则编号：F8
#规则属性：业务逻辑
#规则类型：月
#指标摘要：F8: 移动号码正常使用用户数占比
#规则描述：F8: 95% ≤ 移动号码正常使用用户数/用户到达数 ≤ 115%
#校验对象：
#          BASS1.G_I_06001_MONTH   移动电话号码
#          BASS1.G_A_02004_DAY     用户
#          BASS1.G_A_02008_DAY     用户状态
#输出参数:
# 返回值:   0 成功; -1 失败 
# liuzhilong 2009-8-3 屏弊程序
##################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

##        #本月 yyyymm
##        set op_month [string range $optime_month 0 3][string range $optime_month 5 6]
##        #本月 yyyy-mm
##        set opmonth $optime_month
##        #本月最后一天 yyyymmdd
##        set this_month_last_day [string range $op_month 0 5][GetThisMonthDays [string range $op_month 0 5]01]        
##        #程序名称
##        set app_name "INT_CHECK_F8_MONTH.tcl"
##
###删除本期数据
##        set handle [aidb_open $conn]
##	set sql_buff "
##		delete from bass1.g_rule_check where time_id=$op_month
##    	              and rule_code='F8' "
##	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
##		WriteTrace "$errmsg" 2005
##		aidb_close $handle
##		return -1
##	}
##	aidb_commit $conn
##	aidb_close $handle        
##	
##	#移动号码正常使用用户数
##	set handle [aidb_open $conn]
##	set sqlbuf "
##               SELECT 
##             	 COUNT(DISTINCT PRODUCT_NO)
##               FROM  BASS1.G_I_06001_MONTH
##               WHERE TIME_ID=$op_month
##                  AND STATE_ID='03'"
##
##	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
##		WriteTrace $errmsg 001
##		return -1
##	}
##	while { [set p_row [aidb_fetch $handle]] != "" } {
##		set RESULT_VAL1 [lindex $p_row 0]
##	}
##	aidb_commit $conn
##	aidb_close $handle
##	
##        #用户到达数
##        set RESULT_VAL2 [exec get_kpi.sh $this_month_last_day 2 2]
##        puts $RESULT_VAL2
##
##        #将校验值插入校验结果表
##	set handle [aidb_open $conn]
##	set sqlbuf "INSERT INTO bass1.G_RULE_CHECK VALUES
##			($op_month ,
##			'F8',
##			cast ($RESULT_VAL1 as  DECIMAL(18, 5) ),
##			cast ($RESULT_VAL2 as  DECIMAL(18, 5) ),
##			0,
##			0)"
##	if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
##		WriteTrace $errmsg 003
##		return -1
##	}
##	aidb_commit $conn
##	aidb_close $handle
##        
##        set RESULT [format "%.2f" [expr (${RESULT_VAL1} /1.00000/ ${RESULT_VAL2})]]
##        
##	#判断F8：95% ≤ 移动号码正常使用用户数/用户到达数 ≤ 115%超标
##	if { $RESULT_VAL1==0 || $RESULT<0.95 ||$RESULT>1.15  } {	
##	     	set grade 2
##	        set alarmcontent "准确性指标F8超出集团考核范围"
##	        WriteAlarm $app_name $opmonth $grade ${alarmcontent}
##        }
####################################END#######################
	return 0
}