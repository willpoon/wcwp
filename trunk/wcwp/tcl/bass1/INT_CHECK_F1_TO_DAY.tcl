#################################################################
# 程序名称: INT_CHECK_F1_TO_DAY.tcl
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
#规则编号：F1
#规则属性：业务逻辑
#规则类型：日
#指标摘要：人均WAP信息费
#规则描述：F1: 10≤本月WAP总信息费（计次信息费+包月信息费）/计费用户数（总信息费＞0）≤25（元/户）
#校验对象：
#          1.BASS1.G_S_04006_DAY     梦网WAP话单
#输入参数:
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：王琦
#编写时间：2007-03-22
#问题记录：1.
#修改历史: 1.
#################################################################
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

        #程序名称
        set app_name "INT_CHECK_F1_TO_DAY.tcl"

        #判断日期
##        if { $today_dd <= 32 } {
##        	    	puts "今天 $today_dd 号，未到20号，暂不处理"
##        	    	return 0
##        	}
##        	       
##        #删除本期数据
##        set handle [aidb_open $conn]
##	set sql_buff "
##		DELETE FROM $db_user.G_RULE_CHECK WHERE TIME_ID=$p_timestamp
##    	              AND RULE_CODE='F1' "
##    	puts $sql_buff
##	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
##		WriteTrace "$errmsg" 2005
##		aidb_close $handle
##		return -1
##	}
##	aidb_commit $conn
##	aidb_close $handle
##	
##        ###本月WAP总信息费（计次信息费+包月信息费）--计费用户数
##	set handle [aidb_open $conn]
##	set sql_buff "
##               SELECT 
##               	SUM(BIGINT(INFO_FEE)+BIGINT(MONTH_FEE))/100.0,
##               	COUNT(DISTINCT PRODUCT_NO)
##               FROM  bass1.G_S_04006_DAY 
##               WHERE TIME_ID/100=$this_month
##               	     AND BIGINT(INFO_FEE)+BIGINT(MONTH_FEE)>0 
##                     AND CDR_STATUS='00'"
##         puts $sql_buff
##	if [catch {aidb_sql $handle $sql_buff } errmsg] {
##		WriteTrace $errmsg 001
##		return -1
##	}
##	while { [set p_row [aidb_fetch $handle]] != "" } {
##		set CHECK_VAL1 [lindex $p_row 0]
##		set CHECK_VAL2 [lindex $p_row 1]
##	}
##	aidb_commit $conn
##	aidb_close $handle
##	
##        puts $CHECK_VAL1
##        puts $CHECK_VAL2
##        
##       set RESULT_VAL1 [format "%.5f" [expr ${CHECK_VAL1} /1.00]]
##       set RESULT_VAL2 [format "%.5f" [expr ${CHECK_VAL2} /1.00]]
##       
##       puts $RESULT_VAL1
##       puts $RESULT_VAL2
##       
##       set RESULT [format "%.5f" [expr (${RESULT_VAL1}/1.000/${RESULT_VAL2})]]
##       
##       puts $RESULT
##    #将校验值插入校验结果表
##	set handle [aidb_open $conn]
##	set sqlbuf "INSERT INTO bass1.G_RULE_CHECK VALUES
##			($p_timestamp ,
##			'F1',
##			cast ($RESULT_VAL1 as  DECIMAL(18, 5) ),
##			cast ($RESULT_VAL2 as  DECIMAL(18, 5) ),
##			cast ($RESULT as  DECIMAL(18, 5) ),
##			0)"
##        puts $sqlbuf	
##	if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
##		WriteTrace $errmsg 003
##		return -1
##	}
##	aidb_commit $conn
##	aidb_close $handle
##	
##	#判断F1：10≤本月WAP总信息费（计次信息费+包月信息费）/计费用户数（总信息费＞0）≤25（元/户）超标
##	if { $RESULT<10.00 || $RESULT>25.00 } {
##		set grade 2
##	        set alarmcontent "准确性指标F1超出集团考核范围"
##	        WriteAlarm $app_name $p_optime $grade ${alarmcontent}	
##	        
##        } elseif {$RESULT<=11.00 || $RESULT>=24.00 } {
##        	set grade 3
##	        set alarmcontent "准确性指标F1接近集团考核范围"
##	        WriteAlarm $app_name $p_optime $grade ${alarmcontent}
##	               	
##        } 
                             
	return 0
}    