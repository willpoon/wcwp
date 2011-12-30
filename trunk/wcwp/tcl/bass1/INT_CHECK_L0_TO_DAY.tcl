#################################################################
# 程序名称: INT_CHECK_L0_TO_DAY.tcl
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
#规则编号：L0
#规则属性：业务逻辑
#规则类型：日
#指标摘要：L0：GPRS业务单价
#规则描述：L0：0<本月GPRS话单总费用 /（本月GPRS上行流量＋本月GPRS下行流量）<0.05（元/KB）
#校验对象：
#          BASS1.G_S_04002_DAY     GPRS话单
#输入参数:
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：王琦
#编写时间：2007-03-22
#问题记录：1.
#修改历史: 1.
###################################################################################
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
        set app_name "INT_CHECK_L0_TO_DAY.tcl"


#删除本期数据
        set handle [aidb_open $conn]
	set sql_buff "
		DELETE FROM $db_user.G_RULE_CHECK WHERE TIME_ID=$p_timestamp
    	              AND RULE_CODE='L0' "
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
	
###############################
 
       if {$today_dd>25} {
          set handle [aidb_open $conn]
	  set sql_buff "
                  SELECT
              	    SUM(BIGINT(ALL_FEE)),
              	    SUM(BIGINT(UP_FLOWS)+BIGINT(DOWN_FLOWS))/1024 
                  FROM bass1.G_S_04002_DAY
                  WHERE TIME_ID/100=$this_month"
	if [catch {aidb_sql $handle $sql_buff } errmsg] {
		WriteTrace $errmsg 001
		return -1
	}
	while { [set p_row [aidb_fetch $handle]] != "" } {
		set CHECK_VAL1 [lindex $p_row 0]
		set CHECK_VAL2 [lindex $p_row 0]
	}
	aidb_commit $conn
	aidb_close $handle
       
       set RESULT_VAL1 [format "%.5f" [expr ${CHECK_VAL1} /100.00]]
       set RESULT_VAL2 [format "%.5f" [expr ${CHECK_VAL2} /1.00]]
       
       set RESULT [format "%.5f" [expr (${RESULT_VAL1}/1.000/${RESULT_VAL2})]]

    #将校验值插入校验结果表
	set handle [aidb_open $conn]
	set sqlbuf "INSERT INTO bass1.G_RULE_CHECK VALUES
			($p_timestamp ,
			'L0',
			cast ($RESULT_VAL1 as  DECIMAL(18, 5) ),
			cast ($RESULT_VAL2 as  DECIMAL(18, 5) ),
			cast ($RESULT as  DECIMAL(18, 5) ),
			0)"
	if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
		WriteTrace $errmsg 003
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
	
	#判断L0：0<本月GPRS话单总费用 /（本月GPRS上行流量＋本月GPRS下行流量）<0.05（元/KB）超标
	if { $RESULT<=0.0 || $RESULT>=0.05 } {
		set grade 2
	        set alarmcontent "准确性指标L0超出集团考核范围"
	        WriteAlarm $app_name $p_optime $grade ${alarmcontent}		
		} elseif {$RESULT<=0.001 || $RESULT>=0.048 } {
		set grade 3
	        set alarmcontent "准确性指标L0接近集团考核范围"
	        WriteAlarm $app_name $p_optime $grade ${alarmcontent}	
		        	
        }     
}
##################################END#######################
	return 0
}    