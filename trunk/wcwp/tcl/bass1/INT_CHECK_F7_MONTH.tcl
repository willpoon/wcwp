#################################################################
#程序名称: INT_CHECK_F7_MONTH.tcl
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
#规则编号：F7
#规则属性：业务逻辑
#规则类型：月
#指标摘要：F7: 欠费记录
#规则描述：F7: 欠费记录接口的文件名日期＞欠费帐期
#校验对象：
#          BASS1.G_I_03007_MONTH   欠费记录
#输出参数:
# 返回值:   0 成功; -1 失败
##################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

        #本月 yyyymm
        set op_month [string range $optime_month 0 3][string range $optime_month 5 6]
        #本月 yyyy-mm
        set opmonth $optime_month
        #程序名称
        set app_name "INT_CHECK_F7_MONTH.tcl"

#删除本期数据
        set handle [aidb_open $conn]
	set sql_buff "\
		delete from bass1.g_rule_check where time_id=$op_month
    	              and rule_code='F7' "
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle        
        
        #欠费记录接口的文件名日期大于欠费帐期的记录数
	set handle [aidb_open $conn]
	set sqlbuf "
                SELECT 
             	  COUNT(*)
                FROM BASS1.G_I_03007_MONTH
                WHERE TIME_ID=$op_month
                    AND TIME_ID<=BIGINT(BILL_CYC_ID)"

	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
		WriteTrace $errmsg 001
		return -1
	}
	while { [set p_row [aidb_fetch $handle]] != "" } {
		set RESULT_VAL1 [lindex $p_row 0]
	}
	aidb_commit $conn
	aidb_close $handle

    #将校验值插入校验结果表
	set handle [aidb_open $conn]
	set sqlbuf "INSERT INTO bass1.G_RULE_CHECK VALUES
			($op_month ,
			'F7',
			cast ($RESULT_VAL1 as  DECIMAL(18, 5) ),
			0,
			0,
			0)"
	if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
		WriteTrace $errmsg 003
		return -1
	}
	aidb_commit $conn
	aidb_close $handle

	#判断F7：欠费记录接口的文件名日期＞欠费帐期超标
	if { $RESULT_VAL1>0 } {	
	     	set grade 2
	        set alarmcontent "准确性指标F7超出集团考核范围"
	        WriteAlarm $app_name $opmonth $grade ${alarmcontent}
        }
##################################END#######################
	return 0
}