#################################################################
#程序名称: INT_CHECK_L5_MONTH.tcl
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
#规则编号：L5
#规则属性：业务逻辑
#规则类型：月
#指标摘要：L5：帐单收入
#规则描述：L5：综合帐单总收入＝明细帐单总收入
#校验对象：
#          BASS1.G_S_03004_MONTH   明细帐单
#          BASS1.G_S_03005_MONTH   综合帐单
#输出参数:
# 返回值:   0 成功; -1 失败 
##################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

        #本月 yyyymm
        set op_month [string range $optime_month 0 3][string range $optime_month 5 6]
        #本月 yyyy-mm
        set opmonth $optime_month
        #程序名称
        set app_name "INT_CHECK_L5_MONTH.tcl"

#删除本期数据
        set handle [aidb_open $conn]
	set sql_buff "\
		delete from bass1.g_rule_check where time_id=$op_month
    	              and rule_code='L5' "
    	puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle        
	
	#综合帐单中收入
	set handle [aidb_open $conn]
	set sqlbuf "
            SELECT 
              SUM(BIGINT(SHOULD_FEE))/100.0
            FROM 
              BASS1.G_S_03005_MONTH
            WHERE 
              TIME_ID=$op_month
              AND BIGINT(ITEM_ID)/100 IN (1,2,3,4,5,6,7,8,9)"
        puts $sqlbuf
	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
		WriteTrace $errmsg 001
		return -1
	}
	while { [set p_row [aidb_fetch $handle]] != "" } {
		set RESULT_VAL1 [lindex $p_row 0]
	}
	aidb_commit $conn
	aidb_close $handle
	
	#明细帐单收入
	set handle [aidb_open $conn]
	set sqlbuf "
            SELECT 
              SUM(BIGINT(FEE_RECEIVABLE))/100.0
            FROM 
              bass1.G_S_03004_MONTH
            WHERE 
              TIME_ID=$op_month
              AND BIGINT(ACCT_ITEM_ID)/100 IN (1,2,3,4,5,6,7,8,9)
              AND ACCT_ITEM_ID NOT IN ('0337','0338','0339','0340')"
        puts $sqlbuf
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
			($op_month,
			'L5',
			cast ($RESULT_VAL1 as  DECIMAL(18, 5) ),
			cast ($RESULT_VAL2 as  DECIMAL(18, 5) ),
			0,
			0)"
        puts $sqlbuf			
	if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
		WriteTrace $errmsg 003
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
        
        
	#判断L5：综合帐单总收入＝明细帐单总收入超标
	if { $RESULT_VAL1 !=$RESULT_VAL2  } {	
	     	set grade 2
	        set alarmcontent "扣分性指标L5超出集团考核范围"
	        WriteAlarm $app_name $opmonth $grade ${alarmcontent}
        }
##################################END#######################
	return 0
}