#################################################################
#程序名称: INT_CHECK_X0123_MONTH.tcl
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
#规则编号：X0,X1,X2,X3
#规则属性：平衡关系
#规则类型：月
#指标摘要：X0: 省内计费语音话单量
#          X1: 计费短信量
#          X2: 网间结算话单量
#          X3: 其他类话单量
#规则描述：X0: 省内计费语音话单量 = 全球通话单量+ 神州行话单量+动感地带话单量
#          X1: 计费短信量 =  点对点MO + 移动梦网信息费话单 + 移动梦网通信费话单
#          X2: 网间结算话单量 = 与中国电信 + 与中国网通 + 与中国联通 + 与中国铁通 + 其他
#          X3: 其他类话单量 = 省际边界漫游话单 +  17950话单 + 彩信话单 + 呼转话单 + WAP话单 + 其他
#校验对象：
#          BASS1.G_S_22021_MONTH   业务运行月综合
#输出参数:
# 返回值:   0 成功; -1 失败 
##################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

        #本月 yyyymm
        set op_month [string range $optime_month 0 3][string range $optime_month 5 6]
        #本月 yyyy-mm
        set opmonth $optime_month      
        #程序名称
        set app_name "INT_CHECK_X0123_MONTH.tcl"

#删除本期数据
        set handle [aidb_open $conn]
	set sql_buff "\
		delete from bass1.g_rule_check where time_id=$op_month
    	              and rule_code in ('X0','X1','X2','X3') "
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle        

###############################X0: 省内计费语音话单量
        #省内计费语音话单量
	set handle [aidb_open $conn]
	set sqlbuf "
             SELECT 
             	SUM(BIGINT(TARGET_VALUE))
             FROM  BASS1.G_S_22021_MONTH
             WHERE TIME_ID=$op_month
                  AND SHOW_ID='00030001'"
	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
		WriteTrace $errmsg 001
		return -1
	}
	while { [set p_row [aidb_fetch $handle]] != "" } {
		set RESULT_VAL1 [lindex $p_row 0]
	}
	aidb_commit $conn
	aidb_close $handle
	
	#全球通话单量+ 神州行话单量+动感地带话单量
	set handle [aidb_open $conn]
	set sqlbuf "
             SELECT 
             	SUM(BIGINT(TARGET_VALUE))
             FROM  BASS1.G_S_22021_MONTH
             WHERE TIME_ID=$op_month
                  AND SHOW_ID IN('00030002','00030003','00030004')"
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
			'X0',
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
        
	#判断X0：省内计费语音话单量 = 全球通话单量+ 神州行话单量+动感地带话单量超标
	if { $RESULT_VAL1 !=$RESULT_VAL2  } {	
	     	set grade 2
	        set alarmcontent "准确性指标X0超出集团考核范围"
	        WriteAlarm $app_name $opmonth $grade ${alarmcontent}
        }
        
###############################X1: 计费短信量
        #计费短信量
	set handle [aidb_open $conn]
	set sqlbuf "
             SELECT 
             	SUM(BIGINT(TARGET_VALUE))
             FROM  BASS1.G_S_22021_MONTH
             WHERE TIME_ID=$op_month
                  AND SHOW_ID='00030006'"
	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
		WriteTrace $errmsg 001
		return -1
	}
	while { [set p_row [aidb_fetch $handle]] != "" } {
		set RESULT_VAL1 [lindex $p_row 0]
	}
	aidb_commit $conn
	aidb_close $handle
	
	#点对点MO + 移动梦网信息费话单 + 移动梦网通信费话单
	set handle [aidb_open $conn]
	set sqlbuf "
             SELECT 
             	SUM(BIGINT(TARGET_VALUE))
             FROM  BASS1.G_S_22021_MONTH
             WHERE TIME_ID=$op_month
                  AND SHOW_ID IN ('00030007','00030008','00030009')"
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
			'X1',
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
        
        
	#判断X1：计费短信量 =  点对点MO + 移动梦网信息费话单 + 移动梦网通信费话单超标
	if { $RESULT_VAL1 !=$RESULT_VAL2  } {	
	     	set grade 2
	        set alarmcontent "准确性指标X1超出集团考核范围"
	        WriteAlarm $app_name $opmonth $grade ${alarmcontent}
        }

###############################X2: 网间结算话单量
        #网间结算话单量
	set handle [aidb_open $conn]
	set sqlbuf "
             SELECT 
             	SUM(BIGINT(TARGET_VALUE))
             FROM  BASS1.G_S_22021_MONTH
             WHERE TIME_ID=$op_month
                  AND SHOW_ID='00030012'"
	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
		WriteTrace $errmsg 001
		return -1
	}
	while { [set p_row [aidb_fetch $handle]] != "" } {
		set RESULT_VAL1 [lindex $p_row 0]
	}
	aidb_commit $conn
	aidb_close $handle
	
	#与中国电信 + 与中国网通 + 与中国联通 + 与中国铁通 + 其他
	set handle [aidb_open $conn]
	set sqlbuf "
             SELECT 
             	SUM(BIGINT(TARGET_VALUE))
             FROM  BASS1.G_S_22021_MONTH
             WHERE TIME_ID=$op_month
                  AND SHOW_ID IN ('00030013','00030014','00030015','00030016','00030017')"

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
			'X1',
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
        
        
	#判断X2：网间结算话单量 = 与中国电信 + 与中国网通 + 与中国联通 + 与中国铁通 + 其他超标
	if { $RESULT_VAL1 !=$RESULT_VAL2  } {	
	     	set grade 2
	        set alarmcontent "准确性指标X2超出集团考核范围"
	        WriteAlarm $app_name $opmonth $grade ${alarmcontent}
        }

###############################X3: 其他类话单量
        #其他类话单量
	set handle [aidb_open $conn]
	set sqlbuf "
             SELECT 
             	SUM(BIGINT(TARGET_VALUE))
             FROM  BASS1.G_S_22021_MONTH
             WHERE TIME_ID=$op_month
                  AND SHOW_ID='00030018'"
	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
		WriteTrace $errmsg 001
		return -1
	}
	while { [set p_row [aidb_fetch $handle]] != "" } {
		set RESULT_VAL1 [lindex $p_row 0]
	}
	aidb_commit $conn
	aidb_close $handle
	
	#省际边界漫游话单 +  17950话单 + 彩信话单 + 呼转话单 + WAP话单 + 其他
	set handle [aidb_open $conn]
	set sqlbuf "
             SELECT 
             	SUM(BIGINT(TARGET_VALUE))
             FROM  BASS1.G_S_22021_MONTH
             WHERE TIME_ID=$op_month
                  AND SHOW_ID IN ('00030019','00030020','00030021','00030022','00030023','00030024')"

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
			'X1',
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
        
        
	#判断X3：其他类话单量 = 省际边界漫游话单 +  17950话单 + 彩信话单 + 呼转话单 + WAP话单 + 其他超标
	if { $RESULT_VAL1 !=$RESULT_VAL2  } {	
	     	set grade 2
	        set alarmcontent "准确性指标X3超出集团考核范围"
	        WriteAlarm $app_name $opmonth $grade ${alarmcontent}
        }

##################################END#######################
	return 0
}        