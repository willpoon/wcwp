#################################################################
# 程序名称: INT_CHECK_G1234_MONTH.tcl
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
#指标摘要：G1: 使用集团短信的集团客户月变动率
#          G2: 使用移动总机的集团客户月变动率
#          G3: 使用集团彩铃的集团客户月变动率
#          G4: 实现信息化解决方案的集团客户月变动率
#规则描述：G1: |本月使用集团短信的集团客户数/上月使用集团短信的集团客户数 - 1| ≤ 15%，且客户数大于零
#          G2: |本月使用移动总机的集团客户数/上月使用移动总机的集团客户数 - 1| ≤ 20%	
#          G3: |本月使用集团彩铃的集团客户数/上月使用集团彩铃的集团客户数 - 1| ≤ 20%，且客户数大于零
#          G4: |本月实现信息化解决方案的集团客户数/上月实现信息化解决方案的集团客户数 - 1| ≤ 15%，且客户数大于零
#校验对象：
#          BASS1.G_S_22035_MONTH   集团客户标准化产品使用情况
#输出参数:
# 返回值:   0 成功; -1 失败 
#修改记录
#将	if { $RESULT_VAL1<=0 || $RESULT<-0.20 ||$RESULT>0.20  } 改为
#   if { $RESULT_VAL1<=0 || $RESULT<-0.15 ||$RESULT>0.15  }
#
##################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

        #本月 yyyymm
        set op_month [string range $optime_month 0 3][string range $optime_month 5 6]
        #本月 yyyy-mm
        set opmonth $optime_month
        #上月  yyyymm
        set last_month [GetLastMonth [string range $op_month 0 5]]
        #程序名称
        set app_name "INT_CHECK_G1234_MONTH.tcl"

#删除本期数据
        set handle [aidb_open $conn]
	set sql_buff "\
		delete from bass1.g_rule_check where time_id=$op_month
    	              and rule_code  in ('G1','G2','G3','G4')"
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle        

#############################G1: 使用集团短信的集团客户月变动率#########
        #本月使用集团短信的集团客户数
        set handle [aidb_open $conn]
	set sqlbuf "
               SELECT 
             	 COUNT(DISTINCT ENTERPRISE_ID)
               FROM  bass1.G_S_22035_MONTH
               WHERE TIME_ID=$op_month
                   AND ENT_PROD_ID='2001'"

	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
		WriteTrace $errmsg 001
		return -1
	}
	while { [set p_row [aidb_fetch $handle]] != "" } {
		set RESULT_VAL1 [lindex $p_row 0]
	}
	aidb_commit $conn
	aidb_close $handle
	
	#上月使用集团短信的集团客户数
        set handle [aidb_open $conn]
	set sqlbuf "
               SELECT 
             	 COUNT(DISTINCT ENTERPRISE_ID)
               FROM  bass1.G_S_22035_MONTH
               WHERE TIME_ID=$last_month
                   AND ENT_PROD_ID='2001'"

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
			'G1',
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
        
	#判断G1：|本月使用集团短信的集团客户数/上月使用集团短信的集团客户数 - 1| ≤ 15%,且客户数大于零超标
	if { $RESULT_VAL1<=0 || $RESULT<-0.15 ||$RESULT>0.15  } {	
	     	set grade 2
	        set alarmcontent "准确性指标G1超出集团考核范围"
	        WriteAlarm $app_name $opmonth $grade ${alarmcontent}
        }

#############################G2: 使用移动总机的集团客户月变动率#########
        #本月使用移动总机的集团客户数
        set handle [aidb_open $conn]
	set sqlbuf "
               SELECT 
             	 COUNT(DISTINCT ENTERPRISE_ID)
               FROM  bass1.G_S_22035_MONTH
               WHERE TIME_ID=$op_month
                   AND ENT_PROD_ID='1005'"

	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
		WriteTrace $errmsg 001
		return -1
	}
	while { [set p_row [aidb_fetch $handle]] != "" } {
		set RESULT_VAL1 [lindex $p_row 0]
	}
	aidb_commit $conn
	aidb_close $handle
	
	#上月使用移动总机的集团客户数
        set handle [aidb_open $conn]
	set sqlbuf "
               SELECT 
             	 COUNT(DISTINCT ENTERPRISE_ID)
               FROM  bass1.G_S_22035_MONTH
               WHERE TIME_ID=$last_month
                   AND ENT_PROD_ID='1005'"

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
			'G2',
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
        
#       set RESULT [format "%.2f" [expr (${RESULT_VAL1}/1.00000/ ${RESULT_VAL2}-1.0000)]]
        
	#判断G2：|本月使用集团短信的集团客户数/上月使用集团短信的集团客户数 - 1| ≤ 15%,且客户数大于零超标
	#因为该业务目前没有，所有客户数判断大于0，则告警
	if { $RESULT_VAL1>0 } {	
	     	set grade 2
	        set alarmcontent "准确性指标G2超出集团考核范围"
	        WriteAlarm $app_name $opmonth $grade ${alarmcontent}
        }

#############################G3: 使用集团彩铃的集团客户月变动率#########
        #本月使用集团彩铃的集团客户数
        set handle [aidb_open $conn]
	set sqlbuf "
               SELECT 
             	 COUNT(DISTINCT ENTERPRISE_ID)
               FROM  bass1.G_S_22035_MONTH
               WHERE TIME_ID=$op_month
                   AND ENT_PROD_ID='2006'"

	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
		WriteTrace $errmsg 001
		return -1
	}
	while { [set p_row [aidb_fetch $handle]] != "" } {
		set RESULT_VAL1 [lindex $p_row 0]
	}
	aidb_commit $conn
	aidb_close $handle
	
	#上月使用集团彩铃的集团客户数
        set handle [aidb_open $conn]
	set sqlbuf "
               SELECT 
             	 COUNT(DISTINCT ENTERPRISE_ID)
               FROM  bass1.G_S_22035_MONTH
               WHERE TIME_ID=$last_month
                   AND ENT_PROD_ID='2006'"

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
			'G3',
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
        
	#判断G3：|本月使用集团彩铃的集团客户数/上月使用集团彩铃的集团客户数 - 1| ≤ 20%,且客户数大于零超标
	if { $RESULT_VAL1<=0 || $RESULT<-0.20 ||$RESULT>0.20  } {	
	     	set grade 2
	        set alarmcontent "准确性指标G3超出集团考核范围"
	        WriteAlarm $app_name $opmonth $grade ${alarmcontent}
        }

#############################G4: 实现信息化解决方案的集团客户月变动率#########
        #本月实现信息化解决方案的集团客户数
        set handle [aidb_open $conn]
	set sqlbuf "
             SELECT 
             	COUNT(DISTINCT T.ENTERPRISE_ID)
             FROM  
             (
             	SELECT ENTERPRISE_ID ,COUNT(DISTINCT ENT_PROD_ID)
             	FROM BASS1.G_S_22035_MONTH
             	WHERE TIME_ID=$op_month
             	      AND ENT_PROD_ID IN ('2001','2006','2008')
             	GROUP BY ENTERPRISE_ID
             	HAVING COUNT(DISTINCT ENT_PROD_ID)>=2
             ) T"

	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
		WriteTrace $errmsg 001
		return -1
	}
	while { [set p_row [aidb_fetch $handle]] != "" } {
		set RESULT_VAL1 [lindex $p_row 0]
	}
	aidb_commit $conn
	aidb_close $handle
	
	#上月实现信息化解决方案的集团客户数
        set handle [aidb_open $conn]
	set sqlbuf "
             SELECT 
             	COUNT(DISTINCT T.ENTERPRISE_ID)
             FROM  
             (
             	SELECT ENTERPRISE_ID ,COUNT(DISTINCT ENT_PROD_ID)
             	FROM BASS1.G_S_22035_MONTH
             	WHERE TIME_ID=$last_month
             	      AND ENT_PROD_ID IN ('2001','2006','2008')
             	GROUP BY ENTERPRISE_ID
             	HAVING COUNT(DISTINCT ENT_PROD_ID)>=2
             ) T"

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
			'G4',
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
        
	#判断G4：|本月实现信息化解决方案的集团客户数/上月实现信息化解决方案的集团客户数 - 1| ≤ 15%,且客户数大于零超标
	if { $RESULT_VAL1<=0 || $RESULT<-0.15 ||$RESULT>0.15  } {	
	     	set grade 2
	        set alarmcontent "准确性指标G4超出集团考核范围"
	        WriteAlarm $app_name $opmonth $grade ${alarmcontent}
        }
 
 ##################################END#######################
	return 0
}