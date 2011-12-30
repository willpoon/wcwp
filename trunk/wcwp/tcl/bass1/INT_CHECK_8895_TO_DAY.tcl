#################################################################
# 程序名称: INT_CHECK_R142_TO_DAY.tcl
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
#规则编号：R142
#规则属性：交叉验证
#规则类型：月(日校验)
#指标摘要：R142：人均点对点短信计费量
#规则描述：R142：|抽样详单人均点对点短信计费量/汇总数据人均点对点短信计费量 - 1| ≤ 5%，且计费量＞0
#校验对象：
#          BASS1.G_S_04011_DAY     抽样普通短信话单
#          BASS1.G_S_04012_DAY     抽样智能网短信话单
#          BASS1.G_S_21008_TO_DAY  普通短信业务月使用临时表(日使用)
#输出参数:
# 返回值:   0 成功; -1 失败
# # liuzhilong 20090927 修改校验规则编码 R142->R109
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
        set today_dd [format "%.0f" [string range $p_timestamp 6 7]]
        #puts $today_dd

        #程序名称
        set app_name "INT_CHECK_8895_TO_DAY.tcl"


#删除本期数据
        set handle [aidb_open $conn]
	set sql_buff "
		DELETE FROM $db_user.G_RULE_CHECK WHERE TIME_ID=$p_timestamp
    	              AND RULE_CODE IN ('R109') "
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
###########################R142：人均点对点短信计费量#######################
	#R142：人均点对点短信计费量
	if { $today_dd>=25 } {
        #抽样数据
	     set handle [aidb_open $conn]
	     set sqlbuf "
                  SELECT 
                   	SUM(T.TXL) AS TXL,
                   	COUNT(DISTINCT T.PRODUCT_NO) AS RS
                   FROM (
                         SELECT PRODUCT_NO,COUNT(*) AS TXL  FROM BASS1.G_S_04011_DAY
                         WHERE TIME_ID/100=$this_month
                         	   AND CDR_TYPE IN ('00','10','21')
                         	   AND SVC_TYPE IN ('11','12','13')
                         	   AND SMS_STATUS='0'
                         GROUP BY PRODUCT_NO
                         UNION 
                         SELECT PRODUCT_NO,COUNT(*)AS TXL  FROM BASS1.G_S_04012_DAY
                         WHERE TIME_ID/100=$this_month
                               AND COMM_TYPE='0'
                         GROUP BY PRODUCT_NO
                         )T"
       puts $sqlbuf
       
	     if [catch {aidb_sql $handle $sqlbuf} errmsg] {
		WriteTrace $errmsg 001
		return -1
	      }
	      while { [set p_row [aidb_fetch $handle]] != "" } {
		set CHECK_VAL1 [lindex $p_row 0]
		set CHECK_VAL2 [lindex $p_row 1]
		puts $CHECK_VAL1
		puts $CHECK_VAL2
	      }
	      aidb_commit $conn
	      aidb_close $handle
	      
	     set RESULT_VAL1 [format "%.5f" [expr (${CHECK_VAL1}/1.00000/${CHECK_VAL2})]]
	     puts $RESULT_VAL1
	     
             #数据清零
             set CHECK_VAL1 0.00
             set CHECK_VAL2 0.00
             	     
             #汇总数据
	     set handle [aidb_open $conn]
	     set sqlbuf "
                     SELECT 
                     	SUM(BIGINT(SMS_COUNTS)) AS TXL,
                     	COUNT(DISTINCT PRODUCT_NO) AS RS
                     FROM BASS1.G_S_21008_TO_DAY
                     WHERE TIME_ID/100=$this_month
                           AND END_STATUS='0'
                           AND CDR_TYPE_ID IN ('00','10','21') 
                           AND SVC_TYPE_ID IN ('11','12','13')"

	     if [catch {aidb_sql $handle $sqlbuf} errmsg] {
		WriteTrace $errmsg 001
		return -1
	      }
	      while { [set p_row [aidb_fetch $handle]] != "" } {
		set CHECK_VAL1 [lindex $p_row 0]
		set CHECK_VAL2 [lindex $p_row 1]
		puts $CHECK_VAL1
		puts $CHECK_VAL2
	      }
	      aidb_commit $conn
	      aidb_close $handle
	      
	     set RESULT_VAL2 [format "%.2f" [expr (${CHECK_VAL1}/1.00000/${CHECK_VAL2})]]
	     puts $RESULT_VAL2

#(抽样值/汇总值-1)     
#	     set RESULT [format "%.2f" [expr ($RESULT_VAL1/1.00/$RESULT_VAL2-1.00)]]
	     set RESULT [format "%.5f" [expr (${RESULT_VAL1}/1.00000/${RESULT_VAL2}-1)]]
	     puts $RESULT
	     
             #将校验值插入校验结果表
             set handle [aidb_open $conn]
             set sqlbuf "INSERT INTO bass1.G_RULE_CHECK VALUES
             		($p_timestamp ,
             		'R109',
             		cast ($RESULT_VAL1 as  DECIMAL(18, 5) ),
             		cast ($RESULT_VAL2 as  DECIMAL(18, 5) ),
             		cast ($RESULT as  DECIMAL(18, 5) ),
             		0.05)"
             if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
             	WriteTrace $errmsg 003
             	return -1
             }
             aidb_commit $conn
             aidb_close $handle
             
             
             
             #判断R109：|抽样详单人均点对点短信计费量/汇总数据人均点对点短信计费量 - 1| ≤ 5%，且计费量＞0超标
	     if { $RESULT_VAL1==0 || $RESULT_VAL2==0 || ($RESULT>0.05 ||$RESULT<-0.05 ) } {
	     	set grade 2
	        set alarmcontent "扣分性指标 人均点对点短信计费量R109(原R142)超出集团考核范围"
	        WriteAlarm $app_name $p_optime $grade ${alarmcontent}		
	     	     		
	     } elseif { $RESULT_VAL1==0 || $RESULT_VAL2==0 || ($RESULT>0.04 ||$RESULT<-0.04 ) } {	
	           set grade 3
	           set alarmcontent "扣分性指标 人均点对点短信计费量R109(原R142)接近集团考核范围5%,达到${RESULT}"
	           WriteAlarm $app_name $p_optime $grade ${alarmcontent}
	          
	    }
	    	      	       
     }
##################################END#######################
	return 0
}	    