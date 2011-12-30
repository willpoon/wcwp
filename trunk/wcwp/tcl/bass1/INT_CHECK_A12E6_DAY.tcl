#################################################################
# 程序名称: INT_CHECK_A12E6_DAY.tcl
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
#规则编号：A1,A2,E6
#规则属性：指标异动
#规则类型：日
#指标摘要：A1：梦网短信通信费
#          A2：梦网短信下行通信费
#          E6：人均梦网短信计费量
#规则描述：A1：单条梦网短信通信费≤0.15元
#          A2：单条梦网短信下行的通信费＝0元
#          E6：10 ≤ 本月梦网短信计费量/本月梦网短信使用用户数 ≤ 40（条/户）
#校验对象：
#          BASS1.G_S_04005_DAY     梦网短信话单
#输入参数:
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：王琦
#编写时间：2007-03-22
#问题记录：1.
#修改历史: 1.
#################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {
        
        #当天 yyyymmdd
        set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]   
        #当天 yyyy-mm-dd
        set optime $op_time
        #本月 yyyymm
        set op_month [string range $op_time 0 3][string range $op_time 5 6]        
        ##今天的日期，格式dd(例：输入20070411 返回11)
        set today_dd [string range $timestamp 6 7]
        #程序名称
        set app_name "INT_CHECK_A12E6_DAY.tcl"
        puts $app_name

##        #删除本期数据
##        set handle [aidb_open $conn]
##	set sql_buff "\
##		delete from bass1.g_rule_check where time_id=$timestamp
##    	              and rule_code in ('A1','A2','E6') "
##    	puts $sql_buff
##	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
##		WriteTrace "$errmsg" 2005
##		aidb_close $handle
##		return -1
##	}
##	aidb_commit $conn
##	aidb_close $handle
##
#####################################A1：梦网短信通信费##########################
##	set handle [aidb_open $conn]
##	set sqlbuf "
##                   SELECT 
##            	     COUNT(*)
##                   FROM 
##                     bass1.G_S_04005_DAY
##                   WHERE 
##                     TIME_ID=$timestamp 
##                     AND INT(SMS_BASEFEE)>15"
##
##	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
##		WriteTrace $errmsg 001
##		return -1
##	}
##	while { [set p_row [aidb_fetch $handle]] != "" } {
##		set CHECK_VAL1 [lindex $p_row 0]
##	}
##	aidb_commit $conn
##	aidb_close $handle
##
###
##     set RESULT_VAL1 [format "%.2f" [expr ${CHECK_VAL1} /1.00]]
##
###数据清零
##     set CHECK_VAL2 "0.00"
##
##    #将校验值插入校验结果表
##	set handle [aidb_open $conn]
##	set sqlbuf "INSERT INTO bass1.G_RULE_CHECK VALUES
##			($timestamp ,
##			'A1',
##			cast ($RESULT_VAL1 as  DECIMAL(18, 5) ),
##			0,
##			0,
##			0)"
##	if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
##		WriteTrace $errmsg 003
##		return -1
##	}
##	aidb_commit $conn
##	aidb_close $handle
##	
##	#判断A1：单条梦网短信通信费≤0.15元超标
##	if { $RESULT_VAL1>0 } {	
##		set grade 2
##	        set alarmcontent "准确性指标A1超出集团考核范围"
##	        WriteAlarm $app_name $optime $grade ${alarmcontent}
##            }
#####################################A2：梦网短信下行通信费##########################
##	set handle [aidb_open $conn]
##	set sqlbuf "
##            	SELECT 
##            		COUNT(*)
##            	FROM bass1.G_S_04005_DAY
##            	WHERE TIME_ID=$timestamp 
##                  AND CALLTYPE_ID IN ('01','11')
##                  AND INT(SMS_BASEFEE)<>0"
##
##	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
##		WriteTrace $errmsg 001
##		return -1
##	}
##	while { [set p_row [aidb_fetch $handle]] != "" } {
##		set CHECK_VAL1 [lindex $p_row 0]
##	}
##	aidb_commit $conn
##	aidb_close $handle
##
##     set RESULT_VAL1 [format "%.2f" [expr ${CHECK_VAL1} /1.00]]
##
###数据清零
##     set CHECK_VAL1 "0.00"
##
##    #将校验值插入校验结果表
##	set handle [aidb_open $conn]
##	set sqlbuf "INSERT INTO bass1.G_RULE_CHECK VALUES
##			($timestamp ,
##			'A2',
##			cast ($RESULT_VAL1 as  DECIMAL(18, 5) ),
##			0,
##			0,
##			0)"
##	if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
##		WriteTrace $errmsg 003
##		return -1
##	}
##	aidb_commit $conn
##	aidb_close $handle
##
##	#判断A2:单条梦网短信下行的通信费=0元超标
##	if { $RESULT_VAL1>0 } {	
##		set grade 2
##	        set alarmcontent "准确性A2超出集团考核范围"
##	        WriteAlarm $app_name $optime $grade ${alarmcontent}
##            }
###################E6：10 ≤ 本月梦网短信计费量/本月梦网短信使用用户数 ≤ 40（条/户）##########################  
##	#1-24号直接返回，25号开始从04005抽取的当月数据统计判断
##	if { $today_dd>=32 } {
##	     set handle [aidb_open $conn]
##	     set sqlbuf "
##   	              SELECT 
##   	         	COUNT(*),
##   	                COUNT(DISTINCT PRODUCT_NO) 
##   	              FROM 
##   	                bass1.G_S_04005_DAY
##   	              WHERE 
##   	                TIME_ID/100=$op_month 
##   	                AND CALLTYPE_ID IN ('00','01','10','11')
##   	                AND SMS_STATUS='0'"
##
##	     if [catch {aidb_sql $handle $sqlbuf} errmsg] {
##		WriteTrace $errmsg 001
##		return -1
##	      }
##	      while { [set p_row [aidb_fetch $handle]] != "" } {
##		set CHECK_VAL1 [lindex $p_row 0]
##		set CHECK_VAL2 [lindex $p_row 1]
##		puts $CHECK_VAL1
##		puts $CHECK_VAL2
##	      }
##	      aidb_commit $conn
##	      aidb_close $handle
##	      
##	     set RESULT_VAL1 [format "%.2f" [expr (${CHECK_VAL1}/1.000/${CHECK_VAL2})]]
##
##             #将校验值插入校验结果表
##             set handle [aidb_open $conn]
##             set sqlbuf "INSERT INTO bass1.G_RULE_CHECK VALUES
##             		($timestamp ,
##             		'E6',
##             		cast ($RESULT_VAL1 as  DECIMAL(18, 5) ),
##             		0,
##             		0,
##             		0)"
##             if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
##             	WriteTrace $errmsg 003
##             	return -1
##             }
##             aidb_commit $conn
##             aidb_close $handle
##             
##             #判断E6:10<=本月梦网短信计费量/本月梦网短信使用用户数<=40（条/户）超标
##	     if { $RESULT_VAL1<10 || $RESULT_VAL1>40 } {
##	        set grade 2
##	        set alarmcontent "准确性E6超出集团考核范围"
##	        WriteAlarm $app_name $optime $grade ${alarmcontent}	
##             }
##       }
##
  
##################################END#######################

      return 0
}
