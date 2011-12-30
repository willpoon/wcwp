#################################################################
#程序名称: INT_CHECK_B67_MONTH.tcl
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
#规则编号：B6,B7
#规则属性：业务逻辑（B7交叉验证）
#规则类型：月
#指标摘要：B6：通过IVR（语音）和WEB下载彩铃的用户数/帐单中有彩铃费用的用户数
#          B7：日收入合计与月收入差异率≤20%
#规则描述：B6：70%<通过IVR（语音）和WEB下载彩铃的用户数/帐单中有彩铃费用的用户数<96%
#          B7：|日收入合计/月收入 - 1|≤20%
#          50: 40元 ≤ 本月动感地带ARPU值 ≤ 150元 （RMB）
#校验对象：
#          BASS1.G_S_04015_DAY         
#          BASS1.G_S_22038_DAY             
#          BASS1.G_S_03004_MONTH         
#          BASS1.G_S_03012_MONTH

#输出参数:
# 返回值:   0 成功; -1 失败
#修改记录： 夏华学 20071112 
#          B6口径增加 bill_type not in ('1','2','5','6','8')
#***************************************************/
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

	#本月 yyyymm
	set op_month [string range $optime_month 0 3][string range $optime_month 5 6]
	#本月 yyyy-mm
	set opmonth $optime_month
	#本月最后一天 yyyymmdd
        set this_month_last_day [string range $op_month 0 5][GetThisMonthDays [string range $op_month 0 5]01]	     
        #本月天数
        set this_month_days [GetThisMonthDays ${op_month}01]
        #本年天数 
        set this_year_days [GetThisYearDays ${op_month}01]
        #上月最后一天 yyyymmdd
        set last_month [GetLastMonth [string range $op_month 0 5]]
        set this_month_first_day [string range $optime_month 0 3][string range $optime_month 5 6]01
        set last_month_last_day [GetLastDay [string range $this_month_first_day 0 7]]    
        #程序名称
        set app_name "INT_CHECK_B67_MONTH.tcl"


        #删除本期数据
        set handle [aidb_open $conn]
	set sql_buff "
		DELETE FROM bass1.G_RULE_CHECK WHERE TIME_ID=$op_month
    	              AND RULE_CODE IN ('B6','B7')"
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
	
####################B6:通过IVR（语音）和WEB下载彩铃的用户数#################
##        #计算04015彩铃业务话单中入库日期是本月，定制途径是1（语音）、2(WEB)且业务标识是'1'
##        #(非DIY非集团个人彩铃)彩铃的用户数
##        set handle [aidb_open $conn]
##        set sqlbuf "\
##	    	SELECT 
##	          VALUE(COUNT(DISTINCT PRODUCT_NO),0)
##	        FROM BASS1.G_S_04015_DAY
##	        WHERE TIME_ID/100=$op_month
##	             AND CUSTOM_CHNL IN ('1','2')
##	             AND BUSI_TYPE='1'
##	             AND bill_type not in ('1','2','5','6','8') "
##        puts $sqlbuf
##	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
##		WriteTrace $errmsg 001
##		return -1
##	}
##	if [catch {set DEC_RESULT_VAL1 [lindex [aidb_fetch $handle] 0]} errmsg ] {
##		WriteTrace $errmsg 1004
##		return -1
##	}
##	aidb_commit $conn
##	aidb_close $handle
##	puts "$DEC_RESULT_VAL1"        
##	
##	#帐单中有彩铃费用的用户数
##        set handle [aidb_open $conn]
##        set sqlbuf "\
##	        SELECT COUNT(DISTINCT T.USER_ID)
##	        FROM 
##	        (
##	    	  SELECT 
##	            USER_ID
##	          FROM BASS1.G_S_03004_MONTH
##	          WHERE TIME_ID=$op_month
##	               AND ACCT_ITEM_ID IN('0519','0639')
##	          UNION ALL
##	    	  SELECT 
##	            USER_ID
##	          FROM BASS1.G_S_03012_MONTH
##	          WHERE TIME_ID=$op_month
##	               AND ACCT_ITEM_ID IN('0519','0639')
##	         )T"
##        puts $sqlbuf
##	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
##		WriteTrace $errmsg 001
##		return -1
##	}
##	if [catch {set DEC_RESULT_VAL2 [lindex [aidb_fetch $handle] 0]} errmsg ] {
##		WriteTrace $errmsg 1004
##		return -1
##	}
##	aidb_commit $conn
##	aidb_close $handle
##	puts "$DEC_RESULT_VAL2" 
##	
##	
##	set RESULT [format "%.5f" [expr (${DEC_RESULT_VAL1}/1.00000/${DEC_RESULT_VAL2})]]
##	
##	#将校验值插入校验结果表
##	set handle [aidb_open $conn]
##	set sql_buff "INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'B6',$DEC_RESULT_VAL1,$DEC_RESULT_VAL2,0,0); "
##	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
##		WriteTrace "$errmsg" 2005
##		aidb_close $handle
##		return -1
##	}
##	aidb_commit $conn
##	aidb_close $handle
##	
##        #判断
##        #异常：
##        #B6：70%<通过IVR（语音）和WEB下载彩铃的用户数/帐单中有彩铃费用的用户数<96%超标
##       if { $DEC_RESULT_VAL1==0 || $DEC_RESULT_VAL2==0  } {
##       	set grade 2
##          set alarmcontent "扣分性指标B6IVR（语音）和WEB下载彩铃的用户数/帐单中有彩铃费用的用户数为0"
##          WriteAlarm $app_name $opmonth $grade ${alarmcontent}		
##       	     		
##       } elseif { ($RESULT<=0.05 ||$RESULT>0.30) } {	
##             set grade 3
##             set alarmcontent "扣分性指标B6：通过IVR（语音）和WEB下载彩铃的用户数/帐单中有彩铃费用的用户数超出集团考核范围"
##             WriteAlarm $app_name $opmonth $grade ${alarmcontent}
##            
##      }                	
##      
##########################B7:日收入合计与月收入差异率≤20%##################
        #对22038各品牌用户日KPI接口中入库日期是当月的当日收入进行汇总求和  
        set handle [aidb_open $conn]
        set sqlbuf "\
	    	SELECT 
	          SUM(BIGINT(INCOME))
	        FROM BASS1.G_S_22038_DAY
	        WHERE TIME_ID/100=$op_month"
        puts $sqlbuf
	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
		WriteTrace $errmsg 001
		return -1
	}
	if [catch {set DEC_RESULT_VAL1 [lindex [aidb_fetch $handle] 0]} errmsg ] {
		WriteTrace $errmsg 1004
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
	puts "$DEC_RESULT_VAL1"         
	
	#月收入
        set handle [aidb_open $conn]
        set sqlbuf "\
	        SELECT SUM(T.FEE)
	        FROM 
	        (
	    	  SELECT 
	            SUM(BIGINT(FEE_RECEIVABLE)) AS FEE
	          FROM BASS1.G_S_03004_MONTH
	          WHERE TIME_ID=$op_month
	          UNION ALL
	    	  SELECT 
	            SUM(BIGINT(INCM_AMT)) AS FEE
	          FROM BASS1.G_S_03012_MONTH
	          WHERE TIME_ID=$op_month
	         )T"
        puts $sqlbuf
	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
		WriteTrace $errmsg 001
		return -1
	}
	if [catch {set DEC_RESULT_VAL2 [lindex [aidb_fetch $handle] 0]} errmsg ] {
		WriteTrace $errmsg 1004
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
	puts "$DEC_RESULT_VAL2"         	
	     		
	set RESULT [format "%.5f" [expr (${DEC_RESULT_VAL1}/1.00000/${DEC_RESULT_VAL2}-1)]]
	     		
	#将校验值插入校验结果表
	set handle [aidb_open $conn]
	set sql_buff "INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'B7',$DEC_RESULT_VAL1,$DEC_RESULT_VAL2,0,0); "
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle	   

        #判断
        #异常：
        #B7：|日收入合计/月收入 - 1|≤20%超标
	if {$RESULT<-0.20 || $RESULT>0.20} {
	           set grade 2
	           set alarmcontent "扣分性指标B7：|日收入合计/月收入 - 1|≤20%超出集团考核范围"
	           WriteAlarm $app_name $opmonth $grade ${alarmcontent}
        }	  	
        
        	
	return 0
}