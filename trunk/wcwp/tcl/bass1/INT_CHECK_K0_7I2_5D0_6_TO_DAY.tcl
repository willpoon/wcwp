#################################################################
#程序名称: INT_CHECK_K0_7I2_5D0_6_TO_DAY.tcl
#功能描述:  
#规则编号：D0-D6,I2-I5,R099-K7
#规则属性：指标异动
#规则类型：日
#指标摘要：R099: 全球通语音通话费占比
#          R100: 神州行语音通话费占比
#          R101: 动感地带语音通话费占比
#规则描述：R099: 本月比上月变动率（全球通语音通话费/合计语音通话费）≤ 10%，且15%≤占比≤40%   --15%≤占比≤50% 
#          R100: 本月比上月变动率（神州行语音通话费/合计语音通话费）≤ 8%，且35%≤占比≤65%
#          R101: 本月比上月变动率（动感地带语音通话费/合计语音通话费）≤ 10%，且3<占比<18%
#          
#校验对象：
#          1.BASS1.G_S_21003_TO_DAY
#          2.BASS1.G_S_21006_TO_DAY
#          3.BASS1.G_S_21009_DAY
#          4.BASS1.G_A_02004_DAY
#          5.BASS1.G_A_02008_DAY
#输入参数:
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：王琦
#编写时间：2007-03-22
#问题记录：1.add by zhanght on 20090623 修改三大品牌的占比波动率
#修改历史: 1.
#  动感地带计费时长占比
#         } elseif {${DEC_RESULT_VAL1} >=4.0 || ${DEC_TARGET_VAL1}>=0.12} {
#         	set grade 3
#	        set alarmcontent "准确性指标D2接近集团考核范围"
#    这里的判断应该是接近考核范围，而不是超出考核范围 20070901 夏华学
#***************************************************/

proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

	global env

  #set op_time  2008-05-31
	set Optime $op_time
	set p_optime $op_time
	set Timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
	set op_month [string range $op_time 0 3][string range $op_time 5 6]
	puts $op_month
	set day [format "%.0f" [string range $Timestamp 6 7]]
        append op_time_month ${optime_month}-01
        set db_user $env(DB_USER)
        #求上个月 格式 yyyymm
        set last_month [GetLastMonth [string range $Timestamp 0 5]]
        puts $last_month
        #puts $last_month
        #----求上月最后一天---#,格式 yyyymmdd
        set last_month_day [GetLastDay [string range $Timestamp 0 5]01]
        #----求本月天数-----#
        set thismonthdays [GetThisMonthDays ${op_month}01]
        #puts $thismonthdays
        #----求本年天数-----#
        set thisyeardays [GetThisYearDays ${op_month}01]
        #puts $thisyeardays
        #puts $last_day_month
        set app_name "INT_CHECK_R099_7I2_5D0_6_TO_DAY.tcl"

##        set handle [aidb_open $conn]
##
##        #判断日期
##        if { $day <=28 } {
##        	    	puts "今天 $day 号，未到28号，暂不处理"
##        	    	return 0
##        	        }
##
##
##  #--删除本期数据
##  puts $Timestamp
##	set sql_buff "\
##		DELETE FROM bass1.G_RULE_CHECK WHERE TIME_ID=$Timestamp
##         AND RULE_CODE IN ('R099','R100','R101'); "
##	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
##		WriteTrace "$errmsg" 2005
##		aidb_close $handle
##		return -1
##	}
##
##
##
##
##	 #--DEC_CHECK_VALUE_11保留本月三品牌合计语音通话费,DEC_CHECK_VALUE_12保留上月三品牌合计语音通话费
##         #--本月三品牌合计语音通话费
##         set handle [aidb_open $conn]
##
##	 set sqlbuf "SELECT SUM(T.FY)
##   	         FROM 
##   	         (
##   	            SELECT SUM(BIGINT(FAVOURED_CALL_FEE)) AS FY
##   	            FROM BASS1.G_S_21003_TO_DAY
##   	            WHERE TIME_ID/100=${op_month}
##   	            UNION
##   	            SELECT SUM(BIGINT(FAVOURED_CALL_FEE)) AS FY
##   	            FROM BASS1.G_S_21006_TO_DAY
##   	            WHERE TIME_ID/100=${op_month}
##   	         )T"
##  puts $sqlbuf
##
##        if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
##		WriteTrace $errmsg 1001
##		return -1
##	}
##	if [catch {set DEC_CHECK_VALUE_11 [lindex [aidb_fetch $handle] 0]} errmsg ] {
##		WriteTrace $errmsg 1002
##		return -1
##	}
##
##	aidb_commit $conn
##
##	#--R099: 全球通语音通话费占比
##        #--本月占比乘100
##        set handle [aidb_open $conn]
##
##	 set sqlbuf "SELECT SUM(BIGINT(FAVOURED_CALL_FEE)) AS FY
##   	       FROM BASS1.G_S_21003_TO_DAY
##   	       WHERE TIME_ID/100=${op_month}
##   	             AND BRAND_ID='1'"
##  puts $sqlbuf
##
##        if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
##		WriteTrace $errmsg 1001
##		return -1
##	}
##
##	if [catch {set DEC_RESULT_VAL1 [lindex [aidb_fetch $handle] 0]} errmsg ] {
##		WriteTrace $errmsg 1002
##		return -1
##	}
##
##	aidb_commit $conn
##	
##       set DEC_RESULT_VAL1 [format "%.4f" [expr (${DEC_RESULT_VAL1} /1.00000 / ${DEC_CHECK_VALUE_11} *100)]]
##       #--数据清零
##       set DEC_CHECK_VALUE_1 "0"
##       #--上月占比乘100
##        set handle [aidb_open $conn]
##        set sqlbuf "SELECT TARGET1 
##   	         FROM BASS1.G_RULE_CHECK 
##   	         WHERE TIME_ID=${last_month_day}
##                   AND RULE_CODE='R099'"
##  puts $sqlbuf
##
##	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
##		WriteTrace $errmsg 001
##		return -1
##	}
##	if [catch {set DEC_RESULT_VAL2 [lindex [aidb_fetch $handle] 0]} errmsg ] {
##		WriteTrace $errmsg 1004
##		return -1
##	}
##	aidb_commit $conn
##	#set DEC_RESULT_VAL2 "100"
##	
##	puts "11111111111111111111111111"
##	puts "$DEC_RESULT_VAL2"
##	
##	set DEC_TARGET_VAL1 [format "%.4f" [expr (${DEC_RESULT_VAL1} /1.00000/ ${DEC_RESULT_VAL2} - 1 )]]
##	if {$DEC_TARGET_VAL1 <=0 }  {
##		set DEC_TARGET_VAL1 [format "%.4f" [expr $DEC_TARGET_VAL1 * (-1)]]
##		}
##  
##  puts "22222222222222222222222222"
##	puts ${DEC_TARGET_VAL1}
##
##	#--将校验值插入校验结果表
##	set handle [aidb_open $conn]
##	set sql_buff "INSERT INTO BASS1.G_RULE_CHECK VALUES (INT($Timestamp),'R099',$DEC_RESULT_VAL1,$DEC_RESULT_VAL2,$DEC_TARGET_VAL1,0); "
##  puts $sqlbuf
##  
##	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
##		WriteTrace "$errmsg" 2005
##		aidb_close $handle
##		return -1
##	}
##	aidb_commit $conn
##	#--判断
##        #--异常：
###R099: 本月比上月变动率（全球通语音通话费/合计语音通话费）≤ 10%，且15%≤占比≤40%
##        
##        #set DEC_TARGET_VAL1 "0.19"
###	if {${DEC_RESULT_VAL1}<15.0  || ${DEC_RESULT_VAL1}>40.0 || ${DEC_TARGET_VAL1}>0.10} {
###		set grade 2
###	        set alarmcontent "准确性指标R099超出集团考核范围"
###	        WriteAlarm $app_name $p_optime $grade ${alarmcontent}
###	                } elseif {${DEC_RESULT_VAL1}<=19.0  || ${DEC_RESULT_VAL1}>=39.5  || ${DEC_TARGET_VAL1}>0.09} {
###	        set grade 3
###	        set alarmcontent "准确性指标R099接近集团考核范围"
###	        WriteAlarm $app_name $p_optime $grade ${alarmcontent}        	
###		
###	}
##	
##	
###add by zhanght on 20090623 全球通语音通话费占比变动率≤10%	
##		if {${DEC_TARGET_VAL1}>0.10} {
##		set grade 2
##	        set alarmcontent "准确性指标R099超出集团考核范围"
##	        WriteAlarm $app_name $p_optime $grade ${alarmcontent}
##	                } elseif {${DEC_RESULT_VAL1}<=19.0  || ${DEC_RESULT_VAL1}>=39.5  || ${DEC_TARGET_VAL1}>0.09} {
##	        set grade 3
##	        set alarmcontent "准确性指标R099接近集团考核范围"
##	        WriteAlarm $app_name $p_optime $grade ${alarmcontent}        	
##		
##	}
##	
##	puts "R099 finish"
##	#----------------------------------------------------------
##	#--R100: 神州行语音通话费占比
##        #--本月占比乘100
##        set handle [aidb_open $conn]
##
##	 set sqlbuf "SELECT SUM(BIGINT(FAVOURED_CALL_FEE)) AS FY
##   	       FROM BASS1.G_S_21003_TO_DAY
##   	       WHERE TIME_ID/100=${op_month}
##   	             AND BRAND_ID='2'"
##  puts $sqlbuf
##
##        if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
##		WriteTrace $errmsg 1001
##		return -1
##	}
##
##	if [catch {set DEC_RESULT_VAL1 [lindex [aidb_fetch $handle] 0]} errmsg ] {
##		WriteTrace $errmsg 1002
##		return -1
##	}
##
##	aidb_commit $conn
##	
##       set DEC_RESULT_VAL1 [format "%.4f" [expr (${DEC_RESULT_VAL1} /1.00000 / ${DEC_CHECK_VALUE_11} *100)]]
##       #--数据清零
##       set DEC_CHECK_VALUE_1 "0"
##       #--上月占比乘100
##        set handle [aidb_open $conn]
##        set sqlbuf "SELECT TARGET1 
##   	         FROM BASS1.G_RULE_CHECK 
##   	         WHERE TIME_ID=${last_month_day}
##                   AND RULE_CODE='R100'"
##  puts $sqlbuf
##
##	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
##		WriteTrace $errmsg 001
##		return -1
##	}
##	if [catch {set DEC_RESULT_VAL2 [lindex [aidb_fetch $handle] 0]} errmsg ] {
##		WriteTrace $errmsg 1004
##		return -1
##	}
##	aidb_commit $conn
##	#set DEC_RESULT_VAL2 "100"
##	puts "$DEC_RESULT_VAL2"
##	
##	set DEC_TARGET_VAL1 [format "%.4f" [expr (${DEC_RESULT_VAL1} /1.00000/ ${DEC_RESULT_VAL2} - 1 )]]
##	if {$DEC_TARGET_VAL1 <=0 }  {
##		set DEC_TARGET_VAL1 [format "%.4f" [expr $DEC_TARGET_VAL1 * (-1)]]
##		}
##
##	puts ${DEC_TARGET_VAL1}
##
##	#--将校验值插入校验结果表
##	set handle [aidb_open $conn]
##	set sql_buff "INSERT INTO BASS1.G_RULE_CHECK VALUES (INT($Timestamp),'R100',$DEC_RESULT_VAL1,$DEC_RESULT_VAL2,$DEC_TARGET_VAL1,0); "
##	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
##		WriteTrace "$errmsg" 2005
##		aidb_close $handle
##		return -1
##	}
##	aidb_commit $conn
##	#--判断
##        #--异常：
###R100: 本月比上月变动率（神州行语音通话费/合计语音通话费）≤ 8%，且35%≤占比≤65%
##        
##        #set DEC_TARGET_VAL1 "0.19"
###	if {${DEC_RESULT_VAL1}<35.0  || ${DEC_RESULT_VAL1}>65.0 || ${DEC_TARGET_VAL1}>0.08 } {
###		set grade 2
###	        set alarmcontent "准确性指标R100超出集团考核范围"
###	        WriteAlarm $app_name $p_optime $grade ${alarmcontent}
###	        } elseif {${DEC_RESULT_VAL1}<=38.0  || ${DEC_RESULT_VAL1}>=63.0  || ${DEC_TARGET_VAL1}>0.07} {
###	        set grade 3
###	        set alarmcontent "准确性指标R100接近集团考核范围"
###	        WriteAlarm $app_name $p_optime $grade ${alarmcontent}
###	 	}
##	 	
###add by zhanght on 20090623	 	神州行语音通话费占比变动率≤10%
##	 	if { ${DEC_TARGET_VAL1}>0.10 } {
##		set grade 2
##	        set alarmcontent "准确性指标R100超出集团考核范围"
##	        WriteAlarm $app_name $p_optime $grade ${alarmcontent}
##	        } elseif {${DEC_RESULT_VAL1}<=38.0  || ${DEC_RESULT_VAL1}>=63.0  || ${DEC_TARGET_VAL1}>0.07} {
##	        set grade 3
##	        set alarmcontent "准确性指标R100接近集团考核范围"
##	        WriteAlarm $app_name $p_optime $grade ${alarmcontent}
##	 	}
##	 	
##	 	
##	 	
##	 	
##	 	
##	 	
##     puts "R100 finish"
##     #------------------------------------------------------------------
##     #--R101: 动感地带语音通话费占比
##   #--本月占比乘100
##        set handle [aidb_open $conn]
##
##	 set sqlbuf "SELECT SUM(BIGINT(FAVOURED_CALL_FEE)) AS FY
##   	       FROM BASS1.G_S_21003_TO_DAY
##   	       WHERE TIME_ID/100=${op_month}
##   	             AND BRAND_ID='3'"
##  puts $sqlbuf
##
##        if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
##		WriteTrace $errmsg 1001
##		return -1
##	}
##
##	if [catch {set DEC_RESULT_VAL1 [lindex [aidb_fetch $handle] 0]} errmsg ] {
##		WriteTrace $errmsg 1002
##		return -1
##	}
##
##	aidb_commit $conn
##	
##       set DEC_RESULT_VAL1 [format "%.4f" [expr (${DEC_RESULT_VAL1} /1.00000 / ${DEC_CHECK_VALUE_11} *100)]]
##       #--数据清零
##       set DEC_CHECK_VALUE_1 "0"
##       #--上月占比乘100
##        set handle [aidb_open $conn]
##        set sqlbuf "SELECT TARGET1 
##   	         FROM BASS1.G_RULE_CHECK 
##   	         WHERE TIME_ID=${last_month_day}
##                   AND RULE_CODE='R101'"
##  puts $sqlbuf
##
##	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
##		WriteTrace $errmsg 001
##		return -1
##	}
##	if [catch {set DEC_RESULT_VAL2 [lindex [aidb_fetch $handle] 0]} errmsg ] {
##		WriteTrace $errmsg 1004
##		return -1
##	}
##	aidb_commit $conn
##	#set DEC_RESULT_VAL2 "100"
##	puts "$DEC_RESULT_VAL2"
##	
##	set DEC_TARGET_VAL1 [format "%.4f" [expr (${DEC_RESULT_VAL1} /1.00000/ ${DEC_RESULT_VAL2} - 1 )]]
##	if {$DEC_TARGET_VAL1 <=0 }  {
##		set DEC_TARGET_VAL1 [format "%.4f" [expr $DEC_TARGET_VAL1 * (-1)]]
##		}
##
##	puts ${DEC_TARGET_VAL1}
##
##	#--将校验值插入校验结果表
##	set handle [aidb_open $conn]
##	set sql_buff "INSERT INTO BASS1.G_RULE_CHECK VALUES (INT($Timestamp),'R101',$DEC_RESULT_VAL1,$DEC_RESULT_VAL2,$DEC_TARGET_VAL1,0); "
##	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
##		WriteTrace "$errmsg" 2005
##		aidb_close $handle
##		return -1
##	}
##	aidb_commit $conn
##	#--判断
##        #--异常：
## #R101: 本月比上月变动率（动感地带语音通话费/合计语音通话费）≤ 10%，且3<占比<18%
##       
##        #set DEC_TARGET_VAL1 "0.19"
###	if {${DEC_RESULT_VAL1}<3.0   || ${DEC_RESULT_VAL1}>18.0 || ${DEC_TARGET_VAL1}>0.10 } {
###		set grade 2
###	        set alarmcontent "准确性指标R101超出集团考核范围"
###	        WriteAlarm $app_name $p_optime $grade ${alarmcontent}
###	        } elseif {${DEC_RESULT_VAL1}<=2.6|| ${DEC_TARGET_VAL1}>=17.0} {
###	        set grade 3
###	        set alarmcontent "准确性指标R101接近集团考核范围"
###	        WriteAlarm $app_name $p_optime $grade ${alarmcontent}
###	 	}
##	 	
###add by zhanght on 20090623 动感地带语音通话费占比变动率≤12%	 	
##	if { ${DEC_TARGET_VAL1}>0.12 } {
##		set grade 2
##	        set alarmcontent "准确性指标R101超出集团考核范围"
##	        WriteAlarm $app_name $p_optime $grade ${alarmcontent}
##	        } elseif {${DEC_RESULT_VAL1}<=2.6|| ${DEC_TARGET_VAL1}>=17.0} {
##	        set grade 3
##	        set alarmcontent "准确性指标R101接近集团考核范围"
##	        WriteAlarm $app_name $p_optime $grade ${alarmcontent}
##	 	}	 	
##	 	
##	 	
##	puts "R101 finish"
##	#--------------------------------------------------------------
##
##
##        aidb_close $handle
	return 0
}
