#################################################################
#程序名称: INT_CHECK_C1K844TO46_TO_DAY.tcl
#功能描述: 
#规则编号：C1,K8,44,45,46
#规则属性：交叉验证
#规则类型：日/月
#运行粒度: 日
#指标摘要：C1：点对点短信计费量变动趋势
#          K8：点对点短信信息费
#          44：全球通点对点短信单价
#          45：神州行点对点短信单价
#          46: 动感地带点对点短信单价
#规则描述：C1：置信域下界≤点对点短信计费量日变动率≤置信域上界，
#              其中置信域边界由当日各省计费量变动率的统计均值和标准差共同确定，
#              置信上界=均值+3*标准差，置信下界=均值-3*标准差
#          K8：点对点短信信息费＝0元
#          44：全球通点对点短信，0.05元 ≤ (通信费 / 计费量) ≤ 0.15元 （RMB）
#          45：神州行点对点短信，0.05元 ≤ (通信费 / 计费量) ≤ 0.18元 （RMB）
#          46: 动感地带点对点短信，0.02元 ≤ (通信费 / 计费量) ≤ 0.12元 （RMB）
#校验对象：1.BASS1.G_S_21007_DAY     普通短信业务日使用
#          2.BASS1.G_S_21008_TO_DAY  普通短信业务月使用临时表(日使用)
#输入参数:
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：tym
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
        set app_name "INT_CHECK_C1K844TO46_TO_DAY.tcl"


#删除本期数据
        set handle [aidb_open $conn]
	set sql_buff "\
		delete from bass1.g_rule_check where time_id=$p_timestamp
    	              and rule_code in ('C1','K8','44','45','46') "
    	puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
	
#################################C1：点对点短信计费量变动趋势###################
#当日点对点短信计费量
	set handle [aidb_open $conn]
	set sqlbuf "
                SELECT 
             	  value(SUM(BIGINT(SMS_COUNTS)),0) AS JFL
                FROM BASS1.G_S_21007_DAY
                WHERE TIME_ID=$p_timestamp
                    and SVC_TYPE_ID IN ('11','12','13') AND CDR_TYPE_ID IN ('00','10','21') "
  puts $sqlbuf 
	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
		WriteTrace $errmsg 001
		return -1
	}
	while { [set p_row [aidb_fetch $handle]] != "" } {
		set CHECK_VAL1 [lindex $p_row 0]
	}
	aidb_commit $conn
	aidb_close $handle

  puts $CHECK_VAL1 
       set RESULT_VAL1 [format "%.5f" [expr ${CHECK_VAL1} /1.00]]
     
#上日点对点短信计费量
	set handle [aidb_open $conn]
	set sqlbuf "
                SELECT 
             	  value(SUM(BIGINT(SMS_COUNTS)),0) AS JFL
                FROM BASS1.G_S_21007_DAY
                WHERE TIME_ID=$yesterday
                   and SVC_TYPE_ID IN ('11','12','13') AND CDR_TYPE_ID IN ('00','10','21') "

	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
		WriteTrace $errmsg 001
		return -1
	}
	while { [set p_row [aidb_fetch $handle]] != "" } {
		set CHECK_VAL2 [lindex $p_row 0]
	}
	aidb_commit $conn
	aidb_close $handle
        
        set RESULT_VAL2 [format "%.5f" [expr ${CHECK_VAL2} /1.00]]
 
#数据清零
     set CHECK_VAL1 "0.00"
     set CHECK_VAL2 "0.00"
     
     #set RESULT_VAL2 1551234       
     set RESULT [format "%.5f" [expr (${RESULT_VAL1}-${RESULT_VAL2}) / 1.0000/${RESULT_VAL2}]]
     
    #将校验值插入校验结果表
	set handle [aidb_open $conn]
	set sqlbuf "INSERT INTO bass1.G_RULE_CHECK VALUES
			($p_timestamp ,
			'C1',
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
	
	#判断C1：根据经验，我们暂定为变动率不能过6.9
	if { $RESULT_VAL1==0 || ($RESULT>=0.069 || $RESULT<=-0.069) } {	
		set grade 2
	        set alarmcontent "扣分性指标C1超出集团考核范围"
	        WriteAlarm $app_name $p_optime $grade ${alarmcontent}	
	                }
        puts "C1结束"
#################################K8：点对点短信信息费###########################
	set handle [aidb_open $conn]
	set sqlbuf "
               SELECT 
             	 COUNT(*)
               FROM BASS1.G_S_21008_TO_DAY
               WHERE TIME_ID/100=$this_month
                   AND SVC_TYPE_ID IN ('11','12','13')
                   AND SMS_INFO_FEE<>'0'"

	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
		WriteTrace $errmsg 001
		return -1
	}
	while { [set p_row [aidb_fetch $handle]] != "" } {
		set CHECK_VAL1 [lindex $p_row 0]
	}
	aidb_commit $conn
	aidb_close $handle

#
     set RESULT_VAL1 [format "%.2f" [expr ${CHECK_VAL1} /1.00]]

#数据清零
     set CHECK_VAL1 "0.00"

    #将校验值插入校验结果表
	set handle [aidb_open $conn]
	set sqlbuf "INSERT INTO bass1.G_RULE_CHECK VALUES
			($p_timestamp ,
			'K8',
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
	
	#判断K8：短信业务类型是(11,12,13)点对点短信的信息费不等于零的记录数超标
	if { $RESULT_VAL1>0 } {	
		set grade 2
	        set alarmcontent "扣分性指标K8超出集团考核范围"
	        WriteAlarm $app_name $p_optime $grade ${alarmcontent}	
		  }
        puts "K8结束"
#################################44:全球通点对点短信单价####################
###44:全球通点对点短信单价=通信费/计费量
       if { $today_dd>=20 } {
           set handle [aidb_open $conn]
	   set sqlbuf "
                   SELECT 
             	     SUM(BIGINT(SMS_BASE_FEE)),
             	     SUM(BIGINT(SMS_COUNTS))
                   FROM bass1.G_S_21008_TO_DAY
                   WHERE TIME_ID/100=$this_month
                     AND  BRND_ID='1'
                     AND END_STATUS='0'
                     AND SVC_TYPE_ID IN ('11','12','13') AND CDR_TYPE_ID IN ('00','10','21')"        
	   if [catch {aidb_sql $handle $sqlbuf} errmsg] {
	   	WriteTrace $errmsg 001
	   	return -1
	   }
	   while { [set p_row [aidb_fetch $handle]] != "" } {
	   	set CHECK_VAL1 [lindex $p_row 0]
	   	set CHECK_VAL2 [lindex $p_row 1]
	   }
	   aidb_commit $conn
	   aidb_close $handle
	   
	   set RESULT_VAL1 [format "%.5f" [expr ${CHECK_VAL1} /100.00]]
	   set RESULT_VAL2 $CHECK_VAL2
	   puts $RESULT_VAL1
	   puts $RESULT_VAL2
##数据清零
           set CHECK_VAL1 "0.00"
           set CHECK_VAL2 "0.00"      
                
           set RESULT [format "%.5f" [expr ${RESULT_VAL1} /1.00/${RESULT_VAL2}]]
           puts $RESULT
         
           #将校验值插入校验结果表
	   set handle [aidb_open $conn]
	   set sqlbuf "INSERT INTO bass1.G_RULE_CHECK VALUES
	   		($p_timestamp ,
	   		'44',
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
	   	   
#	   #判断44：全球通点对点短信，0.05元 ≤ (通信费 / 计费量) ≤ 0.15元 （RMB）超标
	if { $RESULT<0.050 || $RESULT>0.150 } {	
		set grade 2
	        set alarmcontent "准确性指标44超出集团考核范围"
	        WriteAlarm $app_name $p_optime $grade ${alarmcontent}	
	} elseif {$RESULT<=0.060 || $RESULT>=0.140 } {
		set grade 3
	        set alarmcontent "准确性指标44超出集团考核范围"
	        WriteAlarm $app_name $p_optime $grade ${alarmcontent}	
		 }
         puts "44结束"
#################################45：神州行点对点短信单价####################
###45:神州行点对点短信单价=通信费/计费量 
           set handle [aidb_open $conn]
	   set sqlbuf "
                   SELECT 
             	     SUM(BIGINT(SMS_BASE_FEE)),
             	     SUM(BIGINT(SMS_COUNTS))
                   FROM bass1.G_S_21008_TO_DAY
                   WHERE TIME_ID/100=$this_month
                     AND  BRND_ID='2'
                     AND END_STATUS='0'
                     AND SVC_TYPE_ID IN ('11','12','13') AND CDR_TYPE_ID IN ('00','10','21')"        
	   if [catch {aidb_sql $handle $sqlbuf} errmsg] {
	   	WriteTrace $errmsg 001
	   	return -1
	   }
	   while { [set p_row [aidb_fetch $handle]] != "" } {
	   	set CHECK_VAL1 [lindex $p_row 0]
	   	set CHECK_VAL2 [lindex $p_row 1]
	   }
	   aidb_commit $conn
	   aidb_close $handle
	   
	   set RESULT_VAL1 [format "%.5f" [expr ${CHECK_VAL1} /100.00]]
	   set RESULT_VAL2 $CHECK_VAL2
	   puts $RESULT_VAL1
	   puts $RESULT_VAL2
##数据清零
           set CHECK_VAL1 "0.00"
           set CHECK_VAL2 "0.00"      
                
           set RESULT [format "%.5f" [expr ${RESULT_VAL1} /1.00/${RESULT_VAL2}]]
           puts $RESULT
         
           #将校验值插入校验结果表
	   set handle [aidb_open $conn]
	   set sqlbuf "INSERT INTO bass1.G_RULE_CHECK VALUES
	   		($p_timestamp ,
	   		'45',
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
	   	   
#	   #判断44：神州行点对点短信，0.05元 ≤ (通信费 / 计费量) ≤ 0.18元 （RMB）超标
	if { $RESULT<0.050 || $RESULT>0.180 } {	
		set grade 2
	        set alarmcontent "准确性指标45超出集团考核范围"
	        WriteAlarm $app_name $p_optime $grade ${alarmcontent}	
		} elseif {$RESULT<=0.060 || $RESULT>=0.170 } {
		set grade 3
	        set alarmcontent "准确性指标45超出集团考核范围"
	        WriteAlarm $app_name $p_optime $grade ${alarmcontent}	
	        } 
         puts "45结束" 
#################################46：动感地带点对点短信单价####################
###46:动感地带点对点短信单价=通信费/计费量 
           set handle [aidb_open $conn]
	   set sqlbuf "
                   SELECT 
             	     SUM(BIGINT(SMS_BASE_FEE)),
             	     SUM(BIGINT(SMS_COUNTS))
                   FROM bass1.G_S_21008_TO_DAY
                   WHERE TIME_ID/100=$this_month
                     AND  BRND_ID='3'
                     AND END_STATUS='0'
                     AND SVC_TYPE_ID IN ('11','12','13') AND CDR_TYPE_ID IN ('00','10','21')"        
	   if [catch {aidb_sql $handle $sqlbuf} errmsg] {
	   	WriteTrace $errmsg 001
	   	return -1
	   }
	   while { [set p_row [aidb_fetch $handle]] != "" } {
	   	set CHECK_VAL1 [lindex $p_row 0]
	   	set CHECK_VAL2 [lindex $p_row 1]
	   }
	   aidb_commit $conn
	   aidb_close $handle
	   
	   set RESULT_VAL1 [format "%.5f" [expr ${CHECK_VAL1} /100.00]]
	   set RESULT_VAL2 $CHECK_VAL2
	   puts $RESULT_VAL1
	   puts $RESULT_VAL2
##数据清零
           set CHECK_VAL1 "0.00"
           set CHECK_VAL2 "0.00"      
                
           set RESULT [format "%.5f" [expr ${RESULT_VAL1} /1.00/${RESULT_VAL2}]]
           puts $RESULT
         
           #将校验值插入校验结果表
	   set handle [aidb_open $conn]
	   set sqlbuf "INSERT INTO bass1.G_RULE_CHECK VALUES
	   		($p_timestamp ,
	   		'46',
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
	   	   
#	   #判断44：神州行点对点短信，0.02元 ≤ (通信费 / 计费量) ≤ 0.12元 （RMB）超标
	if { $RESULT<0.020 || $RESULT>0.120 } {
		set grade 2
	        set alarmcontent "准确性指标46超出集团考核范围"
	        WriteAlarm $app_name $p_optime $grade ${alarmcontent}	
	} elseif {$RESULT<=0.015 || $RESULT>=0.110 } {
		set grade 3
	        set alarmcontent "准确性指标46超出集团考核范围"
	        WriteAlarm $app_name $p_optime $grade ${alarmcontent}
	        } 
	puts "46结束"        
############end of if ##########	 
        }      
##################################END#######################
	return 0
}