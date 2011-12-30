#################################################################
#程序名称: INT_CHECK_59_62H789I01_TO_DAY.tcl
#功能描述: 
#规则编号：59,60,61,62,H7,H8,H9,I0,I1
#规则属性：交叉验证
#规则类型：日
#指标摘要：59：计费时长月/日汇总关系(GSM平台)
#          60：通话时长月/日汇总关系(GSM平台)
#          61：通话次数月/日汇总关系(GSM平台)
#          62: 普通短信通信量月/日汇总关系
#          H7: 计费时长月/日汇总关系(智能网平台)
#          H8：通话时长月/日汇总关系(智能网平台)
#          H9：通话次数月/日汇总关系(智能网平台)
#          I0: 总费用月/日汇总关系(智能网平台)
#          I1: 总费用月/日汇总关系(GSM平台)
#规则描述：59：|∑(本月每日GSM普通语音业务日使用的计费时长合计)/本月GSM普通语音业务月使用的计费时长合计 - 1|≤1％
#          60：|∑(本月每日GSM普通语音业务日使用的通话时长合计)/本月GSM普通语音业务月使用的通话时长合计 - 1|≤1％
#          61：|∑(本月每日GSM普通语音业务日使用的通话次数合计)/本月GSM普通语音业务月使用的通话次数合计 - 1|≤1％
#          62: |∑(本月每日普通短信业务日使用的通信量合计)/本月普通短信业务月使用的通信量合计 - 1|≤1％
#          H7: |∑(本月每日智能网语音业务日使用的计费时长合计)/本月智能网语音业务月使用的计费时长合计 - 1|≤1％
#          H8：|∑(本月每日智能网语音业务日使用的通话时长合计)/本月智能网语音业务月使用的通话时长合计 - 1|≤1％
#          H9：|∑(本月每日智能网语音业务日使用的通话次数合计)/本月智能网语音业务月使用的通话次数合计 - 1|≤1％
#          I0: |∑(本月每日智能网语音业务日使用的总费用合计)/本月智能网语音业务月使用的总费用合计 - 1|≤1％
#          I1: |∑(本月每日GSM普通语音业务日使用的总费用合计)/本月GSM普通语音业务月使用的总费用合计 - 1|≤1％
#校验对象：1.BASS1.G_S_21001_DAY       GSM普通语音业务日使用
#          2.BASS1.G_S_21003_TO_DAY    GSM普通语音业务月使用(日使用)
#          3.BASS1.G_S_21007_DAY       普通短信业务日使用              
#          4.BASS1.G_S_21008_TO_DAY    普通短信业务月使用(日使用)   
#输入参数:
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：王琦
#编写时间：2007-03-22
#问题记录：1.因为21004、21006接口已经送空，所以H7、H8、H9、I0指标校验屏蔽了。
#修改历史: 1.20100324 liuqf R092/R093/R094 规则为月校验，现取消此校验
#################################################################

proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

	global env

	set Optime $op_time
	set p_optime $op_time
	set Timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
	set op_month [string range $op_time 0 3][string range $op_time 5 6]
###        set db_user $env(DB_USER)
###        #求上个月 格式 yyyymm
###        set last_month [GetLastMonth [string range $Timestamp 0 5]]
###        #puts $last_month
###        #----求上月最后一天---#,格式 yyyymmdd
###        set last_day_month [GetLastDay [string range $Timestamp 0 5]01]
###        #puts $last_day_month
###        ##--求昨天，格式yyyymmdd--##
###        set yesterday [GetLastDay ${Timestamp}]
###           
###        set app_name "INT_CHECK_59_62H789I01_TO_DAY.tcl"
###        
###        set handle [aidb_open $conn]
###	set sql_buff "\
###		DELETE FROM $db_user.G_RULE_CHECK WHERE TIME_ID=${Timestamp} 
###		 AND RULE_CODE IN ('59','60','61','62','H7','H8','H9','I0','I1') "
###	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
###		WriteTrace "$errmsg" 2005
###		aidb_close $handle
###		return -1
###	}
###	aidb_commit $conn
###	aidb_close $handle
###	
###	#--59：计费时长月/日汇总关系(GSM平台)
###        #--|∑(本月每日GSM普通语音业务日使用的计费时长合计)
###    
###         
###        
###        set handle [aidb_open $conn]     
###	set sql_buff "\
###	        select 
###            	  sum(bigint(base_bill_duration))
###                from 
###                  bass1.g_s_21001_day
###                where 
###                  time_id/100=${op_month}"
###	 if [catch { aidb_sql $handle $sql_buff  } errmsg ] {
###		WriteTrace $errmsg 1001
###		return -1
###	}
###        
###	if [catch {set DEC_RESULT_VAL1 [lindex [aidb_fetch $handle] 0]} errmsg ] {
###		WriteTrace $errmsg 1002
###		return -1
###	}
###	aidb_commit $conn
###	aidb_close $handle
###	
###	#--本月GSM普通语音业务月使用的计费时长合计
###	set handle [aidb_open $conn]     
###	set sql_buff "\
###	            select 
###                      sum(bigint(base_bill_duration))
###                    from 
###                      bass1.g_s_21003_to_day
###                    where 
###                      time_id/100=${op_month}"
###	 if [catch { aidb_sql $handle $sql_buff  } errmsg ] {
###		WriteTrace $errmsg 1003
###		return -1
###	}
###        
###	if [catch {set DEC_RESULT_VAL2 [lindex [aidb_fetch $handle] 0]} errmsg ] {
###		WriteTrace $errmsg 1004
###		return -1
###	}
###	aidb_commit $conn
###	aidb_close $handle
###	
###	#--本地校验结果值
###	set DEC_TARGET_VAL1 [format "%.2f" [expr (${DEC_RESULT_VAL1} /1.00000/ ${DEC_RESULT_VAL2} - 1 )]]
###	if {$DEC_TARGET_VAL1 < 0 }  {
###		set DEC_TARGET_VAL1 [format "%.2f" [expr $DEC_TARGET_VAL1 * (-1)]]
###	}
###	#三级告警值
###	set DEC_TARGET_VAL2 0.005
###			
###	#--将校验值插入校验结果表
###	set handle [aidb_open $conn]
###	set sql_buff "\
###		INSERT INTO BASS1.G_RULE_CHECK VALUES ($Timestamp,'59',$DEC_RESULT_VAL1,$DEC_RESULT_VAL2,$DEC_TARGET_VAL1,0) "
###	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
###		WriteTrace "$errmsg" 2005
###		aidb_close $handle
###		return -1
###	}
###	aidb_commit $conn
###	aidb_close $handle
###	
###	#--判断
###	#--异常
###	#--1：|∑(本月每日GSM普通语音业务日使用的计费时长合计)/
###        #--本月GSM普通语音业务月使用的计费时长合计 - 1|≤1％超标
###	
###		
###	if {$DEC_TARGET_VAL1>0.01} {
###		set grade 2
###	        set alarmcontent "准确性指标59超出集团考核范围"
###	        WriteAlarm $app_name $p_optime $grade ${alarmcontent}
###	} elseif {${DEC_TARGET_VAL1} >= ${DEC_TARGET_VAL2}} {
###	        set grade 3
###	        set alarmcontent "准确性指标59接近集团考核范围1%,达到${DEC_TARGET_VAL1}"
###	        WriteAlarm $app_name $p_optime $grade ${alarmcontent}
###	        	
###	}
###	puts "59结束"
###	
###      #--------------------------------------#
###      #--60：通话时长月/日汇总关系（GSM平台）
###      #--∑(本月每日GSM普通语音业务日使用的通话时长合计
###      
###        set handle [aidb_open $conn]    
###	set sql_buff "\
###	        select 
###            	  sum(bigint(call_duration))
###                from 
###                  bass1.g_s_21001_day
###                where 
###                  time_id/100=${op_month}"
###        
###	 if [catch { aidb_sql $handle $sql_buff  } errmsg ] {
###		WriteTrace $errmsg 1001
###		return -1
###	}
###        
###	if [catch {set DEC_RESULT_VAL1 [lindex [aidb_fetch $handle] 0]} errmsg ] {
###		WriteTrace $errmsg 1002
###		return -1
###	}
###        
###	aidb_commit $conn
###	aidb_close $handle
###	
###	#----本月GSM普通语音业务月使用的通话时长合计
###	set handle [aidb_open $conn]      
###	set sql_buff "\
###	           select 
###                     sum(bigint(call_duration))
###                   from 
###                     bass1.g_s_21003_to_day
###                   where 
###                     time_id/100=${op_month}"
###        
###	 if [catch { aidb_sql $handle $sql_buff  } errmsg ] {
###		WriteTrace $errmsg 1003
###		return -1
###	}
###        
###	if [catch {set DEC_RESULT_VAL2 [lindex [aidb_fetch $handle] 0]} errmsg ] {
###		WriteTrace $errmsg 1004
###		return -1
###	}        
###	aidb_commit $conn
###	aidb_close $handle
###	
###	#--本地校验结果值
###	set DEC_TARGET_VAL1 [format "%.2f" [expr (${DEC_RESULT_VAL1} /1.00000/ ${DEC_RESULT_VAL2} - 1 )]]
###	if {$DEC_TARGET_VAL1 < 0 }  {
###		set DEC_TARGET_VAL1 [format "%.2f" [expr $DEC_TARGET_VAL1 * (-1)]]
###	}
###	#三级告警值
###	set DEC_TARGET_VAL2 0.005
###			
###	#--将校验值插入校验结果表
###	set handle [aidb_open $conn]
###	set sql_buff "\
###		INSERT INTO BASS1.G_RULE_CHECK VALUES ($Timestamp,'60',$DEC_RESULT_VAL1,$DEC_RESULT_VAL2,$DEC_TARGET_VAL1,0) "
###	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
###		WriteTrace "$errmsg" 2005
###		aidb_close $handle
###		return -1
###	}
###	aidb_commit $conn
###	aidb_close $handle
###	
###	#--判断
###	#--异常
###	#--|∑(本月每日GSM普通语音业务日使用的通话时长合计)/本月GSM普通语音业务月使用的通话时长合计 - 1|≤1％超标
###	
###		
###	if {$DEC_TARGET_VAL1>0.01} {
###		set grade 2
###	        set alarmcontent "准确性指标60超出集团考核范围"
###	        WriteAlarm $app_name $p_optime $grade ${alarmcontent}
###	} elseif {${DEC_TARGET_VAL1} >= ${DEC_TARGET_VAL2}} {
###	        set grade 3
###	        set alarmcontent "准确性指标60接近集团考核范围1%,达到${DEC_TARGET_VAL1}"
###	        WriteAlarm $app_name $p_optime $grade ${alarmcontent}
###	        }
###	puts "60结束"
###	
###	#--------------------------#
###	#--61：通话次数月/日汇总关系（GSM平台）
###   #--∑(本月每日GSM普通语音业务日使用的通话次数合计)
###      
###      set handle [aidb_open $conn]
###        
###	set sql_buff "select 
###            	sum(bigint(CALL_COUNTS))
###            from bass1.g_s_21001_day
###            where time_id/100=${op_month}"
###        
###	 if [catch { aidb_sql $handle $sql_buff  } errmsg ] {
###		WriteTrace $errmsg 1001
###		return -1
###	}
###        
###	if [catch {set DEC_RESULT_VAL1 [lindex [aidb_fetch $handle] 0]} errmsg ] {
###		WriteTrace $errmsg 1002
###		return -1
###	}
###        
###	aidb_commit $conn
###	aidb_close $handle
###	
###	#------本月GSM普通语音业务月使用的通话次数合计
###	set handle [aidb_open $conn]
###        
###	set sql_buff "select 
###                        sum(bigint(CALL_COUNTS))
###                        from bass1.g_s_21003_to_day
###                        where time_id/100=${op_month}"
###        
###	 if [catch { aidb_sql $handle $sql_buff  } errmsg ] {
###		WriteTrace $errmsg 1003
###		return -1
###	}
###        
###	if [catch {set DEC_RESULT_VAL2 [lindex [aidb_fetch $handle] 0]} errmsg ] {
###		WriteTrace $errmsg 1004
###		return -1
###	}
###        
###	aidb_commit $conn
###	aidb_close $handle
###	
###	#--本地校验结果值
###	set DEC_TARGET_VAL1 [format "%.2f" [expr (${DEC_RESULT_VAL1} /1.00000/ ${DEC_RESULT_VAL2} - 1 )]]
###	if {$DEC_TARGET_VAL1 < 0 }  {
###		set DEC_TARGET_VAL1 [format "%.2f" [expr $DEC_TARGET_VAL1 * (-1)]]
###	}
###	#三级告警值
###	set DEC_TARGET_VAL2 0.005
###			
###	#--将校验值插入校验结果表
###	set handle [aidb_open $conn]
###	set sql_buff "\
###		INSERT INTO BASS1.G_RULE_CHECK VALUES ($Timestamp,'61',$DEC_RESULT_VAL1,$DEC_RESULT_VAL2,$DEC_TARGET_VAL1,0) "
###	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
###		WriteTrace "$errmsg" 2005
###		aidb_close $handle
###		return -1
###	}
###	aidb_commit $conn
###	aidb_close $handle
###	
###	#--判断
###	#--异常
###	#--|∑(本月每日GSM普通语音业务日使用的通话次数合计)/本月GSM普通语音业务月使用的通话次数合计 - 1|≤1％超标
###	
###		
###	if {$DEC_TARGET_VAL1>0.01} {
###		set grade 2
###	        set alarmcontent "准确性指标61超出集团考核范围"
###	        WriteAlarm $app_name $p_optime $grade ${alarmcontent}
###		} elseif {${DEC_TARGET_VAL1} >= ${DEC_TARGET_VAL2}} {
###		set grade 3
###	        set alarmcontent "准确性指标61接近集团考核范围1%,达到${DEC_TARGET_VAL1}"
###	        WriteAlarm $app_name $p_optime $grade ${alarmcontent}	
###	        }
###	puts "61结束"
###	
###	#------------------------#
###	#--62：普通短信通信量月/日汇总关系
###        #--∑(本月每日普通短信业务日使用的通信量合计)
###      
###      set handle [aidb_open $conn]
###        
###	set sql_buff "select 
###            	sum(bigint(sms_counts))
###            from bass1.G_S_21007_DAY
###            where time_id/100=${op_month}"
###        
###	 if [catch { aidb_sql $handle $sql_buff  } errmsg ] {
###		WriteTrace $errmsg 1001
###		return -1
###	}
###	
###	if [catch {set DEC_RESULT_VAL1 [lindex [aidb_fetch $handle] 0]} errmsg ] {
###		WriteTrace $errmsg 1002
###		return -1
###	}
###	aidb_commit $conn
###	aidb_close $handle
###	
###   #------本月普通短信业务月使用的通信量合计
###	set handle [aidb_open $conn]
###        
###	set sql_buff "select 
###                        sum(bigint(sms_counts))
###                        from bass1.G_S_21008_TO_DAY
###                        where time_id/100=${op_month}"
###        
###	 if [catch { aidb_sql $handle $sql_buff  } errmsg ] {
###		WriteTrace $errmsg 1003
###		return -1
###	}
###        
###	if [catch {set DEC_RESULT_VAL2 [lindex [aidb_fetch $handle] 0]} errmsg ] {
###		WriteTrace $errmsg 1004
###		return -1
###	}
###        
###	aidb_commit $conn
###	aidb_close $handle
###	
###	#--本地校验结果值
###	set DEC_TARGET_VAL1 [format "%.2f" [expr (${DEC_RESULT_VAL1} /1.00000/ ${DEC_RESULT_VAL2} - 1 )]]
###	if {$DEC_TARGET_VAL1 < 0 }  {
###		set DEC_TARGET_VAL1 [format "%.2f" [expr $DEC_TARGET_VAL1 * (-1)]]
###	}
###	#三级告警值
###	set DEC_TARGET_VAL2 0.005
###			
###	#--将校验值插入校验结果表
###	set handle [aidb_open $conn]
###	set sql_buff "\
###		INSERT INTO BASS1.G_RULE_CHECK VALUES ($Timestamp,'62',$DEC_RESULT_VAL1,$DEC_RESULT_VAL2,$DEC_TARGET_VAL1,0) "
###	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
###		WriteTrace "$errmsg" 2005
###		aidb_close $handle
###		return -1
###	}
###	aidb_commit $conn
###	
###	#--判断
###	#--异常
###	#--|∑(本月每日普通短信业务日使用的通信量合计)/本月普通短信业务月使用的通信量合计 - 1|≤1％超标
###	
###		
###	if {$DEC_TARGET_VAL1>0.01} {
###		set grade 2
###	        set alarmcontent "准确性指标62超出集团考核范围"
###	        WriteAlarm $app_name $p_optime $grade ${alarmcontent}
###		 
###		} elseif {${DEC_TARGET_VAL1} >= ${DEC_TARGET_VAL2}} {
###		set grade 3
###	        set alarmcontent "准确性指标62接近集团考核范围1%,达到${DEC_TARGET_VAL1}"
###	        WriteAlarm $app_name $p_optime $grade ${alarmcontent}	        	
###	}
###	puts "62结束"
###	
####	 #--H7：计费时长月/日汇总关系（智能网平台）
####         #--∑(本月每日智能网语音业务日使用的计费时长合计)
####      
####      set handle [aidb_open $conn]
####        
####	set sql_buff "select 
####            	value(sum(bigint(call_counts)),0)
####            from bass1.G_S_21004_DAY
####            where time_id/100=${op_month}"
####        
####	 if [catch { aidb_sql $handle $sql_buff  } errmsg ] {
####		WriteTrace $errmsg 1001
####		return -1
####	}
####        
####	if [catch {set DEC_RESULT_VAL1 [lindex [aidb_fetch $handle] 0]} errmsg ] {
####		WriteTrace $errmsg 1002
####		return -1
####	}
####        
####	aidb_commit $conn
####	
####	#------本月智能网语音业务月使用的计费时长合计
####	set handle [aidb_open $conn]
####        
####	set sql_buff "select 
####                        value(sum(bigint(base_bill_duration)),9)
####                        from bass1.G_S_21006_TO_DAY
####                        where time_id/100=${op_month}"
####        
####	 if [catch { aidb_sql $handle $sql_buff  } errmsg ] {
####		WriteTrace $errmsg 1003
####		return -1
####	}
####        
####	if [catch {set DEC_RESULT_VAL2 [lindex [aidb_fetch $handle] 0]} errmsg ] {
####		WriteTrace $errmsg 1004
####		return -1
####	}
####        
####	aidb_commit $conn
####	
####	#--本地校验结果值
####	set DEC_TARGET_VAL1 [format "%.2f" [expr (${DEC_RESULT_VAL1} /1.00000/ ${DEC_RESULT_VAL2} - 1 )]]
####	if {$DEC_TARGET_VAL1 < 0 }  {
####		set DEC_TARGET_VAL1 [format "%.2f" [expr $DEC_TARGET_VAL1 * (-1)]]
####	}
####	#三级告警值
####	set DEC_TARGET_VAL1 0.005
####			
####	#--将校验值插入校验结果表
####	set handle [aidb_open $conn]
####	set sql_buff "\
####		INSERT INTO BASS1.G_RULE_CHECK VALUES ($Timestamp,'H7',$DEC_RESULT_VAL1,$DEC_RESULT_VAL2,$DEC_TARGET_VAL1,0) "
####	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
####		WriteTrace "$errmsg" 2005
####		aidb_close $handle
####		return -1
####	}
####	aidb_commit $conn
####	
####	#--判断
####	#--异常
####	#--|∑(本月每日GSM普通语音业务日使用的通话次数合计)/本月GSM普通语音业务月使用的通话次数合计 - 1|≤1％超标
####	
####		
####	if {$DEC_TARGET_VAL1>0.01} {
####		set grade 2
####	        set alarmcontent "准确性指标H7超出集团考核范围"
####	        WriteAlarm $app_name $p_optime $grade ${alarmcontent}
####	} elseif {${DEC_TARGET_VAL1} >= ${DEC_TARGET_VAL1}} {
####		set grade 3
####	        set alarmcontent "准确性指标H7接近集团考核范围1%,达到${DEC_TARGET_VAL1}"
####	        WriteAlarm $app_name $p_optime $grade ${alarmcontent}
####	        }
####	puts "H7结束"
####	
####	#----------------------------#
####	#--H8：通话时长月/日汇总关系（智能网平台）
####   #--∑(本月每日智能网语音业务日使用的通话时长合计)
####      
####      set handle [aidb_open $conn]
####        
####	set sql_buff "select 
####            	value(sum(bigint(call_duration)),0)
####            from bass1.G_S_21004_DAY
####            where time_id/100=${op_month}"
####        
####	 if [catch { aidb_sql $handle $sql_buff  } errmsg ] {
####		WriteTrace $errmsg 1001
####		return -1
####	}
####        
####	if [catch {set DEC_RESULT_VAL1 [lindex [aidb_fetch $handle] 0]} errmsg ] {
####		WriteTrace $errmsg 1002
####		return -1
####	}
####        
####	aidb_commit $conn
####	
####	#------本月智能网语音业务月使用的通话时长合计
####	set handle [aidb_open $conn]
####        
####	set sql_buff "select 
####                        value(sum(bigint(call_duration)),0)
####                        from bass1.G_S_21006_TO_DAY
####                        where time_id/100=${op_month}"
####        
####	 if [catch { aidb_sql $handle $sql_buff  } errmsg ] {
####		WriteTrace $errmsg 1003
####		return -1
####	}
####        
####	if [catch {set DEC_RESULT_VAL2 [lindex [aidb_fetch $handle] 0]} errmsg ] {
####		WriteTrace $errmsg 1004
####		return -1
####	}
####        
####	aidb_commit $conn
####	
####	#--本地校验结果值
####	set DEC_TARGET_VAL1 [format "%.2f" [expr (${DEC_RESULT_VAL1} /1.00000/ ${DEC_RESULT_VAL2} - 1 )]]
####	if {$DEC_TARGET_VAL1 < 0 }  {
####		set DEC_TARGET_VAL1 [format "%.2f" [expr $DEC_TARGET_VAL1 * (-1)]]
####	}
####	#三级告警值
####	set DEC_TARGET_VAL1 0.005
####			
####	#--将校验值插入校验结果表
####	set handle [aidb_open $conn]
####	set sql_buff "\
####		INSERT INTO BASS1.G_RULE_CHECK VALUES ($Timestamp,'H8',$DEC_RESULT_VAL1,$DEC_RESULT_VAL2,$DEC_TARGET_VAL1,0) "
####	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
####		WriteTrace "$errmsg" 2005
####		aidb_close $handle
####		return -1
####	}
####	aidb_commit $conn
####	
####	#--判断
####	#--异常
####	#--|∑(本月每日智能网语音业务日使用的通话时长合计)/本月智能网语音业务月使用的通话时长合计 - 1|≤1％超标
####	
####		
####	if {$DEC_TARGET_VAL1>0.01} {
####		set grade 2
####	        set alarmcontent "准确性指标H8超出集团考核范围"
####	        WriteAlarm $app_name $p_optime $grade ${alarmcontent}
####	} elseif {${DEC_TARGET_VAL1} >= ${DEC_TARGET_VAL1}} {
####		set grade 3
####	        set alarmcontent "准确性指标H8接近集团考核范围1%,达到${DEC_TARGET_VAL1}"
####	        WriteAlarm $app_name $p_optime $grade ${alarmcontent}
####	        		
####	}
####	puts "H8结束"
####	
####	#-----------------------##--H9：通话次数月/日汇总关系（智能网平台）
####        #--∑(本月每日智能网语音业务日使用的通话次数合计)
####      
####      set handle [aidb_open $conn]
####        
####	set sql_buff "select 
####            	value(sum(bigint(CALL_COUNTS)),0)
####            from bass1.G_S_21004_DAY
####            where time_id/100=${op_month}"
####        
####	 if [catch { aidb_sql $handle $sql_buff  } errmsg ] {
####		WriteTrace $errmsg 1001
####		return -1
####	}
####        
####	if [catch {set DEC_RESULT_VAL1 [lindex [aidb_fetch $handle] 0]} errmsg ] {
####		WriteTrace $errmsg 1002
####		return -1
####	}
####        
####	aidb_commit $conn
####	
####	#------本月智能网语音业务月使用的通话次数合计
####	set handle [aidb_open $conn]
####        
####	set sql_buff "select 
####                        value(sum(bigint(CALL_COUNTS)),0)
####                        from bass1.G_S_21006_TO_DAY
####                        where time_id/100=${op_month}"
####        
####	 if [catch { aidb_sql $handle $sql_buff  } errmsg ] {
####		WriteTrace $errmsg 1003
####		return -1
####	}
####        
####	if [catch {set DEC_RESULT_VAL2 [lindex [aidb_fetch $handle] 0]} errmsg ] {
####		WriteTrace $errmsg 1004
####		return -1
####	}
####        
####	aidb_commit $conn
####	
####	#--本地校验结果值
####	set DEC_TARGET_VAL1 [format "%.2f" [expr (${DEC_RESULT_VAL1} /1.00000/ ${DEC_RESULT_VAL2} - 1 )]]
####	if {$DEC_TARGET_VAL1 < 0 }  {
####		set DEC_TARGET_VAL1 [format "%.2f" [expr $DEC_TARGET_VAL1 * (-1)]]
####	}
####	#三级告警值
####	set DEC_TARGET_VAL1 0.005
####			
####	#--将校验值插入校验结果表
####	set handle [aidb_open $conn]
####	set sql_buff "\
####		INSERT INTO BASS1.G_RULE_CHECK VALUES ($Timestamp,'H9',$DEC_RESULT_VAL1,$DEC_RESULT_VAL2,$DEC_TARGET_VAL1,0) "
####	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
####		WriteTrace "$errmsg" 2005
####		aidb_close $handle
####		return -1
####	}
####	aidb_commit $conn
####	
####	#--判断
####	#--异常
####	#--|∑(本月每日智能网语音业务日使用的通话次数合计)/本月智能网语音业务月使用的通话次数合计 - 1|≤1％超标
####	
####		
####	if {$DEC_TARGET_VAL1>0.01} {
####		set grade 2
####	        set alarmcontent "准确性指标H9超出集团考核范围"
####	        WriteAlarm $app_name $p_optime $grade ${alarmcontent}
####	} elseif {${DEC_TARGET_VAL1} >= ${DEC_TARGET_VAL1}} {
####		set grade 3
####	        set alarmcontent "准确性指标H9接近集团考核范围1%,达到${DEC_TARGET_VAL1}"
####	        WriteAlarm $app_name $p_optime $grade ${alarmcontent}
####	        
####	}
####	puts "H9结束"
####	
####	#--------------#
####	 #--I0：总费用月/日汇总关系（智能网平台）
####   #--∑(本月每日智能网语音业务日使用的总费用合计)
####      
####      set handle [aidb_open $conn]
####        
####	set sql_buff "select 
####            	value(sum(bigint(favoured_call_fee)),0)
####            from bass1.G_S_21004_DAY
####            where time_id/100=${op_month}"
####        
####	 if [catch { aidb_sql $handle $sql_buff  } errmsg ] {
####		WriteTrace $errmsg 1001
####		return -1
####	}
####        
####	if [catch {set DEC_RESULT_VAL1 [lindex [aidb_fetch $handle] 0]} errmsg ] {
####		WriteTrace $errmsg 1002
####		return -1
####	}
####        
####	aidb_commit $conn
####	
####	#------本月智能网语音业务月使用的总费用合计
####	set handle [aidb_open $conn]
####        
####	set sql_buff "select 
####                        value(sum(bigint(favoured_call_fee)),0)
####                        from bass1.G_S_21006_TO_DAY
####                        where time_id/100=${op_month}"
####        
####	 if [catch { aidb_sql $handle $sql_buff  } errmsg ] {
####		WriteTrace $errmsg 1003
####		return -1
####	}
####        
####	if [catch {set DEC_RESULT_VAL2 [lindex [aidb_fetch $handle] 0]} errmsg ] {
####		WriteTrace $errmsg 1004
####		return -1
####	}
####        
####	aidb_commit $conn
####	
####	#--本地校验结果值
####	set DEC_TARGET_VAL1 [format "%.2f" [expr (${DEC_RESULT_VAL1} /1.00000/ ${DEC_RESULT_VAL2} - 1 )]]
####	if {$DEC_TARGET_VAL1 < 0 }  {
####		set DEC_TARGET_VAL1 [format "%.2f" [expr $DEC_TARGET_VAL1 * (-1)]]
####	}
####	#三级告警值
####	set DEC_TARGET_VAL1 0.005
####			
####	#--将校验值插入校验结果表
####	set handle [aidb_open $conn]
####	set sql_buff "\
####		INSERT INTO BASS1.G_RULE_CHECK VALUES ($Timestamp,'I0',$DEC_RESULT_VAL1,$DEC_RESULT_VAL2,$DEC_TARGET_VAL1,0) "
####	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
####		WriteTrace "$errmsg" 2005
####		aidb_close $handle
####		return -1
####	}
####	aidb_commit $conn
####	
####	#--判断
####	#--异常
####	#--|∑(本月每日智能网语音业务日使用的总费用合计)/本月智能网语音业务月使用的总费用合计 - 1|≤1％超标
####	
####		
####	if {$DEC_TARGET_VAL1>0.01} {
####		set grade 2
####	        set alarmcontent "准确性指标I0超出集团考核范围"
####	        WriteAlarm $app_name $p_optime $grade ${alarmcontent}
####	} elseif {${DEC_TARGET_VAL1} >= ${DEC_TARGET_VAL1}} {
####		set grade 3
####	        set alarmcontent "准确性指标I0接近集团考核范围1%,达到${DEC_TARGET_VAL1}"
####	        WriteAlarm $app_name $p_optime $grade ${alarmcontent}
####	        	        	}
####	puts "I0结束"
###	
###	#--I1：总费用月/日汇总关系（GSM平台）
###        #--∑(本月每日GSM语音业务日使用的总费用合计)
###      
###      set handle [aidb_open $conn]
###        
###	set sql_buff "select 
###            	sum(bigint(favoured_call_fee))
###            from bass1.G_S_21001_DAY
###            where time_id/100=${op_month}"
###        
###	 if [catch { aidb_sql $handle $sql_buff  } errmsg ] {
###		WriteTrace $errmsg 1001
###		return -1
###	}
###        
###	if [catch {set DEC_RESULT_VAL1 [lindex [aidb_fetch $handle] 0]} errmsg ] {
###		WriteTrace $errmsg 1002
###		return -1
###	}
###        
###	aidb_commit $conn
###	aidb_close $handle
###	#------ --本月GSM语音业务月使用的总费用合计
###	set handle [aidb_open $conn]
###        
###	set sql_buff "select 
###                        sum(bigint(favoured_call_fee))
###                        from bass1.G_S_21003_TO_DAY
###                        where time_id/100=${op_month}"
###        
###	 if [catch { aidb_sql $handle $sql_buff  } errmsg ] {
###		WriteTrace $errmsg 1003
###		return -1
###	}
###        
###	if [catch {set DEC_RESULT_VAL2 [lindex [aidb_fetch $handle] 0]} errmsg ] {
###		WriteTrace $errmsg 1004
###		return -1
###	}
###        
###	aidb_commit $conn
###	aidb_close $handle
###	#--本地校验结果值
###	set DEC_TARGET_VAL1 [format "%.2f" [expr (${DEC_RESULT_VAL1} /1.00000/ ${DEC_RESULT_VAL2} - 1 )]]
###	if {$DEC_TARGET_VAL1 < 0 }  {
###		set DEC_TARGET_VAL1 [format "%.2f" [expr $DEC_TARGET_VAL1 * (-1)]]
###	}
###	#三级告警值
###	set DEC_TARGET_VAL2 0.005
###			
###	#--将校验值插入校验结果表
###	set handle [aidb_open $conn]
###	set sql_buff "\
###		INSERT INTO BASS1.G_RULE_CHECK VALUES ($Timestamp,'I1',$DEC_RESULT_VAL1,$DEC_RESULT_VAL2,$DEC_TARGET_VAL1,0) "
###	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
###		WriteTrace "$errmsg" 2005
###		aidb_close $handle
###		return -1
###	}
###	aidb_commit $conn
###	aidb_close $handle
###	
###	#--判断
###	#--异常
###	#--|∑(本月每日GSM语音业务日使用的总费用合计)/本月GSM语音业务月使用的总费用合计 - 1|≤1％超标
###	
###		
###	if {$DEC_TARGET_VAL1>0.01} {
###		set grade 2
###	        set alarmcontent "准确性指标I1超出集团考核范围"
###	        WriteAlarm $app_name $p_optime $grade ${alarmcontent}
###	} elseif {${DEC_TARGET_VAL1} >= ${DEC_TARGET_VAL2}} {
###		set grade 3
###	        set alarmcontent "准确性指标I1接近集团考核范围1%,达到${DEC_TARGET_VAL1}"
###	        WriteAlarm $app_name $p_optime $grade ${alarmcontent}
###	       		
###	}
###	puts "I1结束"
###	
###        
	return 0
}
