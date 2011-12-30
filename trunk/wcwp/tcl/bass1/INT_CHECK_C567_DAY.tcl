##############################################################
#程序名称: INT_CHECK_C567_DAY.tcl
#功能描述: 
#规则编号：C5,C6,C7
#规则属性：业务逻辑(C7交叉验证)
#规则类型：日
#指标摘要：C5：竞争对手新增用户数
#          C6：我公司用户市场占有率
#          C7：日收入与日语音通话费收入差异率
#规则描述：C5：竞争对手新增用户数＞0
#          C6：我公司用户市场占有率＞50%
#          C7：|日收入/日语音通话费收入 - 1|  ≤ 50%
#校验对象：1.bass1.G_S_22012_DAY    
#          2.bass1.G_S_21001_DAY
#          3.bass1.G_S_21004_DAY
#          4.bass1.G_S_21009_DAY
#输入参数:
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：tym
#编写时间：2007-03-22
#问题记录：1.
#修改历史: 1.22012接口的校验已经合并到22073接口，针对该接口的校验取消  夏华学 20090502
#####################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

#	global env
#
#	set Optime $op_time
#	set p_optime $op_time
#	set Timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
#	set op_month [string range $op_time 0 3][string range $op_time 5 6]
#        append op_time_month ${optime_month}-01
#        set db_user $env(DB_USER)
#        #求上个月 格式 yyyymm
#        set last_month [GetLastMonth [string range $Timestamp 0 5]]
#        #puts $last_month
#        #----求上月最后一天---#,格式 yyyymmdd
#        set last_day_month [GetLastDay [string range $Timestamp 0 5]01]
#        #puts $last_day_month
#        ##--求昨天，格式yyyymmdd--##
#        set yesterday [GetLastDay ${Timestamp}]
#        
#        set app_name "INT_CHECK_C567_DAY.tcl"
#        
#
#        set handle [aidb_open $conn]
#	set sql_buff "\
#		DELETE FROM $db_user.G_RULE_CHECK WHERE TIME_ID=${Timestamp} AND RULE_CODE IN ('C5','C6','C7')"
#	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
#		WriteTrace "$errmsg" 2005
#		aidb_close $handle
#		return -1
#	}
#	aidb_commit $conn
#	aidb_close $handle
#	
#	#----C5：竞争对手新增用户数
#	
#	 set handle [aidb_open $conn]      
#	set sql_buff "SELECT 
#                       SUM(BIGINT(D_UNION_NUSERS)+BIGINT(D_TELE_FIX_NUSERS)+BIGINT(D_TELE_NET_NUSERS)+BIGINT(D_OTH_NUSERS))
#                       FROM bass1.G_S_22012_DAY
#                       WHERE TIME_ID=${Timestamp}
#		       "
#        
#	 if [catch { aidb_sql $handle $sql_buff  } errmsg ] {
#		WriteTrace $errmsg 1001
#		return -1
#	}
#        
#	if [catch {set DEC_RESULT_VAL1 [lindex [aidb_fetch $handle] 0]} errmsg ] {
#		WriteTrace $errmsg 1002
#		return -1
#	}
#        
#	aidb_commit $conn
#	aidb_close $handle
#	
#	set DEC_RESULT_VAL2 "0"
#	
#	#--将校验值插入校验结果表
#	set handle [aidb_open $conn]
#	set sql_buff "\
#		INSERT INTO BASS1.G_RULE_CHECK VALUES ($Timestamp,'C5',$DEC_RESULT_VAL1,$DEC_RESULT_VAL2,0,0) "
#	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
#		WriteTrace "$errmsg" 2005
#		aidb_close $handle
#		return -1
#	}
#	aidb_commit $conn
#	aidb_close $handle
#	
#	#--判断
#        #--异常：
#        #--C5：竞争对手新增用户数＞0超标
#        
#        if {${DEC_RESULT_VAL1}<=0} {
#        	set grade 2
#	        set alarmcontent "扣分性指标C5超出集团考核范围"
#	        WriteAlarm $app_name $p_optime $grade ${alarmcontent}	
#	        
#		}
#		
#	#--C6：我公司用户市场占有率
#        #--移动当日通信用户数
#        set handle [aidb_open $conn]        
#	set sql_buff "SELECT 
#                       SUM(BIGINT(D_COMM_USERS))
#                       FROM bass1.G_S_22038_DAY
#                       WHERE TIME_ID=${Timestamp}"
#		        
#	 if [catch { aidb_sql $handle $sql_buff  } errmsg ] {
#		WriteTrace $errmsg 1001
#		return -1
#	}
#        
#	if [catch {set DEC_RESULT_VAL1 [lindex [aidb_fetch $handle] 0]} errmsg ] {
#		WriteTrace $errmsg 1002
#		return -1
#	}
#        
#	aidb_commit $conn
#	aidb_close $handle
#	
#	#--移动当日通信用户数与其它运营商用户数合计值
#	set handle [aidb_open $conn]
#	set sql_buff "\
#            SELECT
#	      SUM(T.USERS)
#	    FROM
#	      (
#	          SELECT 
#                    SUM(BIGINT(D_COMM_USERS)) AS  USERS
#                  FROM 
#                    bass1.G_S_22038_DAY
#                  WHERE 
#                    TIME_ID=${Timestamp}
#                  UNION  ALL 
#                  SELECT
#                    BIGINT(D_UNION_USERS)+ BIGINT(D_TELE_FIX_USERS)+
#                    BIGINT(D_TELE_NET_USERS)+BIGINT(D_TELE_OTH_USERS) AS USERS
#                  FROM bass1.G_S_22012_DAY
#                  WHERE TIME_ID=${Timestamp}
#	      )T "        
#	 if [catch { aidb_sql $handle $sql_buff  } errmsg ] {
#		WriteTrace $errmsg 1001
#		return -1
#	}
#        
#	if [catch {set DEC_RESULT_VAL2 [lindex [aidb_fetch $handle] 0]} errmsg ] {
#		WriteTrace $errmsg 1002
#		return -1
#	}       
#	aidb_commit $conn
#	aidb_close $handle
#	
#	#--将校验值插入校验结果表
#	set handle [aidb_open $conn]
#	set sql_buff "\
#		INSERT INTO BASS1.G_RULE_CHECK VALUES ($Timestamp,'C6',$DEC_RESULT_VAL1,$DEC_RESULT_VAL2,0,0) "
#	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
#		WriteTrace "$errmsg" 2005
#		aidb_close $handle
#		return -1
#	}
#	aidb_commit $conn
#	aidb_close $handle
#	
#	#--判断
#        #--异常：
#        #--C6：我公司用户市场占有率＞50%超标	
#	set RESULT_VAL2 [format "%.2f" [expr ${DEC_RESULT_VAL1} /1.00/${DEC_RESULT_VAL2}]]
#	if {${RESULT_VAL2}<=0.05} {
#		set grade 2
#	        set alarmcontent "扣分性指标C6：我公司用户市场占有率超出集团考核范围"
#	        WriteAlarm $app_name $p_optime $grade ${alarmcontent}	
#	        }
#		
#	#--C7：日收入与日语音通话费收入差异率
#        #--从22012日KPI接口中取出当日收入
#        set handle [aidb_open $conn]     
#	set sql_buff "SELECT 
#                        SUM(BIGINT(INCOME))
#                        FROM bass1.G_S_22038_DAY
#                        WHERE TIME_ID=${Timestamp}
#		       "
#        if [catch { aidb_sql $handle $sql_buff  } errmsg ] {
#		WriteTrace $errmsg 1001
#		return -1
#	}
#        
#	if [catch {set DEC_RESULT_VAL1 [lindex [aidb_fetch $handle] 0]} errmsg ] {
#		WriteTrace $errmsg 1002
#		return -1
#	}
#        
#	aidb_commit $conn
#	aidb_close $handle		       	
	



















#以前修改的
#	#--日语音通话费收入
#	set handle [aidb_open $conn]      
#	set sql_buff "SELECT SUM(T.FEE)
#                      FROM
#                      (
#    	                SELECT 
#                          SUM(BIGINT(FAVOURED_CALL_FEE)) as fee
#                        FROM bass1.G_S_21001_DAY
#                        WHERE TIME_ID=${Timestamp} 
#                        union all
#    	                SELECT 
#                          SUM(BIGINT(FAVOURED_CALL_FEE)) as fee
#                        FROM bass1.G_S_21004_DAY
#                        WHERE TIME_ID=${Timestamp} 
#                        union all
#                        SELECT 
#                          SUM(BIGINT(FAVOURED_CALL_FEE)) as fee
#                        FROM bass1.G_S_21009_DAY
#                        WHERE TIME_ID=${Timestamp} 
#                      ) T "
#		        
#	 if [catch { aidb_sql $handle $sql_buff  } errmsg ] {
#		WriteTrace $errmsg 1001
#		return -1
#	}
#        
#	if [catch {set DEC_RESULT_VAL2 [lindex [aidb_fetch $handle] 0]} errmsg ] {
#		WriteTrace $errmsg 1002
#		return -1
#	}
#        
#	aidb_commit $conn
#	aidb_close $handle
#	
#	#--将校验值插入校验结果表
#	set handle [aidb_open $conn]
#	set sql_buff "\
#		INSERT INTO BASS1.G_RULE_CHECK VALUES ($Timestamp,'C7',$DEC_RESULT_VAL1,$DEC_RESULT_VAL2,0,0) "
#	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
#		WriteTrace "$errmsg" 2005
#		aidb_close $handle
#		return -1
#	}
#	aidb_commit $conn
#	aidb_close $handle
#	
#	#--判断
#        #--异常：
#        #--C7：|日收入/日语音通话费收入 - 1|  ≤ 30%超标	
#	
#	
#	set RESULT_VAL2 [format "%.2f" [expr (${DEC_RESULT_VAL1} /1.00/${DEC_RESULT_VAL2} - 1)]]
#	if {${RESULT_VAL2}>0.50 ||${RESULT_VAL2}<-0.50} {
#		set grade 2
#	        set alarmcontent "扣分性指标C7：日收入与日语音通话费收入差异率超出集团考核范围"
#	        WriteAlarm $app_name $p_optime $grade ${alarmcontent}	
#	        }

	return 0
}
