#################################################################
#程序名称: INT_CHECK_A0L694B5_DAY.tcl
#功能描述: 生成A0L694B5的校验程序
#规则编号：A0,L6,94(月校验),B5
#规则属性：业务逻辑
#规则类型：日
#指标摘要：A0：彩信通信费单价
#          L6：点对点彩信单价
#          94: 彩信话单逻辑检查
#          B5: 手机报用户数>0
#规则描述：A0：彩信平均通信费单价≤0.9元
#          L6：0.10 ≤ 本月点对点彩信通信费合计/本月点对点彩信计费量 ≤ 0.60（元/条）
#          94: 彩信话单业务类型与应用类型不匹配
#          B5: 手机报用户数＞0
#校验对象：1.BASS1.G_S_04004_DAY  彩信话单
#输入参数:
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：王琦
#编写时间：2007-03-22
#问题记录：1.20070822 修改手机报付费用户数，夏华学
#修改历史: 1.
#####################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

        #当天 yyyymmdd
        set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
        #当天 yyyy-mm-dd
        set optime $op_time
        #今天的日期，格式dd(例：输入20070411 返回11)
        set today_dd [string range $op_time 8 9]      
        #本月 yyyymm
        set op_month [string range $op_time 0 3][string range $op_time 5 6]
        #程序名称
        set app_name "INT_CHECK_A0L694B5_DAY.tcl"

        #删除本期数据
        set handle [aidb_open $conn]
	set sql_buff "delete from bass1.g_rule_check where time_id=${timestamp} 
		        and rule_code in ('A0','L6','94','B5') "
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
        aidb_close $handle
                

###################################A0：彩信通信费单价##########################
	set handle [aidb_open $conn]
	set sqlbuf "
                SELECT
              	  SUM(BIGINT(CALL_FEE)),
              	  COUNT(*)
                FROM 
                  bass1.G_S_04004_DAY
                WHERE
                  TIME_ID=$timestamp
                  AND (  (APPLCN_TYPE='0' AND MM_BILL_TYPE='00')
                       OR (APPLCN_TYPE IN ('1','2','3','4')  )   ) "
        puts $sql_buff
        if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
        	WriteTrace $errmsg 1001
        	return -1
        }

        while {[set p_success [aidb_fetch $handle]] != "" } {
                set CHECK_VAL1 	      [lindex $p_success 0]
                set CHECK_VAL2        [lindex $p_success 1]
        }

#       puts $CHECK_VAL1
#       puts $CHECK_VAL2
#
     set RESULT_VAL1 [format "%.2f" [expr ${CHECK_VAL1} /100.00]]
     set RESULT_VAL2 [format "%.2f" [expr ${CHECK_VAL2} /1.00]]

#
#    puts $RESULT_VAL1
#    puts $RESULT_VAL2

    #将校验值插入校验结果表
	set handle [aidb_open $conn]
	set sqlbuf "INSERT INTO bass1.G_RULE_CHECK VALUES
			($timestamp ,
			'A0',
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
	
	#判断
	if { $RESULT_VAL1==0.00 || $RESULT_VAL2==0.00 || [format "%.2f" [expr ${RESULT_VAL1} / ${RESULT_VAL2}]]>0.90 } {		
		set grade 2
	        set alarmcontent "准确性指标A0超出集团考核范围"
	        WriteAlarm $app_name $optime $grade ${alarmcontent}		
            }
           puts "A0结束" 
           
##################################94: 彩信话单逻辑检查#################
#           #--彩信话单接口中取出入库日期是当月，
#           #--且短信话单类型是(1,11)下行，通信费不等零的记录数
#            set handle [aidb_open $conn]
#	    set sqlbuf "\
#	           SELECT
#                     COUNT(*)
#	           FROM 
#	             bass1.G_S_04004_DAY
#	           WHERE 
#	             TIME_ID/100=$op_month
#		     AND BUS_SRV_ID IN ('1','4')
#		     AND APPLCN_TYPE IN ('1','2','3','4')"
#
#	 if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
#		WriteTrace $errmsg 1001
#		return -1
#	}
#	if [catch {set DEC_RESULT_VAL1 [lindex [aidb_fetch $handle] 0]} errmsg ] {
#		WriteTrace $errmsg 1002
#		return -1
#	}
#	aidb_commit $conn
#	aidb_close $handle
#	
#	set DEC_RESULT_VAL2 "0.0"
#	
#	#将校验值插入校验结果表
#	set handle [aidb_open $conn]
#	set sqlbuf "INSERT INTO bass1.G_RULE_CHECK VALUES
#			($timestamp ,
#			'94',
#			${DEC_RESULT_VAL1},
#			${DEC_RESULT_VAL2},
#			0,
#			0)"
#	if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
#		WriteTrace $errmsg 003
#		return -1
#	}
#	aidb_commit $conn
#	aidb_close $handle
#	
#	#判断
#        #1：计算彩信话单中业务类型是（1国内点对点彩信，4国际点对点彩信）时，
#        #彩信应用类型为1— Email(MM3接口的外接应用)到终端、2— 终端到Email(MM3接口的外接应用)、
#        #3— VAS应用到终端、4— 终端到VAS应用的话单数，当话单数大于零，则违反此规则。
#	if {${DEC_RESULT_VAL1}>0} {
#		
#		set grade 2
#	        set alarmcontent "准确性指标94超出集团考核范围"
#	        WriteAlarm $app_name $optime $grade ${alarmcontent}	
#		
#            }
#           puts "94结束"            
#####################################################################################################
           if { $today_dd <= 19 } {
        	    	puts "今天 $today_dd 号，未到19号，暂不处理"
        	    	return 0
        	}
##################################L6：点对点彩信单价###########################        	
           set handle [aidb_open $conn]
	   set sqlbuf "\
                SELECT 
            	  SUM(BIGINT(CALL_FEE)) AS TXF,
            	  COUNT(*) AS TXL
                FROM 
                  bass1.G_S_04004_DAY
                WHERE
                  TIME_ID/100=$op_month 
                  AND APPLCN_TYPE='0' 
                  AND MM_BILL_TYPE='00'
                  AND BUS_SRV_ID IN ('1','4') "      
	   if [catch {aidb_sql $handle $sqlbuf} errmsg] {
	   	WriteTrace $errmsg 001
	   	return -1
	   }
	   while { [set p_row [aidb_fetch $handle]] != "" } {
	   	set DEC_CHECK_VALUE_1 [lindex $p_row 0]
	   	set DEC_CHECK_VALUE_2 [lindex $p_row 1]
	   }
	   aidb_commit $conn
	   aidb_close $handle
	   
	   set DEC_CHECK_VALUE_1 [format "%.5f" [expr ${DEC_CHECK_VALUE_1} /100.00]]
	   set DEC_CHECK_VALUE_2
	   #将校验值插入校验结果表
           set handle [aidb_open $conn]
           set sqlbuf "INSERT INTO bass1.G_RULE_CHECK VALUES
             		($timestamp ,
             		'L6',
             		$DEC_CHECK_VALUE_1,
             		$DEC_CHECK_VALUE_2,
             		0,
             		0)"
           if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
             	WriteTrace $errmsg 003
             	return -1
           }
           aidb_commit $conn
           aidb_close $handle
           
           set RESULT [format "%.2f" [expr ($DEC_CHECK_VALUE_1/$DEC_CHECK_VALUE_2)]]
           puts $RESULT
           #puts $RESULT
           
           #--判断
           #--异常
           #----1：点对点彩信通信量=0,异常
           #--2：0.10<=本月点对点彩信通信费合计/本月点对点彩信计费量<=0.60（元/条）
	   if { $DEC_CHECK_VALUE_2==0 } {
	     	set grade 2
	        set alarmcontent "准确性指标L6:点对点彩信通信量=0,异常"
	        WriteAlarm $app_name $optime $grade ${alarmcontent}			     		     			  
	   } elseif {$RESULT<0.10 || $RESULT>0.60} {
		set grade 2
	        set alarmcontent "准确性指标L6超出集团考核范围"
	        WriteAlarm $app_name $optime $grade ${alarmcontent}	
	         }
	    puts "L6结束"
	    
############################B5: 手机报用户数>0############################
            #--手机报付费用户数
            #--注意：SP业务代码会随时调整
            set handle [aidb_open $conn]
#	    set sqlbuf "\
#                     SELECT 
#                       COUNT(DISTINCT PRODUCT_NO )
#		     FROM 
#		       bass1.G_S_04004_DAY
#		     WHERE 
#		       TIME_ID/100=$op_month
#		         AND BIGINT(INFO_FEE)>0
#		         AND SP_ENT_CODE='819234'
#		         AND BUS_CODE IN (
#      '110301', '110302', '110303', '110304', '110305', '110306', '110311', '110321', '110322', '110323', '110325', '110326', '110327', '110332', '110339', '110340',
#      '110349', '110361', '110362', '112301', '112304', '112305', '112306', '112307', '112308', '112309', '112310', '112311', '112312', '112314', '112315', '112316',
#      '112317', '112318', '112319', '112327', '112328', '112329', '112330', '112331', '112332', '112333', '112334', '112345', '112346', '112347', '112348', '112349',
#      '112350', '133302')"
    set sqlbuf "select  COUNT(DISTINCT PRODUCT_NO ) 
                   from bass1.G_S_04004_DAY
		              WHERE TIME_ID/100=$op_month  AND 
		                    sp_ent_code = '801234' AND 
		                    
		                    BUS_SRV_ID in ('1','2','3')  "
#svc_code='7000'        AND    20080131 夏华学修改  原因：手机报口径变化

	 if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
		WriteTrace $errmsg 1001
		return -1
	}
	if [catch {set DEC_RESULT_VAL1 [lindex [aidb_fetch $handle] 0]} errmsg ] {
		WriteTrace $errmsg 1002
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
	
	set DEC_RESULT_VAL2 "0.0"
	
	#将校验值插入校验结果表
	set handle [aidb_open $conn]
	set sqlbuf "INSERT INTO bass1.G_RULE_CHECK VALUES
			($timestamp ,
			'B5',
			${DEC_RESULT_VAL1},
			${DEC_RESULT_VAL2},
			0,
			0)"
	if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
		WriteTrace $errmsg 003
		return -1
	}
	aidb_commit $conn
	aidb_close $handle	
	
	#判断
	if {${DEC_RESULT_VAL1}<=0} {
		set grade 2
	        set alarmcontent "扣分性指标B5超出集团考核范围"
	        WriteAlarm $app_name $optime $grade ${alarmcontent}	
	}
       puts "B5结束" 
           
      return 0
}