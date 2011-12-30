#################################################################
# 程序名称: INT_CHECK_SAMPLE_TO_DAY.tcl
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
#规则编号：87,89,91,92,93,H5,H6
#规则属性：交叉验证
#规则类型：月
#指标摘要：87：人均通话费
#          89：人均网间通话时长
#          91：人均通话时长
#          92：人均漫游通话次数
#          93：基本通话费单价
#          H5：人均计费时长
#          H6：人均长途计费时长
#规则描述：87：|抽样详单人均通话费/汇总数据人均通话费 - 1| ≤ 5%，且人均通话费＞0
#          89：|抽样详单人均网间通话时长/汇总数据人均网间通话时长 - 1| ≤ 5％，且人均网间通话时长＞0
#          91：|抽样详单人均通话时长/汇总数据人均通话时长 - 1| ≤ 5％，且人均通话时长＞0
#          92：|抽样详单人均漫游通话次数/汇总数据人均漫游通话次数 - 1| ≤ 5％，且人均漫游通话次数＞0
#          93：|抽样详单基本通话费单价－汇总数据基本通话费单价| ≤ 0.03，且基本通话费单价＞0（元/分钟）
#          H5：|抽样详单人均计费时长/汇总数据人均计费时长 - 1| ≤ 5%，且人均计费时长＞0
#          H6：|抽样详单人均长途计费时长/汇总数据人均长途计费时长 - 1| ≤ 5％，且人均长途计费时长＞0
#校验对象：
#          BASS1.G_S_04008_DAY     抽样GSM普通语音话单
#          BASS1.G_S_04009_DAY     抽样智能网语音话单
#          BASS1.G_S_21003_TO_DAY  GSM普通语音业务月使用(日使用)
#          BASS1.G_S_21006_TO_DAY  智能网语音业务月使用(日使用)
#输出参数:
# 返回值:   0 成功; -1 失败
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
        set app_name "INT_CHECK_SAMPLE_TO_DAY.tcl"


#删除本期数据
        set handle [aidb_open $conn]
	set sql_buff "
		DELETE FROM $db_user.G_RULE_CHECK WHERE TIME_ID=$p_timestamp
    	              AND RULE_CODE IN ('87','89','91','92','93','H5','H6') "
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "Error: SQL query failed! The error message is:\n	$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
        
#################################87:人均通话费#########################
        if {$today_dd>9 } {
#抽样详单人均通话费
	     set handle [aidb_open $conn]
	     set sqlbuf "
                    SELECT 
            	      SUM(T.JB+T.CT) AS FY,
            	      COUNT(DISTINCT T.PRODUCT_NO) AS RS
                    FROM (
                           SELECT PRODUCT_NO,SUM(BIGINT(BASE_CALL_FEE)) AS JB,
                           		 SUM(BIGINT(TOLL_CALL_FEE)) AS CT  
                           FROM BASS1.G_S_04008_DAY
                           WHERE TIME_ID/100=$this_month
                           GROUP BY PRODUCT_NO
                           UNION 
                           SELECT PRODUCT_NO,SUM(BIGINT(BASE_BILL_DURATION)) AS JB,SUM(BIGINT(TOLL_BILL_DURATION))AS CT
                           FROM BASS1.G_S_04009_DAY
                           WHERE TIME_ID/100=$this_month
                           GROUP BY PRODUCT_NO
                         )T"

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
	      
	     set RESULT_VAL1 [format "%.5f" [expr (${CHECK_VAL1}/100.00000/${CHECK_VAL2})]]
	     puts $RESULT_VAL1  
	     
#数据清零
             set CHECK_VAL1 0.00
             set CHECK_VAL2 0.00      	

#汇总数据
	     set handle [aidb_open $conn]
	     set sqlbuf "
                    SELECT 
            	      SUM(T.JB+T.CT) AS FY,
            	      COUNT(DISTINCT T.PRODUCT_NO) AS RS
                   FROM (
                         SELECT PRODUCT_NO,SUM(BIGINT(FAVOURED_BASECALL_FEE)) AS JB,SUM(BIGINT(FAVOURED_TOLLCALL_FEE)) AS CT  
                         FROM BASS1.G_S_21003_TO_DAY
                         WHERE TIME_ID/100=$this_month
                         GROUP BY PRODUCT_NO
                         UNION 
                         SELECT PRODUCT_NO,SUM(BIGINT(FAVOURED_BASECALL_FEE)) AS JB,SUM(BIGINT(FAVOURED_TOLLCALL_FEE))AS CT
                         FROM BASS1.G_S_21006_TO_DAY
                         WHERE TIME_ID/100=$this_month
                         GROUP BY PRODUCT_NO
                         ) T"    

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

	     set RESULT_VAL2 [format "%.5f" [expr (${CHECK_VAL1}/100.00000/${CHECK_VAL2})]]
	     puts $RESULT_VAL2  
	     
#数据清零
             set CHECK_VAL1 0.00
             set CHECK_VAL2 0.00   	     
             
             set RESULT [format "%.5f" [expr (($RESULT_VAL1/1.0000/$RESULT_VAL2)-1.0000)]]
             puts $RESULT

             #将校验值插入校验结果表
             set handle [aidb_open $conn]
             set sqlbuf "INSERT INTO bass1.G_RULE_CHECK VALUES
             		($p_timestamp ,
             		'87',
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
             
             
             
             #判断87：|抽样详单人均通话费/汇总数据人均通话费 - 1| ≤ 5%，且人均通话费＞0超标
	     if { $RESULT_VAL1==0 || $RESULT_VAL2==0 || ($RESULT>0.05 ||$RESULT<-0.05 ) } {	
	     	set handle [aidb_open $conn]
	     	set sqlbuf "INSERT INTO BASS1.B_ALARM (DEALCODE,PVALUE1,PVALUE2,GRADE,CONTENT,ALARMTIME,FLAG) VALUES
	     	       (
	       		'$app_name',
	       		'$p_optime',
	       		'-',
	       		2,
	       		'扣分性指标87超出集团考核范围',
	       		CURRENT TIMESTAMP,-1
	       		)"
	     	if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
	     		WriteTrace $errmsg 003
	     		return -1
	         }
	        aidb_commit $conn
	        aidb_close $handle	     		
	     } elseif { $RESULT_VAL1==0 || $RESULT_VAL2==0 || ($RESULT>0.04 ||$RESULT<-0.04 ) } {	
	           set handle [aidb_open $conn]
	     	   set sqlbuf "INSERT INTO BASS1.B_ALARM (DEALCODE,PVALUE1,PVALUE2,GRADE,CONTENT,ALARMTIME,FLAG) VALUES
	     	       (
	       		'$app_name',
	       		'$p_optime',
	       		'-',
	       		3,
	       		'扣分性指标87接近集团考核范围',
	       		CURRENT TIMESTAMP,-1
	       		)"
	     	   if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
	     		WriteTrace $errmsg 003
	     		 return -1
	     	    }
	     	   aidb_commit $conn
	           aidb_close $handle	
	    }

##################################89：人均网间通话时长#################
#抽样详单人均网间通话时长   
	     set handle [aidb_open $conn]
	     set sqlbuf "
                    SELECT 
            	     SUM(T.SC),
            	     COUNT(DISTINCT T.PRODUCT_NO)
                   FROM (
                         SELECT SUM(BIGINT(CALL_DURATION)) AS SC, PRODUCT_NO	  
                         FROM bass1.G_S_04008_DAY
                         WHERE TIME_ID/100=$this_month
                              AND ADVERSARY_ID<>'010000'
                         GROUP BY PRODUCT_NO
                         UNION 
                         SELECT SUM(BIGINT(CALL_DURATION)) AS SC,PRODUCT_NO
                         FROM bass1.G_S_04009_DAY
                         WHERE TIME_ID/100=$this_month
                               AND ADVERSARY_ID<>'010000'
                         GROUP BY PRODUCT_NO
                         )T"

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
	      
	     set RESULT_VAL1 [format "%.5f" [expr (${CHECK_VAL1}/1.00000/${CHECK_VAL2})]]
	     puts $RESULT_VAL1  
	     
#数据清零
             set CHECK_VAL1 0.00
             set CHECK_VAL2 0.00      	

#汇总数据
	     set handle [aidb_open $conn]
	     set sqlbuf "
                  SELECT 
            	      SUM(T.SC),
            	      COUNT(DISTINCT T.PRODUCT_NO)
                    FROM (
                          SELECT SUM(BIGINT(CALL_DURATION)) AS SC,PRODUCT_NO
                          FROM BASS1.G_S_21003_TO_DAY
                          WHERE TIME_ID/100=$this_month
                                AND ADVERSARY_ID<>'010000'
                          GROUP BY PRODUCT_NO
                          UNION 
                          SELECT SUM(BIGINT(CALL_DURATION)) AS SC,PRODUCT_NO
                          FROM BASS1.G_S_21006_TO_DAY
                          WHERE TIME_ID/100=$this_month
                                AND ADVERSARY_ID<>'010000'
                          GROUP BY PRODUCT_NO
                          ) T"    

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

	     set RESULT_VAL2 [format "%.5f" [expr (${CHECK_VAL1}/1.00000/${CHECK_VAL2})]]
	     puts $RESULT_VAL2  
	     
#数据清零
             set CHECK_VAL1 0.00
             set CHECK_VAL2 0.00   	     
             
             set RESULT [format "%.5f" [expr (($RESULT_VAL1/1.0000/$RESULT_VAL2)-1.0000)]]
             puts $RESULT

             #将校验值插入校验结果表
             set handle [aidb_open $conn]
             set sqlbuf "INSERT INTO bass1.G_RULE_CHECK VALUES
             		($p_timestamp ,
             		'89',
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
             
             
             
             #判断89：|抽样详单人均网间通话时长/汇总数据人均网间通话时长 - 1| ≤ 5％，且人均网间通话时长＞0超标
	     if { $RESULT_VAL1==0 || $RESULT_VAL2==0 || ($RESULT>0.05 ||$RESULT<-0.05 ) } {	
	     	set handle [aidb_open $conn]
	     	set sqlbuf "INSERT INTO BASS1.B_ALARM (DEALCODE,PVALUE1,PVALUE2,GRADE,CONTENT,ALARMTIME,FLAG) VALUES
	     	       (
	       		'$app_name',
	       		'$p_optime',
	       		'-',
	       		2,
	       		'准确性指标89超出集团考核范围',
	       		CURRENT TIMESTAMP,-1
	       		)"
	     	if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
	     		WriteTrace $errmsg 003
	     		return -1
	         }
	        aidb_commit $conn
	        aidb_close $handle		     		
	     } elseif { $RESULT_VAL1==0 || $RESULT_VAL2==0 || ($RESULT>0.04 ||$RESULT<-0.04 ) } {	
	           set handle [aidb_open $conn]
	     	   set sqlbuf "INSERT INTO BASS1.B_ALARM (DEALCODE,PVALUE1,PVALUE2,GRADE,CONTENT,ALARMTIME,FLAG) VALUES
	     	       (
	       		'$app_name',
	       		'$p_optime',
	       		'-',
	       		3,
	       		'准确性指标89接近集团考核范围',
	       		CURRENT TIMESTAMP,-1
	       		)"
	     	   if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
	     		WriteTrace $errmsg 003
	     		 return -1
	     	    }
	     	   aidb_commit $conn
	           aidb_close $handle
	    }    
	           	
        }
##################################END#######################
	return 0
}	    
