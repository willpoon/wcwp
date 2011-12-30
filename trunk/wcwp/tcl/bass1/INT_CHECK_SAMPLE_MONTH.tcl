#################################################################
# 程序名称: INT_CHECK_SAMPLE_MONTH.tcl
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
#          GBAS.G_S_04008_DAY     --抽样GSM普通语音话单             
#          GBAS.G_S_04009_DAY     --抽样智能网语音话单              
#          GBAS.G_S_21003_MONTH  --GSM普通语音业务月使用
#          GBAS.G_S_21006_MONTH  --智能网语音业务月使用

#输出参数:
# 返回值:   0 成功; -1 失败
##
##|抽样详单人均通话费/汇总数据人均通话费 - 1| ≤ 5%，且人均通话费＞0
##|抽样详单人均计费时长/汇总数据人均计费时长 - 1| ≤ 5%，且人均计费时长＞0
##|抽样详单人均点对点短信计费量/汇总数据人均点对点短信计费量 - 1| ≤ 8%，且计费量＞0
##
#################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

	set p_optime $op_time
	set p_timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
	set this_month [string range $op_time 0 3][string range $op_time 5 6]

	#本月 yyyymm
	set op_month [string range $optime_month 0 3][string range $optime_month 5 6]
	#本月 yyyy-mm
	set opmonth $optime_month	     

        #程序名称
        set app_name "INT_CHECK_SAMPLE_MONTH.tcl"


#删除本期数据
        set handle [aidb_open $conn]
	set sql_buff "
		DELETE FROM bass1.G_RULE_CHECK WHERE TIME_ID=$op_month
    	              AND RULE_CODE IN ('87','89','91','92','93','H5','H6') "
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle

###################################87：人均通话费##########################
        #抽样详单人均通话费
	set handle [aidb_open $conn]
	set sqlbuf "\
                      SELECT 
            	        SUM(T.JB+T.CT+T.CF) AS FY,
            	        COUNT(DISTINCT T.PRODUCT_NO) AS RS
                      FROM (
                             SELECT 
                               PRODUCT_NO,
                               SUM(BIGINT(BASE_CALL_FEE)) AS JB,
                               SUM(BIGINT(TOLL_CALL_FEE)) AS CT,  
                               SUM(BIGINT(callfw_toll_fee)) AS CF
                             FROM 
                               BASS1.G_S_04008_DAY
                             WHERE TIME_ID/100=$op_month and roam_type_id not in ('122','202','302','401')
                             GROUP BY PRODUCT_NO
                             UNION ALL
                             SELECT 
                               PRODUCT_NO,
                               SUM(BIGINT(BASE_CALL_FEE)) AS JB,
                               SUM(BIGINT(TOLL_CALL_FEE)) AS CT,  
                               SUM(BIGINT(0)) AS CF
                             FROM 
                               BASS1.G_S_04009_DAY
                             WHERE TIME_ID/100=$op_month
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
	      
	     set RESULT_VAL1 [format "%.2f" [expr (${CHECK_VAL1}/100.00000/${CHECK_VAL2})]]
	     puts $RESULT_VAL1
	     
             #数据清零
             set CHECK_VAL1 0.00
             set CHECK_VAL2 0.00
             	    
             #汇总数据
	     set handle [aidb_open $conn]
	     set sqlbuf "\	
                   SELECT 
                     SUM(T.JB+T.CT) AS FY,
                     COUNT(DISTINCT T.PRODUCT_NO) AS RS
                   FROM (
                         SELECT PRODUCT_NO,SUM(BIGINT(FAVOURED_BASECALL_FEE)) AS JB,SUM(BIGINT(FAVOURED_TOLLCALL_FEE)) AS CT  
                         FROM BASS1.G_S_21003_MONTH
                         WHERE TIME_ID=$op_month
                         GROUP BY PRODUCT_NO
                         UNION 
                         SELECT PRODUCT_NO,SUM(BIGINT(FAVOURED_BASECALL_FEE)) AS JB,SUM(BIGINT(FAVOURED_TOLLCALL_FEE))AS CT
                         FROM BASS1.G_S_21006_MONTH
                         WHERE TIME_ID=$op_month
                         GROUP BY PRODUCT_NO
                       ) T"
   puts $sqlbuf
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
	      
	     set RESULT_VAL2 [format "%.2f" [expr (${CHECK_VAL1}/100.00000/${CHECK_VAL2})]]
	     puts $RESULT_VAL2

             #(抽样值/汇总值-1)     
#	     set RESULT [format "%.2f" [expr ($RESULT_VAL1/1.00/$RESULT_VAL2-1.00)]]
	     set RESULT [format "%.5f" [expr (${RESULT_VAL1}/1.00000/${RESULT_VAL2}-1)]]
	     puts $RESULT
	     
             #将校验值插入校验结果表
             set handle [aidb_open $conn]
             set sqlbuf "INSERT INTO bass1.G_RULE_CHECK VALUES
             		($op_month ,
             		'87',
             		cast ($RESULT_VAL1 as  DECIMAL(18, 5) ),
             		cast ($RESULT_VAL2 as  DECIMAL(18, 5) ),
             		cast ($RESULT as  DECIMAL(18, 5) ),
             		0.05)"
   puts $sqlbuf			
             if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
             	WriteTrace $errmsg 003
             	return -1
             }
             aidb_commit $conn
             aidb_close $handle
             
             
             
             #判断87：|抽样详单人均通话费/汇总数据人均通话费 - 1| ≤ 5%，且人均通话费＞0超标
	     if { $RESULT_VAL1<=0 || $RESULT_VAL2<=0 || ($RESULT>0.05 ||$RESULT<-0.05 ) } {
	     	set grade 2
	        set alarmcontent "扣分性指标87超出集团考核范围"
	        WriteAlarm $app_name $opmonth $grade ${alarmcontent}		
	     	     		
	     } elseif { ($RESULT>0.04 ||$RESULT<-0.04 ) } {	
	           set grade 3
	           set alarmcontent "扣分性指标87接近集团考核范围5%,达到${RESULT_VAL1}"
	           WriteAlarm $app_name $opmonth $grade ${alarmcontent}
	          
	    }                 
#################################89：人均网间通话时长#############
        #抽样详单人均网间通话时长
#20110805 去掉：
#                          UNION 
#                          SELECT SUM(BIGINT(CALL_DURATION)) AS SC,PRODUCT_NO
#                          FROM BASS1.G_S_04009_DAY
#                          WHERE TIME_ID/100=$op_month
#                                AND ADVERSARY_ID<>'010000'
#                          GROUP BY PRODUCT_NO
			  
	set handle [aidb_open $conn]
	set sqlbuf "\
                  SELECT 
            	    SUM(T.SC),
            	    COUNT(DISTINCT T.PRODUCT_NO)
                  FROM (
                          SELECT SUM(BIGINT(CALL_DURATION)) AS SC, PRODUCT_NO	  
                          FROM BASS1.G_S_04008_DAY
                          WHERE TIME_ID/100=$op_month
                               AND ADVERSARY_ID<>'010000'
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

	      }
	      aidb_commit $conn
	      aidb_close $handle
	      
	     set RESULT_VAL1 [format "%.2f" [expr (${CHECK_VAL1}/1.00000/${CHECK_VAL2})]]
	     puts $RESULT_VAL1
	     
             #数据清零
             set CHECK_VAL1 0.00
             set CHECK_VAL2 0.00
             	    
             #汇总数据人均网间通话时长
	     set handle [aidb_open $conn]
	     set sqlbuf "\	
                  SELECT 
            	    SUM(T.SC),
            	    COUNT(DISTINCT T.PRODUCT_NO)
                  FROM (
                          SELECT SUM(BIGINT(CALL_DURATION)) AS SC,PRODUCT_NO
                          FROM bass1.G_S_21003_MONTH
                          WHERE TIME_ID=$op_month
                                AND ADVERSARY_ID<>'010000'
                          GROUP BY PRODUCT_NO
                          UNION 
                          SELECT SUM(BIGINT(CALL_DURATION)) AS SC,PRODUCT_NO
                          FROM bass1.G_S_21006_MONTH
                          WHERE TIME_ID=$op_month
                                AND ADVERSARY_ID<>'010000'
                          GROUP BY PRODUCT_NO
                        ) T"
   puts $sqlbuf
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
	      
	     set RESULT_VAL2 [format "%.2f" [expr (${CHECK_VAL1}/1.00000/${CHECK_VAL2})]]
	     puts $RESULT_VAL2

             #(抽样值/汇总值-1)     
#	     set RESULT [format "%.2f" [expr ($RESULT_VAL1/1.00/$RESULT_VAL2-1.00)]]
	     set RESULT [format "%.5f" [expr (${RESULT_VAL1}/1.00000/${RESULT_VAL2}-1)]]
	     puts $RESULT
	     
             #将校验值插入校验结果表
             set handle [aidb_open $conn]
             set sqlbuf "INSERT INTO bass1.G_RULE_CHECK VALUES
             		($op_month ,
             		'89',
             		cast ($RESULT_VAL1 as  DECIMAL(18, 5) ),
             		cast ($RESULT_VAL2 as  DECIMAL(18, 5) ),
             		cast ($RESULT as  DECIMAL(18, 5) ),
             		0.05)"
   puts $sqlbuf			
             if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
             	WriteTrace $errmsg 003
             	return -1
             }
             aidb_commit $conn
             aidb_close $handle
             
             
             
             #判断89：|抽样详单人均网间通话时长/汇总数据人均网间通话时长 - 1| ≤ 5％，且人均网间通话时长＞0超标
	     if { $RESULT_VAL1<=0 || $RESULT_VAL2<=0 || ($RESULT>0.05 ||$RESULT<-0.05 ) } {
	     	set grade 2
	        set alarmcontent "准确性指标89超出集团考核范围"
	        WriteAlarm $app_name $opmonth $grade ${alarmcontent}		
	     	     		
	     } elseif { ($RESULT>0.04 ||$RESULT<-0.04 ) } {	
	           set grade 3
	           set alarmcontent "准确性指标89接近集团考核范围5%,达到${RESULT}"
	           WriteAlarm $app_name $opmonth $grade ${alarmcontent}
	          
	    } 	    
################################91：人均通话时长###############
        #抽样详单人均通话时长
	set handle [aidb_open $conn]
	set sqlbuf "\
            SELECT 
            	SUM(T.SC),
            	COUNT(DISTINCT T.PRODUCT_NO)
            FROM (
                  SELECT SUM(BIGINT(CALL_DURATION)) AS SC,PRODUCT_NO	  
                  FROM BASS1.G_S_04008_DAY
                  WHERE TIME_ID/100=$op_month  and roam_type_id not in ('122','202','302','401')  
                  GROUP BY PRODUCT_NO
                  UNION 
                  SELECT SUM(BIGINT(CALL_DURATION)) AS SC,PRODUCT_NO
                  FROM BASS1.G_S_04009_DAY
                  WHERE TIME_ID/100=$op_month
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

	      }
	      aidb_commit $conn
	      aidb_close $handle
	      
	     set RESULT_VAL1 [format "%.2f" [expr (${CHECK_VAL1}/1.00000/${CHECK_VAL2})]]
	     puts $RESULT_VAL1
	     
             #数据清零
             set CHECK_VAL1 0.00
             set CHECK_VAL2 0.00
             	    
             #汇总数据人均通话时长
	     set handle [aidb_open $conn]
	     set sqlbuf "\
	            SELECT 
	            	SUM(T.SC),
	            	COUNT(DISTINCT T.PRODUCT_NO)
	            FROM (
	                  SELECT SUM(BIGINT(CALL_DURATION)) AS SC,PRODUCT_NO
	                  FROM BASS1.G_S_21003_MONTH
	                  WHERE TIME_ID=$op_month
	                  GROUP BY PRODUCT_NO
	                  UNION 
	                  SELECT SUM(BIGINT(CALL_DURATION)) AS SC,PRODUCT_NO
	                  FROM BASS1.G_S_21006_MONTH
	                  WHERE TIME_ID=$op_month
	                  GROUP BY PRODUCT_NO
	                  ) T	  "
   puts $sqlbuf
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
	      
	     set RESULT_VAL2 [format "%.2f" [expr (${CHECK_VAL1}/1.00000/${CHECK_VAL2})]]
	     puts $RESULT_VAL2

             #(抽样值/汇总值-1)     
#	     set RESULT [format "%.2f" [expr ($RESULT_VAL1/1.00/$RESULT_VAL2-1.00)]]
	     set RESULT [format "%.5f" [expr (${RESULT_VAL1}/1.00000/${RESULT_VAL2}-1)]]
	     puts $RESULT
	     
             #将校验值插入校验结果表
             set handle [aidb_open $conn]
             set sqlbuf "INSERT INTO bass1.G_RULE_CHECK VALUES
             		($op_month ,
             		'91',
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
             
             
             
             #判断91：|抽样详单人均通话时长/汇总数据人均通话时长 - 1| ≤ 5％，且人均通话时长＞0超标
	     if { $RESULT_VAL1<=0 || $RESULT_VAL2<=0 || ($RESULT>0.05 ||$RESULT<-0.05 ) } {
	     	set grade 2
	        set alarmcontent "准确性指标91超出集团考核范围"
	        WriteAlarm $app_name $opmonth $grade ${alarmcontent}		
	     	     		
	     } elseif { ($RESULT>0.04 ||$RESULT<-0.04 ) } {	
	           set grade 3
	           set alarmcontent "准确性指标91接近集团考核范围5%,达到${RESULT_VAL1}"
	           WriteAlarm $app_name $opmonth $grade ${alarmcontent}
	          
	    } 	    
############################92：人均漫游通话次数#####################
        #抽样详单人均漫游通话次数
	set handle [aidb_open $conn]
	set sqlbuf "\
            SELECT 
            	SUM(T.CS),
            	COUNT(DISTINCT T.PRODUCT_NO)
            FROM (
                  SELECT COUNT(*) AS CS , PRODUCT_NO		  
                  FROM BASS1.G_S_04008_DAY
                  WHERE TIME_ID/100=$op_month
                        AND ROAM_TYPE_ID NOT IN ('0','500','122','202','302','401')
                  GROUP BY PRODUCT_NO
                  UNION 
                  SELECT COUNT(*) AS CS, PRODUCT_NO 
                  FROM BASS1.G_S_04009_DAY
                  WHERE TIME_ID/100=$op_month
                       AND ROAM_TYPE_ID NOT IN ('0','500','122','202','302','401')
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

	      }
	      aidb_commit $conn
	      aidb_close $handle
	      
	     set RESULT_VAL1 [format "%.2f" [expr (${CHECK_VAL1}/1.00000/${CHECK_VAL2})]]
	     puts $RESULT_VAL1
	     
             #数据清零
             set CHECK_VAL1 0.00
             set CHECK_VAL2 0.00
             
             #汇总数据人均漫游通话次数
	set handle [aidb_open $conn]
        set sqlbuf "\
                SELECT 
            	   SUM(T.CS),
            	   COUNT(DISTINCT T.PRODUCT_NO)
                FROM (
                        SELECT SUM(BIGINT(CALL_COUNTS)) AS CS,PRODUCT_NO	  
                        FROM BASS1.G_S_21003_MONTH
                        WHERE TIME_ID=$op_month
                              AND ROAM_TYPE_ID NOT IN ('0','500','122','202','302','401')
                        GROUP BY PRODUCT_NO
                        UNION 
                        SELECT SUM(BIGINT(CALL_COUNTS)) AS CS,PRODUCT_NO	  
                        FROM BASS1.G_S_21006_MONTH
                        WHERE TIME_ID=$op_month
                              AND ROAM_TYPE_ID NOT IN ('0','500','122','202','302','401')
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

	      }
	      aidb_commit $conn
	      aidb_close $handle
	      
	      puts $CHECK_VAL1
	      puts $CHECK_VAL2
	     set RESULT_VAL2 [format "%.2f" [expr (${CHECK_VAL1}/1.00000/${CHECK_VAL2})]]
	     puts $RESULT_VAL2

             #(抽样值/汇总值-1)     
#	     set RESULT [format "%.2f" [expr ($RESULT_VAL1/1.00/$RESULT_VAL2-1.00)]]
	     set RESULT [format "%.5f" [expr (${RESULT_VAL1}/1.00000/${RESULT_VAL2}-1)]]
	     puts $RESULT
	     
             #将校验值插入校验结果表
             set handle [aidb_open $conn]
             set sqlbuf "INSERT INTO bass1.G_RULE_CHECK VALUES
             		($op_month ,
             		'92',
             		cast ($RESULT_VAL1 as  DECIMAL(18, 5) ),
             		cast ($RESULT_VAL2 as  DECIMAL(18, 5) ),
             		cast ($RESULT as  DECIMAL(18, 5) ),
             		0.05)"
   puts $sqlbuf			
             if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
             	WriteTrace $errmsg 003
             	return -1
             }
             aidb_commit $conn
             aidb_close $handle
             
             
             
             #判断92：|抽样详单人均漫游通话次数/汇总数据人均漫游通话次数 - 1| ≤ 5％，且人均漫游通话次数＞0超标
	     if { $RESULT_VAL1<=0 || $RESULT_VAL2<=0 || ($RESULT>0.05 ||$RESULT<-0.05 ) } {
	     	set grade 2
	        set alarmcontent "准确性指标92超出集团考核范围"
	        WriteAlarm $app_name $opmonth $grade ${alarmcontent}		
	     	     		
	     } elseif { ($RESULT>0.04 ||$RESULT<-0.04 ) } {	
	           set grade 3
	           set alarmcontent "准确性指标92接近集团考核范围5%,达到${RESULT_VAL1}"
	           WriteAlarm $app_name $opmonth $grade ${alarmcontent}
	          
	    } 	  	    	  
###########################93：基本通话费单价#################	   
        #抽样详单基本通话费单价
	set handle [aidb_open $conn]
	set sqlbuf "\
	            SELECT 
	            	SUM(T.FY),
	            	SUM(T.SC)
	            FROM (
	                  SELECT 
	                    SUM(BIGINT(BASE_BILL_DURATION)) AS SC ,
	                    SUM(BIGINT(BASE_CALL_FEE)) AS FY		  
	                  FROM BASS1.G_S_04008_DAY
	                  WHERE TIME_ID/100=$op_month  and roam_type_id not in ('122','202','302','401')  
	                  UNION ALL
	                  SELECT
	                    SUM(CEIL(BIGINT(CALL_DURATION)/60.0))AS SC, 
	                    SUM(BIGINT(BASE_CALL_FEE)) AS FY
	                  FROM BASS1.G_S_04009_DAY
	                  WHERE TIME_ID/100=$op_month
	                  )T"
             puts $sqlbuf
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
	      
	     set RESULT_VAL1 [format "%.2f" [expr (${CHECK_VAL1}/100.00000/${CHECK_VAL2})]]
	     puts $RESULT_VAL1
	     
             #数据清零
             set CHECK_VAL1 0.00
             set CHECK_VAL2 0.00
             	    
             #汇总数据基本通话费单价
	     set handle [aidb_open $conn]
	     set sqlbuf "\	
	            SELECT 
	            	SUM(T.FY),
	            	SUM(T.SC)
	            FROM (
	                  SELECT 
	                    SUM(BIGINT(BASE_BILL_DURATION)) AS SC,
	                    SUM(BIGINT(FAVOURED_BASECALL_FEE)) AS FY	  
	                  FROM BASS1.G_S_21003_MONTH
	                  WHERE TIME_ID=$op_month
	                  UNION ALL
	                  SELECT 
	                    SUM(BIGINT(BASE_BILL_DURATION)) AS SC,
	                    SUM(BIGINT(FAVOURED_BASECALL_FEE)) AS FY	  
	                  FROM BASS1.G_S_21006_MONTH
	                  WHERE TIME_ID=$op_month
	                  )T"
             puts $sqlbuf
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
	      
	     set RESULT_VAL2 [format "%.2f" [expr (${CHECK_VAL1}/100.00000/${CHECK_VAL2})]]
	     puts $RESULT_VAL2

             #(抽样值-汇总值)     
#	     set RESULT [format "%.2f" [expr ($RESULT_VAL1/1.00/$RESULT_VAL2-1.00)]]
	     set RESULT [format "%.5f" [expr ($RESULT_VAL1-$RESULT_VAL2)]]
	     puts $RESULT
	     
             #将校验值插入校验结果表
             set handle [aidb_open $conn]
             set sqlbuf "INSERT INTO bass1.G_RULE_CHECK VALUES
             		($op_month ,
             		'93',
             		cast ($RESULT_VAL1 as  DECIMAL(18, 5) ),
             		cast ($RESULT_VAL2 as  DECIMAL(18, 5) ),
             		cast ($RESULT as  DECIMAL(18, 5) ),
             		0.05)"
   puts $sqlbuf			
             if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
             	WriteTrace $errmsg 003
             	return -1
             }
             aidb_commit $conn
             aidb_close $handle
             
             
             
             #判断93：|抽样详单基本通话费单价－汇总数据基本通话费单价| ≤ 0.03，且基本通话费单价＞0（元/分钟）超标
	     if { $RESULT_VAL1<=0 || $RESULT_VAL2<=0 || ($RESULT>0.03 ||$RESULT<-0.03 ) } {
	     	set grade 2
	        set alarmcontent "扣分性指标93超出集团考核范围"
	        WriteAlarm $app_name $opmonth $grade ${alarmcontent}		
	     	     		
	     } elseif { ($RESULT>0.02 ||$RESULT<-0.02 ) } {	
	           set grade 3
	           set alarmcontent "扣分性指标93接近集团考核范围0.03,达到${RESULT_VAL1}"
	           WriteAlarm $app_name $opmonth $grade ${alarmcontent}
	          
	    } 
#############################H5：人均计费时长#################
        #抽样详单人均计费时长
	set handle [aidb_open $conn]
	set sqlbuf "\
	            SELECT 
	            	SUM(T.SC),
	            	COUNT(DISTINCT T.PRODUCT_NO)AS RS
	            FROM (
	                  SELECT PRODUCT_NO,SUM(BIGINT(BASE_BILL_DURATION)) AS SC		  
	                  FROM BASS1.G_S_04008_DAY
	                  WHERE TIME_ID/100=$op_month
	                  GROUP BY PRODUCT_NO
	                  UNION 
	                  SELECT PRODUCT_NO,SUM(CEIL(BIGINT(CALL_DURATION)/60.0)) AS SC
	                  FROM BASS1.G_S_04009_DAY
	                  WHERE TIME_ID/100=$op_month
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

	      }
	      aidb_commit $conn
	      aidb_close $handle
	      
	     set RESULT_VAL1 [format "%.2f" [expr (${CHECK_VAL1}/1.00000/${CHECK_VAL2})]]
	     puts $RESULT_VAL1
	     
             #数据清零
             set CHECK_VAL1 0.00
             set CHECK_VAL2 0.00
             	    
             #汇总数据人均计费时长
	     set handle [aidb_open $conn]
	     set sqlbuf "\	
	            SELECT 
	            	SUM(T.SC),
	            	COUNT(DISTINCT T.PRODUCT_NO) 
	            FROM (
	                  SELECT PRODUCT_NO,SUM(BIGINT(BASE_BILL_DURATION)) AS SC	  
	                  FROM BASS1.G_S_21003_MONTH
	                  WHERE TIME_ID=$op_month
	                  GROUP BY PRODUCT_NO
	                  UNION 
	                  SELECT PRODUCT_NO,SUM(BIGINT(BASE_BILL_DURATION)) AS SC  
	                  FROM BASS1.G_S_21006_MONTH
	                  WHERE TIME_ID=$op_month
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
             		($op_month ,
             		'H5',
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
             
             
             
             #判断H5：|抽样详单人均计费时长/汇总数据人均计费时长 - 1| ≤ 5%，且人均计费时长＞0超标
	     if { $RESULT_VAL1<=0 || $RESULT_VAL2<=0 || ($RESULT>0.05 ||$RESULT<-0.05 ) } {
	     	set grade 2
	        set alarmcontent "扣分性指标H5超出集团考核范围"
	        WriteAlarm $app_name $opmonth $grade ${alarmcontent}		
	     	     		
	     } elseif { ($RESULT>0.04 ||$RESULT<-0.04 ) } {	
	           set grade 3
	           set alarmcontent "扣分性指标H5接近集团考核范围5%,达到${RESULT_VAL1}"
	           WriteAlarm $app_name $opmonth $grade ${alarmcontent}
	          
	    } 	  
###########################################H6：人均长途计费时长##########
        #抽样详单人均长途计费时长
	set handle [aidb_open $conn]
	set sqlbuf "\
	            SELECT 
	            	SUM(T.SC),
	            	COUNT(DISTINCT T.PRODUCT_NO)
	            FROM (
	                  SELECT SUM(BIGINT(BASE_BILL_DURATION)) AS SC,PRODUCT_NO	  
	                  FROM BASS1.G_S_04008_DAY
	                  WHERE TIME_ID/100=$op_month
	                        AND TOLL_TYPE_ID<>'010'
	                  GROUP BY PRODUCT_NO
	                  UNION 
	                  SELECT SUM(CEIL(BIGINT(CALL_DURATION)/60.0)) AS SC,PRODUCT_NO
	                  FROM BASS1.G_S_04009_DAY
	                  WHERE TIME_ID/100=$op_month
	                        AND TOLL_TYPE_ID<>'010'
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

	      }
	      aidb_commit $conn
	      aidb_close $handle
	      
	     set RESULT_VAL1 [format "%.2f" [expr (${CHECK_VAL1}/1.00000/${CHECK_VAL2})]]
	     puts $RESULT_VAL1
	     
             #数据清零
             set CHECK_VAL1 0.00
             set CHECK_VAL2 0.00
             	    
             #汇总数据人均长途计费时长
	     set handle [aidb_open $conn]
	     set sqlbuf "\	
		            SELECT 
		            	SUM(T.SC),
		            	COUNT(DISTINCT T.PRODUCT_NO)
		            FROM (
		                  SELECT  SUM(BIGINT(BASE_BILL_DURATION)) AS SC,PRODUCT_NO
		                  FROM BASS1.G_S_21003_MONTH
		                  WHERE TIME_ID=$op_month
		                        AND TOLL_TYPE_ID<>'010'
		                  GROUP BY PRODUCT_NO
		                  UNION 
		                  SELECT SUM(BIGINT(BASE_BILL_DURATION)) AS SC,PRODUCT_NO
		                  FROM BASS1.G_S_21006_MONTH
		                  WHERE TIME_ID=$op_month
		                        AND TOLL_TYPE_ID<>'010'
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
             		($op_month ,
             		'H6',
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
             
             
             
             #判断H6：|抽样详单人均长途计费时长/汇总数据人均长途计费时长 - 1| ≤ 5％，且人均长途计费时长＞0超标
	     if { $RESULT_VAL1<=0 || $RESULT_VAL2<=0 || ($RESULT>0.05 ||$RESULT<-0.05 ) } {
	     	set grade 2
	        set alarmcontent "准确性指标H6超出集团考核范围"
	        WriteAlarm $app_name $opmonth $grade ${alarmcontent}		
	     	     		
	     } elseif { ($RESULT>0.04 ||$RESULT<-0.04 ) } {	
	           set grade 3
	           set alarmcontent "准确性指标H6接近集团考核范围5%,达到${RESULT_VAL1}"
	           WriteAlarm $app_name $opmonth $grade ${alarmcontent}
	          
	    }           
##################################END#######################
	return 0
}	    	    