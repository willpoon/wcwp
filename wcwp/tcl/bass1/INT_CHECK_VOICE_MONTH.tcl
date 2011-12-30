######################################################################################################
#程序名称：INT_CHECK_VOICE_MONTH.tcl
#校验接口：R092 R093 R094
#运行粒度: 月
#输入参数: 
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：zhanght
#编写时间：2009-06-23
#问题记录：
#修改历史: liuqf 20090805 修改上月指标抓取口径
#          liuzhilong 20091003  修改R109校验逻辑 加上条件 CDR_TYPE IN ('00','10','21')   
#######################################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

        #本月 yyyymm
        set op_month [string range $optime_month 0 3][string range $optime_month 5 6]
            
        #上月  yyyymm
        set last_month [GetLastMonth [string range $op_month 0 5]]
        
        puts $last_month
        
        #自然月第一天 yyyymmdd
        set timestamp [string range $op_time 0 3][string range $op_time 5 6]01
        
        puts $timestamp
        
        #本月第一天 yyyymmdd
        set l_timestamp [string range $optime_month 0 3][string range $optime_month 5 6]01
        
        puts $l_timestamp
        
        #当天 yyyymmdd
        set timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
        
        #当天 yyyy-mm-dd
        set optime $op_time
        
        #前一天 yyyymmdd
        set last_day [GetLastDay [string range $timestamp 0 7]]
        
        #程序名
        set app_name "INT_CHECK_VOICE_MONTH.tcl"

        #本月最后一天 yyyy-mm-dd
        set this_month_last_day [string range $op_month 0 3][string range $op_time 4 4][string range $op_month 4 5][string range $op_time 4 4][GetThisMonthDays [string range $op_month 0 5]01]
        
        puts $this_month_last_day

        #本月最后一天#,格式 yyyymmdd
        set last_month_day [GetLastDay [string range $timestamp 0 5]01]
        
        puts $last_month_day
        
        #上月最后一天 yyyymmdd
        
        set last_month_last_day [GetLastDay [string range $op_month 0 5]01]
        
        puts $last_month_last_day


	set handle [ aidb_open $conn ]
	set sqlbuf "delete from bass1.g_rule_check where time_id = $op_month 
	                    and rule_code in ('R092','R093','R094','R097','R109','R108','R107','R089','R090','R091')"
	puts ${sqlbuf}
	if [ catch { aidb_sql $handle $sqlbuf } errmsg ] {
		WriteTrace "$errmsg" 10000
		return -1
	}
	aidb_commit $conn
	aidb_close $handle



   #--R092 |∑(本月每日GSM普通语音业务日使用的计费时长合计)/本月GSM普通语音业务月使用的计费时长合计 - 1|≤1％
   
   set sqlbuf "select sum(bigint(BASE_BILL_DURATION))+sum(bigint(TOLL_BILL_DURATION))
               from G_S_21001_DAY
               where time_id/100=$op_month with ur"
               
   set RESULT_VAL1 [get_single $sqlbuf]
   
   set RESULT_VAL1 [format "%.3f" [expr ${RESULT_VAL1} /1.00]]
   
   set sqlbuf "select sum(bigint(BASE_BILL_DURATION))+sum(bigint(TOLL_BILL_DURATION))
               from G_S_21003_MONTH
               where time_id=$op_month with ur"
                
   set RESULT_VAL2 [get_single $sqlbuf]
   
   set RESULT_VAL2 [format "%.3f" [expr ${RESULT_VAL2} /1.00]]
     
   set sqlbuf "INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R092',$RESULT_VAL1,$RESULT_VAL2,0,0) "        
   exec_sql $sqlbuf

  puts $RESULT_VAL1
  puts $RESULT_VAL2
  set  RESULT_VAL3 [expr $RESULT_VAL1/$RESULT_VAL2 - 1 ]
  puts  $RESULT_VAL3
	if {$RESULT_VAL3>0.01} {
		set grade 2
	        set alarmcontent "R092 校验不通过"
	        WriteAlarm $app_name $optime $grade ${alarmcontent}
	} 
	puts "|∑(本月每日GSM普通语音业务日使用的计费时长合计)/本月GSM普通语音业务月使用的计费时长合计 - 1|≤1％"




   #--R093 |∑(本月每日GSM普通语音业务日使用的通话时长合计)/本月GSM普通语音业务月使用的通话时长合计 - 1|≤1％
   
   set sqlbuf "select sum(bigint(CALL_DURATION))
               from G_S_21001_DAY
               where time_id/100=$op_month with ur
               "
   puts $sqlbuf            
   set RESULT_VAL1 [get_single $sqlbuf]
   
   set RESULT_VAL1 [format "%.3f" [expr ${RESULT_VAL1} /1.00]]
   
   set sqlbuf "select sum(bigint(CALL_DURATION))
               from G_S_21003_MONTH
               where time_id=$op_month with ur"
   
   puts $sqlbuf         
   set RESULT_VAL2 [get_single $sqlbuf]
   
   set RESULT_VAL2 [format "%.3f" [expr ${RESULT_VAL2} /1.00]]
     
   set sqlbuf "INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R093',$RESULT_VAL1,$RESULT_VAL2,0,0) "        
   exec_sql $sqlbuf

  puts $RESULT_VAL1
  puts $RESULT_VAL2
  set  RESULT_VAL3 [expr $RESULT_VAL1/$RESULT_VAL2 - 1 ]
  puts  $RESULT_VAL3
	if {$RESULT_VAL3>0.01} {
		set grade 2
	        set alarmcontent "R093 校验不通过"
	        WriteAlarm $app_name $optime $grade ${alarmcontent}
	} 
	puts "|∑(本月每日GSM普通语音业务日使用的通话时长合计)/本月GSM普通语音业务月使用的通话时长合计 - 1|≤1％"



   #--R094 |∑(本月每日GSM普通语音业务日使用的总费用合计)/本月GSM普通语音业务月使用的总费用合计 - 1|≤1％
   
   set sqlbuf "select sum(bigint(FAVOURED_CALL_FEE))
               from G_S_21001_DAY
               where time_id/100=$op_month with ur"
   puts $sqlbuf            
   set RESULT_VAL1 [get_single $sqlbuf]
   
   set RESULT_VAL1 [format "%.3f" [expr ${RESULT_VAL1} /1.00]]
   
   set sqlbuf "select sum(bigint(FAVOURED_CALL_FEE))
               from G_S_21003_MONTH
               where time_id=$op_month with ur"
   
   puts $sqlbuf             
   set RESULT_VAL2 [get_single $sqlbuf]
   
   set RESULT_VAL2 [format "%.3f" [expr ${RESULT_VAL2} /1.00]]
     
   set sqlbuf "INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R094',$RESULT_VAL1,$RESULT_VAL2,0,0) "        
   exec_sql $sqlbuf

  puts $RESULT_VAL1
  puts $RESULT_VAL2
  set  RESULT_VAL3 [expr $RESULT_VAL1/$RESULT_VAL2 - 1 ]
  puts  $RESULT_VAL3
	if {$RESULT_VAL3>0.01} {
		set grade 2
	        set alarmcontent "R094 校验不通过"
	        WriteAlarm $app_name $optime $grade ${alarmcontent}
	} 
	puts "|∑(本月每日GSM普通语音业务日使用的总费用合计)/本月GSM普通语音业务月使用的总费用合计 - 1|≤1％"



   #--R097 |∑(本月每日普通短信业务日使用的通信量合计)/本月普通短信业务月使用的通信量合计 - 1|≤1％
   
   set sqlbuf "select sum(bigint(SMS_COUNTS))
               from BASS1.G_S_21007_DAY
               where time_id/100=$op_month
                with ur  "
   puts $sqlbuf            
   set RESULT_VAL1 [get_single $sqlbuf]
   
   set RESULT_VAL1 [format "%.3f" [expr ${RESULT_VAL1} /1.00]]
   
   set sqlbuf "select sum(bigint(SMS_COUNTS))
               from BASS1.G_S_21008_MONTH 
               where time_id=$op_month
               with ur"
   puts $sqlbuf             
   set RESULT_VAL2 [get_single $sqlbuf]
   
   set RESULT_VAL2 [format "%.3f" [expr ${RESULT_VAL2} /1.00]]
     
   set sqlbuf "INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R097',$RESULT_VAL1,$RESULT_VAL2,0,0) "        
   exec_sql $sqlbuf

  puts $RESULT_VAL1
  puts $RESULT_VAL2
  set  RESULT_VAL3 [expr $RESULT_VAL1/$RESULT_VAL2 - 1 ]
  puts  $RESULT_VAL3
	if {$RESULT_VAL3>0.01} {
		set grade 2
	        set alarmcontent "R097 校验不通过"
	        WriteAlarm $app_name $optime $grade ${alarmcontent}
	} 
	puts "|∑(本月每日普通短信业务日使用的通信量合计)/本月普通短信业务月使用的通信量合计 - 1|≤1％"




#############################R109：人均点对点计费量#################
        #抽样详单人均计费时长
	set handle [aidb_open $conn]
	set sqlbuf "\
	            select count(*),
	                   count(distinct product_no)
              from BASS1.G_S_04011_DAY
              where time_id/100=$op_month
                and svc_type in ('11','12','13')
                AND CDR_TYPE IN ('00','10','21')
		         	   AND SMS_STATUS='0'
               with ur "
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
	            select sum(bigint(SMS_COUNTS)),
	                        count(distinct product_no)
                   from BASS1.G_S_21008_MONTH
                    where time_id=$op_month
                     and END_STATUS='0' 
                     and svc_type_id in ('11','12','13')
                     AND CDR_TYPE_ID IN ('00','10','21') 
                   with ur"
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
             
             
             
             #判断R109：|抽样详单人均点对点短信计费量/汇总数据人均点对点短信计费量 - 1| ≤ 5%，且计费量＞0
	     if { $RESULT_VAL1<=0 || $RESULT_VAL2<=0 || ($RESULT>0.05 ||$RESULT<-0.05 ) } {
	     	set grade 2
	        set alarmcontent "扣分性指标R109超出集团考核范围"
	        WriteAlarm $app_name $optime $grade ${alarmcontent}		
	     	     		
	     } elseif { ($RESULT>0.04 ||$RESULT<-0.04 ) } {	
	           set grade 3
	           set alarmcontent "扣分性指标R109接近集团考核范围5%,达到${RESULT}"
	           WriteAlarm $app_name $optime $grade ${alarmcontent}
	          
	    } 	  


#############################R108：人均计费时长#################
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
	                  FROM BASS1.G_S_21003_TO_DAY
	                  WHERE TIME_ID/100=$op_month
	                  GROUP BY PRODUCT_NO
	                  UNION 
	                  SELECT PRODUCT_NO,SUM(BIGINT(BASE_BILL_DURATION)) AS SC  
	                  FROM BASS1.G_S_21006_TO_DAY
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
             		'R108',
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
             
             
             
             #判断R139：|抽样详单人均计费时长/汇总数据人均计费时长 - 1| ≤ 5%，且人均计费时长＞0超标
	     if { $RESULT_VAL1<=0 || $RESULT_VAL2<=0 || ($RESULT>0.05 ||$RESULT<-0.05 ) } {
	     	set grade 2
	        set alarmcontent "扣分性指标R108超出集团考核范围"
	        WriteAlarm $app_name $optime $grade ${alarmcontent}		
	     	     		
	     } 
	     
##	     elseif { ($RESULT>0.04 ||$RESULT<-0.04 ) } {	
##	           set grade 3
##	           set alarmcontent "扣分性指标R108接近集团考核范围5%,达到${RESULT}"
##	           WriteAlarm $app_name $optime $grade ${alarmcontent}
##	          
##	    } 	  


###################################R107：人均通话费##########################

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

	      }
	      aidb_commit $conn
	      aidb_close $handle
	     puts $CHECK_VAL2
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
                         FROM BASS1.G_S_21003_TO_DAY
                         WHERE TIME_ID/100=$op_month
                         GROUP BY PRODUCT_NO
                         UNION 
                         SELECT PRODUCT_NO,SUM(BIGINT(FAVOURED_BASECALL_FEE)) AS JB,SUM(BIGINT(FAVOURED_TOLLCALL_FEE))AS CT
                         FROM BASS1.G_S_21006_TO_DAY
                         WHERE TIME_ID/100=$op_month
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
             		'R107',
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
             
             
             
             #判断R107：|抽样详单人均通话费/汇总数据人均通话费 - 1| ≤ 5%，且人均通话费＞0超标
	     if { $RESULT_VAL1<=0 || $RESULT_VAL2<=0 || ($RESULT>0.05 ||$RESULT<-0.05 ) } {
	     	set grade 2
	        set alarmcontent "扣分性指标R107超出集团考核范围"
	        WriteAlarm $app_name $optime $grade ${alarmcontent}		
	     	     		
	     } elseif { ($RESULT>0.04 ||$RESULT<-0.04 ) } {	
	           set grade 3
	           set alarmcontent "扣分性指标R107接近集团考核范围5%,达到${RESULT}"
	           WriteAlarm $app_name $optime $grade ${alarmcontent}
	          
	    }                 
	    
	    
	    
	    
#add by zhanght on 20090708

	 #--DEC_CHECK_VALUE_11保留本月三品牌合计语音通话费,DEC_CHECK_VALUE_12保留上月三品牌合计语音通话费
         #--本月三品牌合计语音通话费
         set handle [aidb_open $conn]

	 set sqlbuf "SELECT SUM(T.FY)
   	         FROM 
   	         (
   	            SELECT SUM(BIGINT(FAVOURED_CALL_FEE)) AS FY
   	            FROM BASS1.G_S_21003_TO_DAY
   	            WHERE TIME_ID/100=${op_month}
   	            UNION
   	            SELECT SUM(BIGINT(FAVOURED_CALL_FEE)) AS FY
   	            FROM BASS1.G_S_21006_TO_DAY
   	            WHERE TIME_ID/100=${op_month}
   	         )T"
  puts $sqlbuf

  if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
		WriteTrace $errmsg 1001
		return -1
	}
	if [catch {set DEC_CHECK_VALUE_11 [lindex [aidb_fetch $handle] 0]} errmsg ] {
		WriteTrace $errmsg 1002
		return -1
	}


#############
	 #--DEC_CHECK_VALUE_99保留上月三品牌合计语音通话费
         #--上月三品牌合计语音通话费
         set handle [aidb_open $conn]

	 set sqlbuf "SELECT SUM(T.FY)
   	         FROM 
   	         (
   	            SELECT SUM(BIGINT(FAVOURED_CALL_FEE)) AS FY
   	            FROM BASS1.G_S_21003_TO_DAY
   	            WHERE TIME_ID/100=${last_month}
   	            UNION
   	            SELECT SUM(BIGINT(FAVOURED_CALL_FEE)) AS FY
   	            FROM BASS1.G_S_21006_TO_DAY
   	            WHERE TIME_ID/100=${last_month}
   	         )T"
  puts $sqlbuf

  if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
		WriteTrace $errmsg 1001
		return -1
	}
	if [catch {set DEC_CHECK_VALUE_99 [lindex [aidb_fetch $handle] 0]} errmsg ] {
		WriteTrace $errmsg 1002
		return -1
	}


	aidb_commit $conn


	#--R089: 全球通语音通话费占比
        #--本月占比乘100
        set handle [aidb_open $conn]

	 set sqlbuf "SELECT SUM(BIGINT(FAVOURED_CALL_FEE)) AS FY
   	       FROM BASS1.G_S_21003_month
   	       WHERE TIME_ID=${op_month}
   	             AND BRAND_ID='1'"
   puts $sqlbuf

   if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
		WriteTrace $errmsg 1001
		return -1
	}

	if [catch {set DEC_RESULT_VAL1 [lindex [aidb_fetch $handle] 0]} errmsg ] {
		WriteTrace $errmsg 1002
		return -1
	}

	aidb_commit $conn
	
       set DEC_RESULT_VAL1 [format "%.4f" [expr (${DEC_RESULT_VAL1} /1.00000 / ${DEC_CHECK_VALUE_11} *100)]]
       
       puts "------------------------R089-----------------------------"
       puts "------------------------1-----------------------------"
       puts $DEC_RESULT_VAL1
       #--数据清零
       set DEC_CHECK_VALUE_1 "0"
       #--上月占比乘100
        set handle [aidb_open $conn]
###        set sqlbuf "SELECT TARGET1 
###   	         FROM BASS1.G_RULE_CHECK 
###   	         WHERE TIME_ID=${last_month_day}
###                   AND RULE_CODE='R089'"

      set sqlbuf "SELECT SUM(BIGINT(FAVOURED_CALL_FEE)) AS FY
 	       FROM BASS1.G_S_21003_month
 	      WHERE TIME_ID=${last_month}
 	        AND BRAND_ID='1'"

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
	
	set DEC_RESULT_VAL2 [format "%.4f" [expr (${DEC_RESULT_VAL2} /1.00000 / ${DEC_CHECK_VALUE_99} *100)]]

  puts "------------------------2-----------------------------"
	puts "$DEC_RESULT_VAL2"
	
	set DEC_TARGET_VAL1 [format "%.4f" [expr (${DEC_RESULT_VAL1} /1.00000/ ${DEC_RESULT_VAL2} - 1 )]]
	if {$DEC_TARGET_VAL1 <=0 }  {
		set DEC_TARGET_VAL1 [format "%.4f" [expr $DEC_TARGET_VAL1 * (-1)]]
		}
		
  puts "------------------------R089--result-----------------------------"
	puts ${DEC_TARGET_VAL1}
	
##	set handle [aidb_open $conn]
##	set sql_buff "delete from bass1.g_rule_check where time_id = $op_month and rule_code = 'R089'"
##  puts $sql_buff
##  
##	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
##		WriteTrace "$errmsg" 2005
##		aidb_close $handle
##		return -1
##	}
##	aidb_commit $conn
##	
	#--将校验值插入校验结果表
	set handle [aidb_open $conn]
	set sql_buff "INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R089',$DEC_RESULT_VAL1,$DEC_RESULT_VAL2,$DEC_TARGET_VAL1,0); "
  puts $sql_buff
  
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	#--判断
        #--异常：
#R089: 本月比上月变动率（全球通语音通话费/合计语音通话费）≤ 10%，且15%≤占比≤40%

	
#add by zhanght on 20090623 全球通语音通话费占比变动率≤10%	
		if {${DEC_TARGET_VAL1}>0.10} {
		set grade 2
	  set alarmcontent "准确性指标R089超出集团考核范围"
	  WriteAlarm $app_name $optime $grade ${alarmcontent}
	  } 

	
	puts "R089 finish"
	#----------------------------------------------------------
	#--R090: 神州行语音通话费占比
        #--本月占比乘100
        set handle [aidb_open $conn]

	 set sqlbuf "SELECT SUM(BIGINT(FAVOURED_CALL_FEE)) AS FY
   	       FROM BASS1.G_S_21003_TO_DAY
   	       WHERE TIME_ID/100=${op_month}
   	             AND BRAND_ID='2'"
  puts $sqlbuf

        if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
		WriteTrace $errmsg 1001
		return -1
	}

	if [catch {set DEC_RESULT_VAL1 [lindex [aidb_fetch $handle] 0]} errmsg ] {
		WriteTrace $errmsg 1002
		return -1
	}

	aidb_commit $conn
	
       set DEC_RESULT_VAL1 [format "%.4f" [expr (${DEC_RESULT_VAL1} /1.00000 / ${DEC_CHECK_VALUE_11} *100)]]
       #--数据清零
       set DEC_CHECK_VALUE_1 "0"
       #--上月占比乘100
        set handle [aidb_open $conn]
##        set sqlbuf "SELECT TARGET1 
##   	         FROM BASS1.G_RULE_CHECK 
##   	         WHERE TIME_ID=${last_month_day}
##                   AND RULE_CODE='R090'"  

    set sqlbuf "SELECT SUM(BIGINT(FAVOURED_CALL_FEE)) AS FY
       FROM BASS1.G_S_21003_TO_DAY
      WHERE TIME_ID/100=${last_month}
        AND BRAND_ID='2'"

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
	
	set DEC_RESULT_VAL2 [format "%.4f" [expr (${DEC_RESULT_VAL2} /1.00000 / ${DEC_CHECK_VALUE_99} *100)]]
	
	#set DEC_RESULT_VAL2 "100"
	puts "$DEC_RESULT_VAL2"
	
	set DEC_TARGET_VAL1 [format "%.4f" [expr (${DEC_RESULT_VAL1} /1.00000/ ${DEC_RESULT_VAL2} - 1 )]]
	if {$DEC_TARGET_VAL1 <=0 }  {
		set DEC_TARGET_VAL1 [format "%.4f" [expr $DEC_TARGET_VAL1 * (-1)]]
		}

	puts ${DEC_TARGET_VAL1}

##	set handle [aidb_open $conn]
##	set sql_buff "delete from bass1.g_rule_check where time_id = $op_month and rule_code = 'R090'"
##  puts $sql_buff
##  
##	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
##		WriteTrace "$errmsg" 2005
##		aidb_close $handle
##		return -1
##	}
##	aidb_commit $conn
##
	#--将校验值插入校验结果表
	set handle [aidb_open $conn]
	set sql_buff "INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R090',$DEC_RESULT_VAL1,$DEC_RESULT_VAL2,$DEC_TARGET_VAL1,0); "
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	#--判断
        #--异常：
#R090: 本月比上月变动率（神州行语音通话费/合计语音通话费）≤ 8%，且35%≤占比≤65%
        

#add by zhanght on 20090623	 	神州行语音通话费占比变动率≤10%
	 	if { ${DEC_TARGET_VAL1}>0.10 } {
		set grade 2
	        set alarmcontent "准确性指标R090超出集团考核范围"
	        WriteAlarm $app_name $optime $grade ${alarmcontent}
	        } 
	 	
	 	
     puts "R090 finish"
     #------------------------------------------------------------------
     #--R091: 动感地带语音通话费占比
   #--本月占比乘100
        set handle [aidb_open $conn]

	 set sqlbuf "SELECT SUM(BIGINT(FAVOURED_CALL_FEE)) AS FY
   	       FROM BASS1.G_S_21003_TO_DAY
   	       WHERE TIME_ID/100=${op_month}
   	             AND BRAND_ID='3'"
  puts $sqlbuf

        if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
		WriteTrace $errmsg 1001
		return -1
	}

	if [catch {set DEC_RESULT_VAL1 [lindex [aidb_fetch $handle] 0]} errmsg ] {
		WriteTrace $errmsg 1002
		return -1
	}

	aidb_commit $conn
	
       set DEC_RESULT_VAL1 [format "%.4f" [expr (${DEC_RESULT_VAL1} /1.00000 / ${DEC_CHECK_VALUE_11} *100)]]
       #--数据清零
       set DEC_CHECK_VALUE_1 "0"
       #--上月占比乘100
        set handle [aidb_open $conn]
##        set sqlbuf "SELECT TARGET1 
##   	         FROM BASS1.G_RULE_CHECK 
##   	         WHERE TIME_ID=${last_month_day}
##                   AND RULE_CODE='R091'"

    set sqlbuf "SELECT SUM(BIGINT(FAVOURED_CALL_FEE)) AS FY
       FROM BASS1.G_S_21003_TO_DAY
      WHERE TIME_ID/100=${last_month}
        AND BRAND_ID='3'"

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
	#set DEC_RESULT_VAL2 "100"
	
	set DEC_RESULT_VAL2 [format "%.4f" [expr (${DEC_RESULT_VAL2} /1.00000 / ${DEC_CHECK_VALUE_99} *100)]]
	
	puts "$DEC_RESULT_VAL2"
	
	set DEC_TARGET_VAL1 [format "%.4f" [expr (${DEC_RESULT_VAL1} /1.00000/ ${DEC_RESULT_VAL2} - 1 )]]
	if {$DEC_TARGET_VAL1 <=0 }  {
		set DEC_TARGET_VAL1 [format "%.4f" [expr $DEC_TARGET_VAL1 * (-1)]]
		}

	puts ${DEC_TARGET_VAL1}
	
##	set handle [aidb_open $conn]
##	set sql_buff "delete from bass1.g_rule_check where time_id = $timestamp and rule_code = 'R091'"
##  puts $sql_buff
##  
##	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
##		WriteTrace "$errmsg" 2005
##		aidb_close $handle
##		return -1
##	}
##	aidb_commit $conn
##
	#--将校验值插入校验结果表
	set handle [aidb_open $conn]
	set sql_buff "INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'R091',$DEC_RESULT_VAL1,$DEC_RESULT_VAL2,$DEC_TARGET_VAL1,0); "
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	#--判断
        #--异常：
 #R091: 本月比上月变动率（动感地带语音通话费/合计语音通话费）≤ 10%，且3<占比<18%
       

	 	
#add by zhanght on 20090623 动感地带语音通话费占比变动率≤12%	 	
	if { ${DEC_TARGET_VAL1}>0.12 } {
		set grade 2
	        set alarmcontent "准确性指标R091超出集团考核范围"
	        WriteAlarm $app_name $optime $grade ${alarmcontent}
	        } 

	 	
	puts "R091 finish"

  aidb_close $handle



		
	return 0
}

#内部函数部分	
proc exec_sql {MySQL} {

	global env

	global conn

	global handle

	set handle [aidb_open $conn]
	set sql_buff $MySQL
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		puts $errmsg
		exit -1
	}
	aidb_commit $conn
	aidb_close $handle
	return 0
}

proc get_single {MySQL} {

	global env

	global conn

	global handle

	set handle [aidb_open $conn]
	set sql_buff $MySQL
  if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace $errmsg 1001
		puts $errmsg
		exit -1
	}
	if [catch {set result [lindex [aidb_fetch $handle] 0]} errmsg ] {
		WriteTrace $errmsg 1002
		puts $errmsg
		exit -1
	}
	aidb_commit $conn
	aidb_close $handle

	
	return $result
}