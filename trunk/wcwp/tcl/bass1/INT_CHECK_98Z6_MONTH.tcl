#################################################################
#程序名称: INT_CHECK_98Z6_MONTH.tcl
#功能描述: 
#规则编号：98，Z6
#规则属性：业务逻辑
#规则类型：月
#指标摘要：98：无主IMEI占比
#          Z6：IMEI号码数占在网用户数比例
#规则描述：98：无主IMEI占全部IMEI数量比例 ≤ 0.01％
#          Z6：80％ ≤ IMEI数占在网用户数比例 ≤ 105％
#校验对象：1.BASS1.G_A_02004_DAY  用户
#          2.BASS1.G_A_02008_DAY  用户状态
#          3.BASS1.G_I_02047_MONTH 用户标识与IMEI的对应关系
#输入参数:
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：王琦
#编写时间：2007-03-22
#问题记录：1.
#修改历史: 1.
##################################################################

proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

	#本月 yyyymm
	set op_month [string range $optime_month 0 3][string range $optime_month 5 6]
	#本月 yyyy-mm
	set opmonth $optime_month
	#本月最后一天 yyyymmdd
        set this_month_last_day [string range $op_month 0 5][GetThisMonthDays [string range $op_month 0 5]01]	
        #程序名称
##        set app_name "INT_CHECK_98Z6_MONTH.tcl"
##        
##        
##        
##        set handle [aidb_open $conn]
##	set sql_buff "\
##		delete from bass1.g_rule_check where time_id=$op_month 
##		  and rule_code in ('98','Z6')"
##	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
##		WriteTrace "$errmsg" 2005
##		aidb_close $handle
##		return -1
##	}
##	aidb_commit $conn
##	aidb_close $handle
##	
##	#--98：无主IMEI占比
##        #--统计口径：分别统计存在于02047中的而不存在于02004中的当月有效用户数，
##        #和02047中当月有效用户数， 两者相除无主IMEI占比
##        
##        #--存在于02047中的而不存在于02004中的当月有效用户数
##       
##           
##       set handle [aidb_open $conn]
##	set sqlbuf "
##                SELECT 
##                  COUNT(DISTINCT P.USER_ID) 
##		FROM 
##		(
##                  SELECT USER_ID 
##                  FROM BASS1.G_I_02047_MONTH 
##                  WHERE TIME_ID=$op_month
##		  EXCEPT
##		  SELECT T.USER_ID
##    		  FROM 
##    		    (    SELECT
##    		 	   A.TIME_ID,
##    		 	   A.USER_ID,
##    		 	   A.USERTYPE_ID,
##    		 	   A.SIM_CODE
##    			 FROM BASS1.G_A_02004_DAY A,
##    			(SELECT USER_ID,MAX(TIME_ID) AS TIME_ID FROM BASS1.G_A_02004_DAY
##    	 	 	 WHERE TIME_ID <=$this_month_last_day
##    	  	         GROUP BY USER_ID
##    	     	         ) B
##    	                WHERE A.USER_ID = B.USER_ID AND A.TIME_ID = B.TIME_ID 
##    	            )T, 	
##    	    
##                    (     SELECT
##    	 		    A.TIME_ID,
##    	 		    A.USER_ID,
##    	 		    A.USERTYPE_ID
##    		    	  FROM BASS1.G_A_02008_DAY A,
##    		         (SELECT USER_ID,MAX(TIME_ID) AS TIME_ID FROM BASS1.G_A_02008_DAY
##    		          WHERE TIME_ID <=$this_month_last_day
##    		 	  GROUP BY USER_ID
##    	                 ) B
##    			 WHERE A.USER_ID = B.USER_ID AND A.TIME_ID = B.TIME_ID 
##    		     ) M		
##   		  WHERE 
##   		     T.USER_ID = M.USER_ID
##    		     AND T.TIME_ID <=$this_month_last_day
##    		     AND M.TIME_ID <=$this_month_last_day
##    		     AND T.USERTYPE_ID <> '3'
##    		     AND T.SIM_CODE <> '1'	
##	     )P"
##        puts $sqlbuf
##	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
##		WriteTrace $errmsg 001
##		return -1
##	}
##	while { [set p_row [aidb_fetch $handle]] != "" } {
##		set DEC_RESULT_VAL1 [lindex $p_row 0]
##	}
##	aidb_commit $conn
##	aidb_close $handle
##	
##       puts $DEC_RESULT_VAL1
##       
##       #--02047中当月有效用户数
##        set handle [aidb_open $conn]
##	set sqlbuf "
##                SELECT 
##            	COUNT(DISTINCT USER_ID) 
##            FROM BASS1.G_I_02047_MONTH 
##            WHERE TIME_ID=$op_month"
##        puts $sqlbuf
##	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
##		WriteTrace $errmsg 001
##		return -1
##	}
##	while { [set p_row [aidb_fetch $handle]] != "" } {
##		set DEC_RESULT_VAL2 [lindex $p_row 0]
##	}
##	aidb_commit $conn
##	aidb_close $handle
##	
##	puts $DEC_RESULT_VAL2
##	
##	#--将校验值插入校验结果表
##	set handle [aidb_open $conn]
##	set sql_buff "INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'98',$DEC_RESULT_VAL1,$DEC_RESULT_VAL2,0,0); "
##	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
##		WriteTrace "$errmsg" 2005
##		aidb_close $handle
##		return -1
##	}
##	aidb_commit $conn
##	aidb_close $handle
##	
##	 #--判断
##         #--异常
##         #--1：无主IMEI占全部IMEI数量比例 ≤ 0.01％超标
##	if {[format "%.2f" [expr (${DEC_RESULT_VAL1} /1.00000/ ${DEC_RESULT_VAL2})]]>0.0001} {
##	        set grade 2
##	        set alarmcontent "准确性指标98超出集团考核范围"
##	        WriteAlarm $app_name $opmonth $grade ${alarmcontent}
##          }
##        
##       # ---------------------------------
##       # --Z6：IMEI号码数占在网用户数比例
##       # --统计口径：Step1.计算在02047中且同时在02004中状态是在网，即用户状态不等于
##       # --               (2010,2020,2030,1040,1021,9000),同时在02008接口中用户类型不等于测试用户，
##       # --               也不是数据SIM卡的IMEI用户数；
##       # --          Step2.从KPI中取用户到达数作为在网用户数
##       # --IMEI数
##       set handle [aidb_open $conn]
##	set sqlbuf "
##                SELECT 
##     	          BIGINT(COUNT(DISTINCT AB.USER_ID))
##                FROM
##    	        (
##    	           SELECT 
##         	     USER_ID 
##                   FROM BASS1.G_I_02047_MONTH
##                   WHERE TIME_ID=$op_month 
##                )AB,
##          
##    	       (  
##    	          SELECT 
##    		    T.USER_ID
##    	          FROM 
##    		  (
##    		    SELECT
##    		      A.TIME_ID,
##    		      A.USER_ID,
##    		      A.USERTYPE_ID,
##    		      A.SIM_CODE
##    		    FROM BASS1.G_A_02004_DAY  A,
##    		   (SELECT USER_ID,MAX(TIME_ID) AS TIME_ID FROM BASS1.G_A_02004_DAY
##    	 	    WHERE TIME_ID <= $this_month_last_day
##    	  	    GROUP BY USER_ID
##    	           ) B
##    	           WHERE A.USER_ID = B.USER_ID AND A.TIME_ID = B.TIME_ID
##    	         )T,  	          	  	    
##    		 (
##    		    SELECT
##    	 	      A.TIME_ID,
##    	 	      A.USER_ID,
##    	 	      A.USERTYPE_ID
##    		    FROM BASS1.G_A_02008_DAY A,
##    		   (SELECT USER_ID,MAX(TIME_ID) AS TIME_ID FROM BASS1.G_A_02008_DAY
##    		    WHERE TIME_ID <=$this_month_last_day
##    		    GROUP BY USER_ID
##    	           ) B
##    		   WHERE A.USER_ID = B.USER_ID AND A.TIME_ID = B.TIME_ID 
##    		) M		
##   	       WHERE T.USER_ID = M.USER_ID
##    	         AND T.TIME_ID <=$this_month_last_day
##    		 AND M.TIME_ID <=$this_month_last_day
##    		 AND T.USERTYPE_ID <> '3'
##    		 AND T.SIM_CODE <> '1'
##    		 AND M.USERTYPE_ID NOT IN ('2010','2020','2030','1040','1021','9000')
##         ) CD
##        WHERE AB.USER_ID=CD.USER_ID;"
##        puts $sqlbuf
##	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
##		WriteTrace $errmsg 001
##		return -1
##	}
##	while { [set p_row [aidb_fetch $handle]] != "" } {
##		set DEC_RESULT_VAL1 [lindex $p_row 0]
##	}
##	aidb_commit $conn
##	aidb_close $handle
##	
##       puts $DEC_RESULT_VAL1
##       set DEC_RESULT_VAL2 [exec get_kpi.sh ${this_month_last_day} 2 2]
##       
##       #--将校验值插入校验结果表
##	set handle [aidb_open $conn]
##	set sql_buff "INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'Z6',$DEC_RESULT_VAL1,$DEC_RESULT_VAL2,0,0); "
##	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
##		WriteTrace "$errmsg" 2005
##		aidb_close $handle
##		return -1
##	}
##	aidb_commit $conn
##	aidb_close $handle
##	
##	set DEC_TARGET_VAL1 [format "%.2f" [expr (${DEC_RESULT_VAL1} /1.00000/ ${DEC_RESULT_VAL2})]]    
##	puts $DEC_TARGET_VAL1
##	#--判断
##        #--异常
##        #--1：80％ ≤ IMEI数占在网用户数比例 ≤ 105％超标 
##	if {$DEC_TARGET_VAL1>1.05 || $DEC_TARGET_VAL1<0.8} {
##		set grade 2
##	        set alarmcontent "准确性指标Z6超出集团考核范围"
##	        WriteAlarm $app_name $opmonth $grade ${alarmcontent}
##        }                                                                                                                                                                               
##
	return 0
}