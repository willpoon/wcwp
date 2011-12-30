#################################################################
#程序名称: INT_CHECK_36_MONTH.tcl
#功能描述: 
#规则编号：36
#规则属性：指标异动
#规则类型：月
#运行粒度：月
#指标摘要：36: 大客户月离网率
#规则描述：36: 本月离网大客户数 /（本月天数×12/本年天数）/ 本月用户到达数 ≤ 3%
#校验对象：1.BASS1.G_A_02004_DAY  用户
#          2.BASS1.G_A_02008_DAY  用户状态
#          3.BASS1.G_I_02005_MONTH 大客户
#输入参数:							
#输出参数: 返回值:0 成功;-1 失败							
#编 写 人：王琦							
#编写时间：2007-03-22							
#问题记录：1.							
#修改历史: 1.							
#################################################################							
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

        #本月 yyyymm
        set op_month [string range $optime_month 0 3][string range $optime_month 5 6]
        #当月 yyyy-mm
        set opmonth $optime_month
        #上月  yyyymm
        set last_month [GetLastMonth [string range $op_month 0 5]]
        #本月最后一天 yyyymmdd
        set this_month_last_day [string range $op_month 0 5][GetThisMonthDays [string range $op_month 0 5]01]
        #本年天数 
        set this_year_days [GetThisYearDays ${op_month}01]
        #本月天数
        set this_month_days [GetThisMonthDays ${op_month}01]        
        #程序名称
        set app_name "INT_CHECK_36_MONTH.tcl"
        
        #--删除本期数据
        set handle [aidb_open $conn]
	set sql_buff "\
		delete from bass1.g_rule_check where time_id=$op_month
                     and rule_code='36'"
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle

#########################################36: 大客户月离网率################################
        #--本月离网大客户数
        set handle [aidb_open $conn]
        set sqlbuf "\
	 SELECT 
	   SUM(P.RC) 
	 FROM 
         (	
    	   SELECT 
             COUNT(USER_ID) AS RC
    	   FROM 
    	     BASS1.G_I_02005_MONTH
    	   WHERE 
    	     TIME_ID=${last_month}
    	     AND USER_ID IN 
    		         (
    		           SELECT
    	 		     A.USER_ID
    		           FROM 
    		             BASS1.G_A_02008_DAY A,
    		             (SELECT USER_ID,MAX(TIME_ID) AS TIME_ID FROM BASS1.G_A_02008_DAY
    		              WHERE TIME_ID<= $this_month_last_day
    		              GROUP BY USER_ID
    	                     ) B
    		           WHERE 
    		             A.USER_ID = B.USER_ID AND A.TIME_ID = B.TIME_ID  
    			     AND A.USERTYPE_ID IN ('2010','2020','2030')
    		          ) 
    	   UNION ALL
    	   SELECT 
             COUNT(*) AS RC
    	   FROM 
    	     ( SELECT USER_ID FROM BASS1.G_I_02005_MONTH WHERE TIME_ID=${op_month}
    	       EXCEPT
    	       SELECT USER_ID FROM BASS1.G_I_02005_MONTH WHERE TIME_ID=${last_month}
    	      )T
    	   WHERE 
    	     USER_ID IN 
    	     	      (
    	     	        SELECT
    	 		  A.USER_ID
    		        FROM 
    		          BASS1.G_A_02008_DAY A,
    		          (SELECT USER_ID,MAX(TIME_ID) AS TIME_ID FROM BASS1.G_A_02008_DAY
    		           WHERE TIME_ID<= $this_month_last_day
    		           GROUP BY USER_ID
    	                  ) B
    		        WHERE 
    		           A.USER_ID = B.USER_ID AND A.TIME_ID = B.TIME_ID  
    			   AND A.USERTYPE_ID IN ('2010','2020','2030')
    		      )
    	  )P;"
        if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
		WriteTrace $errmsg 1001
		return -1
	}
	if [catch {set DEC_CHECK_VALUE_1 [lindex [aidb_fetch $handle] 0]} errmsg ] {
		WriteTrace $errmsg 1002
		return -1
	}

	aidb_commit $conn
	aidb_close $handle	  
        
        set DEC_RESULT_VAL1 [format "%.2f" [expr (${DEC_CHECK_VALUE_1}/1.0000/${this_month_days} *12 /${this_year_days})]]
        
        ##--本月用户到达数
        set DEC_RESULT_VAL2 [exec get_kpi.sh $this_month_last_day 2 2]
        
        #--本地统计校验值
        set DEC_TARGET_VAL1 [format "%.2f" [expr (${DEC_RESULT_VAL1} /1.00000/ ${DEC_RESULT_VAL2})]]

	#--将校验值插入校验结果表
	set handle [aidb_open $conn]
	set sql_buff "INSERT INTO BASS1.G_RULE_CHECK VALUES (INT($op_month),'36',$DEC_RESULT_VAL1,$DEC_RESULT_VAL2,$DEC_TARGET_VAL1,0); "
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
	
	#--判断
        #--异常：
        #1：本月离网大客户数 /（本月天数×12/本年天数）/ 本月用户到达数 
        
	if { ${DEC_TARGET_VAL1}>0.03 } {
		set grade 2
	        set alarmcontent "准确性指标36：大客户月离网率超出集团考核范围"
	        WriteAlarm $app_name $opmonth $grade ${alarmcontent}
	  }         
	return 0
}
