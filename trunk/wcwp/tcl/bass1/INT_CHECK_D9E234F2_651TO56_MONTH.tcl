#################################################################
#程序名称: INT_CHECK_D9E234F2_651TO56_MONTH.tcl
#规则编号：D9,E2,E3,E4,F2,F3,F4,F5,F6,51,52,53,54,55,56
#规则属性：业务逻辑
#规则类型：日
#指标摘要：D9: 新业务普及率
#          E2: 彩信付费用户数占比
#          E3: 主叫显示付费用户数占比
#          E4: GPRS付费客户数占比
#          F2: WAP业务收入占比
#          F3: 主叫显示业务收入占比
#          F4: 彩信业务收入占比
#          F5: 彩铃收入占比
#          F6: GPRS收入占比
#          51: 业务收入月变动率
#          52: 全球通业务收入月变动率             + 告警需处理
#          53: 神州行业务收入变动率               + 告警需处理
#          54: 动感地带业务收入月变动率           + 告警需处理
#          55: 新业务收入占比
#          56: 短信业务收入占比
#规则描述：D9: 70％ ≤ (本月新业务用户数 / 本月到达用户数) < 100%，且普及率较上月变动幅度 ≤ 5％
#          E2: 8% ≤ 彩信付费用户数/新业务付费用户数 ≤ 15%
#          E3: 主叫显示付费用户数/新业务付费用户数 > 40%
#          E4: 8% ≤ GPRS付费用用户数/新业务付费用户数 ≤ 20%
#          F2: （WAP业务收入占/新业务收入）月变动率 ≤ 30%
#          F3: 12% ≤ 主叫显示业务收入/新业务收入 ≤ 35%
#          F4: （彩信业务业务收入/新业务收入）月变动率 ≤ 30%
#          F5: （彩铃业务收入/新业务收入）月变动率 ≤ 30%
#          F6: （GPRS业务收入/新业务收入）月变动率 ≤ 30%
#        +  51: | (本月日平均业务收入 / 上月日平均业务收入 - 1) x 100% | ≤ 10%
#        +  52: | (本月全球通日平均业务收入 / 上月全球通日平均业务收入 - 1) x 100% | ≤ 10%
#        +  53: | (本月神州行日平均业务收入 / 上月神州行日平均业务收入 - 1) x 100% | ≤ 10%
#        +  54: | (本月动感地带日平均业务收入 / 上月动感地带日平均业务收入 - 1) x 100% | ≤ 20%
#          55: 15％ ≤ (本月新业务收入 / 本月业务收入) ≤ 25%
#          56: 20％ ≤ (本月短信业务收入 / 本月新业务收入) ≤ 65%
#校验对象：
#          BASS1.G_S_03004_MONTH
#          BASS1.G_S_03005_MONTH
#          BASS1.G_S_03012_MONTH
#          BASS1.G_S_04006_DAY
#          BASS1.G_A_02004_DAY
#          BASS1.G_A_02008_DAY
#输出参数:                        
# 返回值:   0 成功; -1 失败       
#***************************************************/

proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

	#本月 yyyymm
	set op_month [string range $optime_month 0 3][string range $optime_month 5 6]
	#本月 yyyy-mm
	set opmonth $optime_month	
	#上月 yyyy-mm
	set last_month [GetLastMonth [string range $op_month 0 5]]
	#本月最后一天 yyyymmdd
        set this_month_last_day [string range $op_month 0 5][GetThisMonthDays [string range $op_month 0 5]01]	     
        #本月天数
        set this_month_days [GetThisMonthDays ${op_month}01]
        #本年天数 
        set this_year_days [GetThisYearDays ${op_month}01]
        #上月天数
        set last_month_days [GetThisMonthDays ${last_month}01]
        #上月最后一天 yyyymmdd
        set this_month_first_day [string range $optime_month 0 3][string range $optime_month 5 6]01
        set last_month_last_day [GetLastDay [string range $this_month_first_day 0 7]]   
        #程序名称
        set app_name "INT_CHECK_D9E234F2_651TO56_MONTH.tcl"
        
        
        
        set handle [aidb_open $conn]
	set sql_buff "\
            delete from bass1.g_rule_check where time_id=$op_month
              and rule_code in ('D9','E2','E3','E4','F2','F3','F4','F5','F6','51','52','53','54','55','56')"
        puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
        aidb_close $handle
       
       
        #--DEC_CHECK_VALUE_1:本月新业务收入
        #--DEC_CHECK_VALUE_3:上月新业务收入
        #--DEC_CHECK_VALUE_5:新业务付费用户数
        #--DEC_CHECK_VALUE_2:本月业务收入
        #--DEC_CHECK_VALUE_4:上月业务收入
        #--本月新业务收入
       
        set handle [aidb_open $conn]
        set sqlbuf "SELECT
                     SUM(T.CNT)
                   FROM 
                     (
      		            SELECT COALESCE(SUM(BIGINT(FEE_RECEIVABLE)),0) AS CNT FROM BASS1.G_S_03004_MONTH
      		            WHERE TIME_ID =$op_month
      	                    AND (ACCT_ITEM_ID IN ('0405','0407') OR INT (ACCT_ITEM_ID)/100 IN (5,6,7))
      	                    UNION ALL
                        SELECT COALESCE(SUM(BIGINT(INCM_AMT)),0) AS CNT FROM BASS1.G_S_03012_MONTH
      		            WHERE TIME_ID =$op_month
      	                     AND (ACCT_ITEM_ID IN ('0405','0407')OR INT (ACCT_ITEM_ID)/100 IN (5,6,7))
      	             ) T"
        puts $sqlbuf
	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
		WriteTrace $errmsg 001
		return -1
	}
	if [catch {set DEC_CHECK_VALUE_1 [lindex [aidb_fetch $handle] 0]} errmsg ] {
		WriteTrace $errmsg 1004
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
	
	puts "本月新业务收入:$DEC_CHECK_VALUE_1" 

	#--本月业务收入
	set handle [aidb_open $conn]
        set sqlbuf "\
            SELECT
              SUM(T.CNT)
            FROM 
               (
            	  SELECT SUM(BIGINT(SHOULD_FEE)) AS CNT FROM BASS1.G_S_03005_MONTH
                  WHERE TIME_ID = $op_month
                      AND ITEM_ID IN ('0100','0200','0300','0400','0500','0600','0700','0900')
                   UNION ALL
                  SELECT SUM(BIGINT(INCM_AMT)) AS CNT FROM BASS1.G_S_03012_MONTH
                  WHERE TIME_ID =$op_month
               ) T"
        puts $sqlbuf
	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
		WriteTrace $errmsg 001
		return -1
	}
	if [catch {set DEC_CHECK_VALUE_2 [lindex [aidb_fetch $handle] 0]} errmsg ] {
		WriteTrace $errmsg 1004
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
	puts "本月业务收入:$DEC_CHECK_VALUE_2"
	
	#--上月新业务收入
	set handle [aidb_open $conn]
        set sqlbuf "SELECT
                     SUM(T.CNT)
                   FROM 
                     (
      		            SELECT COALESCE(SUM(BIGINT(FEE_RECEIVABLE)),0) AS CNT FROM BASS1.G_S_03004_MONTH
      		            WHERE TIME_ID =$last_month
      	                    AND (ACCT_ITEM_ID IN ('0405','0407') OR INT (ACCT_ITEM_ID)/100 IN (5,6,7))
      	                     UNION ALL
                        SELECT COALESCE(SUM(BIGINT(INCM_AMT)),0) AS CNT FROM BASS1.G_S_03012_MONTH
      		            WHERE TIME_ID =$last_month
      	                     AND (ACCT_ITEM_ID IN ('0405','0407')OR INT (ACCT_ITEM_ID)/100 IN (5,6,7))
      	             ) T"
        puts $sqlbuf
	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
		WriteTrace $errmsg 001
		return -1
	}
	if [catch {set DEC_CHECK_VALUE_3 [lindex [aidb_fetch $handle] 0]} errmsg ] {
		WriteTrace $errmsg 1004
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
	puts "上月新业务收入:$DEC_CHECK_VALUE_3"	

	#----上月业务收入
	set handle [aidb_open $conn]
        set sqlbuf "SELECT
              SUM(T.CNT)
            FROM 
               (
            	  SELECT SUM(BIGINT(SHOULD_FEE)) AS CNT FROM BASS1.G_S_03005_MONTH
                  WHERE TIME_ID = INT($last_month)
                      AND ITEM_ID IN ('0100','0200','0300','0400','0500','0600','0700','0900')
                  UNION ALL
                  SELECT SUM(BIGINT(INCM_AMT)) AS CNT FROM BASS1.G_S_03012_MONTH
                  WHERE TIME_ID =INT($last_month)
               ) T"
        puts $sqlbuf
	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
		WriteTrace $errmsg 001
		return -1
	}
	if [catch {set DEC_CHECK_VALUE_4 [lindex [aidb_fetch $handle] 0]} errmsg ] {
		WriteTrace $errmsg 1004
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
	puts "上月业务收入:$DEC_CHECK_VALUE_4"

	#--DEC_CHECK_VALUE_5:新业务付费用户数
	set handle [aidb_open $conn]
        set sqlbuf "SELECT
               COUNT(DISTINCT A.USER_ID)
             FROM 
             (
             	SELECT A.USER_ID FROM BASS1.G_S_03004_MONTH A
             	WHERE BIGINT(A.FEE_RECEIVABLE)>0
             	    AND A.TIME_ID =$op_month
             	    AND (A.ACCT_ITEM_ID IN ('0405','0407') OR INT(A.ACCT_ITEM_ID)/100 IN (5,6,7))
             	UNION ALL
			    SELECT A.USER_ID FROM BASS1.G_S_03012_MONTH A
			    WHERE A.TIME_ID =$op_month
			        AND BIGINT(A.INCM_AMT)>0
			        AND (A.ACCT_ITEM_ID IN ('0405','0407') OR INT(A.ACCT_ITEM_ID)/100 IN (5,6,7))
			  ) A,  
             (
             	SELECT A.TIME_ID,A.USER_ID,A.USERTYPE_ID,A.SIM_CODE
                FROM BASS1.G_A_02004_DAY A,
                    (    
            		  SELECT USER_ID,MAX(TIME_ID) AS TIME_ID FROM BASS1.G_A_02004_DAY
                      WHERE TIME_ID <=$this_month_last_day
                      GROUP BY USER_ID
            		 ) B
                WHERE A.USER_ID = B.USER_ID
                   AND A.TIME_ID = B.TIME_ID
             )B
            WHERE A.USER_ID = B.USER_ID
            	 AND B.USERTYPE_ID <> '3'
            	 AND B.SIM_CODE <> '1';"
        puts $sqlbuf
	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
		WriteTrace $errmsg 001
		return -1
	}
	if [catch {set DEC_CHECK_VALUE_5 [lindex [aidb_fetch $handle] 0]} errmsg ] {
		WriteTrace $errmsg 1004
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
	
	puts "新业务付费用户数:$DEC_CHECK_VALUE_5"                  

#	#D9: 新业务普及率
#        #本月新业务普及率*100
#        #分别从KPI表中取出(本月新业务用户数 / 本月到达用户数)
#        #本月新业务用户数/本月到达用户数
#        #本月新业务收入
#         
#        set DEC_CHECK_VALUE_7 [exec get_kpi.sh $this_month_last_day 2 2]
#        set DEC_RESULT_VAL1 [format "%.2f" [expr ${DEC_CHECK_VALUE_5}/1.00/${DEC_CHECK_VALUE_7} *100]] 
#        
#        puts "本月到达用户数：$DEC_CHECK_VALUE_7"
#        puts "本月新业务普及率：$DEC_RESULT_VAL1"
#        
#        #--数据清零
#        set $DEC_CHECK_VALUE_7 "0";
#        
#        #上月新业务用户数/上月到达用户数
#        set DEC_CHECK_VALUE_6 [exec get_kpi.sh $last_month 7 2];
#        puts "上月新业务用户数:$DEC_CHECK_VALUE_6"
#        set DEC_CHECK_VALUE_7 [exec get_kpi.sh $last_month_last_day 2 2]
#        puts "上月到达用户数:$DEC_CHECK_VALUE_7"        
#        set DEC_RESULT_VAL2 [format "%.2f" [expr ${DEC_CHECK_VALUE_6}/1.00000/${DEC_CHECK_VALUE_7}*100.000]] 
#        
#        #将校验值插入校验结果表
#	set handle [aidb_open $conn]
#	set sql_buff "INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'D9',$DEC_RESULT_VAL1,$DEC_RESULT_VAL2,0,0); "
#	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
#		WriteTrace "$errmsg" 2005
#		aidb_close $handle
#		return -1
#	}
#	aidb_commit $conn
#	aidb_close $handle
#	
#	#判断
#        #D9:：70％ ≤ (本月新业务用户数 / 本月到达用户数) < 100%，且普及率较上月变动幅度 ≤ 5％超标
#	if {${DEC_RESULT_VAL1}<70 || ${DEC_RESULT_VAL1}>=100 || ${DEC_RESULT_VAL1}-${DEC_RESULT_VAL2}>5 || ${DEC_RESULT_VAL1}-${DEC_RESULT_VAL2}<-5} {
#	           set grade 2
#	           set alarmcontent "准确性指标D9超出集团考核范围"
#	           WriteAlarm $app_name $opmonth $grade ${alarmcontent}
#        } 
#       puts "D9 end"  	

#       #--E2: 彩信付费用户数占比
#       #--彩信付费用户数
#       set handle [aidb_open $conn]
#        set sqlbuf "SELECT
#              COUNT(DISTINCT T.USER_ID)
#            FROM 
#            (
#              SELECT DISTINCT USER_ID FROM BASS1.G_S_03004_MONTH
#              WHERE TIME_ID=$op_month AND ACCT_ITEM_ID='0615' AND BIGINT(FEE_RECEIVABLE)>0
#              UNION ALL
#              SELECT DISTINCT USER_ID FROM BASS1.G_S_03012_MONTH
#              WHERE TIME_ID=$op_month AND ACCT_ITEM_ID='0615' AND BIGINT(INCM_AMT)>0
#             )T"
#        puts $sqlbuf
#	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
#		WriteTrace $errmsg 001
#		return -1
#	}
#	if [catch {set DEC_RESULT_VAL1 [lindex [aidb_fetch $handle] 0]} errmsg ] {
#		WriteTrace $errmsg 1004
#		return -1
#	}
#	aidb_commit $conn
#	aidb_close $handle
#	puts "彩信付费用户数:$DEC_RESULT_VAL1"
#	
#	#--新业务付费用户数
#	set DEC_RESULT_VAL2 $DEC_CHECK_VALUE_5
#
#	#--将校验值插入校验结果表
#	set handle [aidb_open $conn]
#	set sql_buff "INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'E2',$DEC_RESULT_VAL1,$DEC_RESULT_VAL2,0,0); "
#	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
#		WriteTrace "$errmsg" 2005
#		aidb_close $handle
#		return -1
#	}
#	aidb_commit $conn
#	aidb_close $handle
#	#--判断
#        #--异常
#        #--1：8% ≤ 彩信付费用户数/新业务付费用户数 ≤ 15%超标
#        set DEC_RESULT [format "%.3f" [expr ${DEC_RESULT_VAL1} /1.00 /${DEC_RESULT_VAL2}]] 
#	if {${DEC_RESULT}<0.08 || ${DEC_RESULT}>0.15} {
#	           set grade 2
#	           set alarmcontent "准确性指标E2超出集团考核范围"
#	           WriteAlarm $app_name $opmonth $grade ${alarmcontent}
#        } 
#         puts "E2 end"       
#
#       #--E3: 彩信付费用户数占比
#       #--主叫显示付费用户数
#       set handle [aidb_open $conn]
#        set sqlbuf "SELECT
#              COUNT(DISTINCT T.USER_ID)
#            FROM 
#            (
#              SELECT DISTINCT USER_ID FROM BASS1.G_S_03004_MONTH
#              WHERE TIME_ID=$op_month AND ACCT_ITEM_ID='0501' AND BIGINT(FEE_RECEIVABLE)>0
#              UNION ALL
#              SELECT DISTINCT USER_ID FROM BASS1.G_S_03012_MONTH
#              WHERE TIME_ID=$op_month AND ACCT_ITEM_ID='0501' AND BIGINT(INCM_AMT)>0
#             )T"
#        puts $sqlbuf
#	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
#		WriteTrace $errmsg 001
#		return -1
#	}
#	if [catch {set DEC_RESULT_VAL1 [lindex [aidb_fetch $handle] 0]} errmsg ] {
#		WriteTrace $errmsg 1004
#		return -1
#	}
#	aidb_commit $conn
#	aidb_close $handle
#	puts "主叫显示付费用户数:$DEC_RESULT_VAL1"
#	
#	
#	set DEC_RESULT_VAL2 $DEC_CHECK_VALUE_5
#
#	#--将校验值插入校验结果表
#	set handle [aidb_open $conn]
#	set sql_buff "INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'E3',$DEC_RESULT_VAL1,$DEC_RESULT_VAL2,0,0); "
#	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
#		WriteTrace "$errmsg" 2005
#		aidb_close $handle
#		return -1
#	}
#	aidb_commit $conn
#	aidb_close $handle
#	#--判断
#        #--异常
#        #--1：主叫显示付费用户数/新业务付费用户数 > 40%超标
#        set DEC_RESULT [format "%.3f" [expr ${DEC_RESULT_VAL1} /1.00 /${DEC_RESULT_VAL2}]] 
#        set handle [aidb_open $conn]
#	if {${DEC_RESULT}<=0.4} {
#	           set grade 2
#	           set alarmcontent "准确性指标E3超出集团考核范围"
#	           WriteAlarm $app_name $opmonth $grade ${alarmcontent}
#        } 
#         puts "E3 end"         
#
#         #--E4: GPRS付费客户数占比
#       #--GPRS付费用用户数
#       set handle [aidb_open $conn]
#        set sqlbuf "SELECT
#              COUNT(DISTINCT T.USER_ID)
#            FROM 
#            (
#              SELECT DISTINCT USER_ID FROM BASS1.G_S_03004_MONTH
#              WHERE TIME_ID=$op_month AND ACCT_ITEM_ID IN ('0617','0621','0622','0623','0624') AND BIGINT(FEE_RECEIVABLE)>0
#              UNION ALL
#              SELECT DISTINCT USER_ID FROM BASS1.G_S_03012_MONTH
#              WHERE TIME_ID=$op_month AND ACCT_ITEM_ID IN ('0617','0621','0622','0623','0624') AND BIGINT(INCM_AMT)>0
#             )T"
#        puts $sqlbuf
#	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
#		WriteTrace $errmsg 001
#		return -1
#	}
#	if [catch {set DEC_RESULT_VAL1 [lindex [aidb_fetch $handle] 0]} errmsg ] {
#		WriteTrace $errmsg 1004
#		return -1
#	}
#	aidb_commit $conn
#	aidb_close $handle
#	puts "$DEC_RESULT_VAL1"
#	
#	
#	set DEC_RESULT_VAL2 $DEC_CHECK_VALUE_5
#	#set DEC_RESULT_VAL2 "50"
#	#--将校验值插入校验结果表
#	set handle [aidb_open $conn]
#	set sql_buff "INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'E4',$DEC_RESULT_VAL1,$DEC_RESULT_VAL2,0,0); "
#	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
#		WriteTrace "$errmsg" 2005
#		aidb_close $handle
#		return -1
#	}
#	aidb_commit $conn
#	aidb_close $handle
#	#--判断
#        #--异常
#        #--1：8% ≤ GPRS付费用用户数/新业务付费用户数 ≤20%超标
#        set DEC_RESULT [format "%.3f" [expr ${DEC_RESULT_VAL1} /1.00 /${DEC_RESULT_VAL2}]] 
#	if {${DEC_RESULT}<0.08 ||${DEC_RESULT}>0.20 } {
#	           set grade 2
#	           set alarmcontent "准确性指标E4超出集团考核范围"
#	           WriteAlarm $app_name $opmonth $grade ${alarmcontent}
#        } 
#         puts "E4 end"         
#
#       #--F2: WAP业务收入占比
#       #--本月占比*100.0
#       #--WAP业务收入
#       set handle [aidb_open $conn]
#        set sqlbuf "SELECT SUM(T.SR)
#            FROM 
#            (
#              SELECT SUM(BIGINT(FEE_RECEIVABLE))AS SR
#              FROM  BASS1.G_S_03004_MONTH 
#              WHERE TIME_ID=$op_month AND ACCT_ITEM_ID IN ('0617','0618','0638')
#              UNION ALL
#              SELECT  SUM(BIGINT(INCM_AMT)) AS SR
#              FROM BASS1.G_S_03012_MONTH
#              WHERE TIME_ID=$op_month AND ACCT_ITEM_ID IN ('0617','0618','0638')
#            )T"
#        puts $sqlbuf
#	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
#		WriteTrace $errmsg 001
#		return -1
#	}
#	if [catch {set DEC_RESULT_VAL1 [lindex [aidb_fetch $handle] 0]} errmsg ] {
#		WriteTrace $errmsg 1004
#		return -1
#	}
#	aidb_commit $conn
#	aidb_close $handle
#	puts "$DEC_RESULT_VAL1"
#	#set DEC_CHECK_VALUE_1 "50"
#	puts "$DEC_CHECK_VALUE_1"
#	puts "($DEC_RESULT_VAL1 /1.0000/$DEC_CHECK_VALUE_1* 100.000)"
#	set DEC_RESULT_VAL1 [format "%.2f" [expr ($DEC_RESULT_VAL1 /1.0000/$DEC_CHECK_VALUE_1* 100.000)]] 
#	puts $DEC_RESULT_VAL1
#	puts "1111111"
#	#--上月占比*100.0
#        #--WAP业务收入
#        set handle [aidb_open $conn]
#        set sqlbuf "SELECT SUM(T.SR)
#            FROM 
#            (
#              SELECT SUM(BIGINT(FEE_RECEIVABLE))AS SR
#              FROM  BASS1.G_S_03004_MONTH 
#              WHERE TIME_ID=INT($last_month) AND ACCT_ITEM_ID IN ('0617','0618','0638')
#              UNION
#              SELECT  SUM(BIGINT(INCM_AMT)) AS SR
#              FROM BASS1.G_S_03012_MONTH
#              WHERE TIME_ID=INT($last_month) AND ACCT_ITEM_ID IN ('0617','0618','0638')
#            )T"
#
#	puts "222222"
#	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
#		WriteTrace $errmsg 001
#		return -1
#	}
#	if [catch {set DEC_RESULT_VAL2 [lindex [aidb_fetch $handle] 0]} errmsg ] {
#		WriteTrace $errmsg 1004
#		return -1
#	}
#	aidb_commit $conn
#	aidb_close $handle
#	puts "3333333"
#	
#	
#	puts $DEC_RESULT_VAL2
#        #set DEC_RESULT_VAL2 "45"
#	#set DEC_CHECK_VALUE_3 "50.0"
#	set DEC_RESULT_VAL2 [format "%.2f" [expr (${DEC_RESULT_VAL2} /1.0000/${DEC_CHECK_VALUE_3} * 100.00)]]  
#	#set DEC_RESULT_VAL2 "0.8"
#	
#	
#	puts $DEC_RESULT_VAL2
#	#--将校验值插入校验结果表
#	set handle [aidb_open $conn]
#	set sql_buff "INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'F2',$DEC_RESULT_VAL1,$DEC_RESULT_VAL2,0,0); "
#	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
#		WriteTrace "$errmsg" 2005
#		aidb_close $handle
#		return -1
#	}
#	aidb_commit $conn
#	aidb_close $handle
#	#--判断
#        #--异常
#        #--F2：（WAP业务收入占/新业务收入）月变动率 ≤ 30%超标
#        set DEC_RESULT [format "%.3f" [expr ${DEC_RESULT_VAL1} /1.00 /${DEC_RESULT_VAL2} - 1]] 
#	if {${DEC_RESULT}<-0.3 ||${DEC_RESULT}>0.3 } {
#	           set grade 2
#	           set alarmcontent "准确性指标F2超出集团考核范围"
#	           WriteAlarm $app_name $opmonth $grade ${alarmcontent}
#        } 
#         puts "F2 end"      
#
#       #--F3: 主叫显示业务收入占比
#       #-- 主叫显示业务收入
#       set handle [aidb_open $conn]
#        set sqlbuf "SELECT SUM(T.SR)
#            FROM 
#            (
#              SELECT SUM(BIGINT(FEE_RECEIVABLE))AS SR
#              FROM  BASS1.G_S_03004_MONTH 
#              WHERE TIME_ID=$op_month AND ACCT_ITEM_ID='0501'
#              UNION ALL
#              SELECT  SUM(BIGINT(INCM_AMT)) AS SR
#              FROM BASS1.G_S_03012_MONTH
#              WHERE TIME_ID=$op_month AND ACCT_ITEM_ID='0501'
#            )T"
#
#	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
#		WriteTrace $errmsg 001
#		return -1
#	}
#	if [catch {set DEC_RESULT_VAL1 [lindex [aidb_fetch $handle] 0]} errmsg ] {
#		WriteTrace $errmsg 1004
#		return -1
#	}
#	aidb_commit $conn
#	aidb_close $handle
#	puts "主叫显示业务收入:$DEC_RESULT_VAL1"
#	#set DEC_CHECK_VALUE_1 "50"
#	set DEC_RESULT_VAL1 [format "%.2f" [expr (${DEC_RESULT_VAL1} /1.00 /100.00)]] 
#	
#	#--新业务收入
#	#set DEC_CHECK_VALUE_1 "50"
#	set DEC_RESULT_VAL2 [format "%.2f" [expr (${DEC_CHECK_VALUE_1} /1.00 /100.00)]]  
#	#set DEC_RESULT_VAL2 "0.8"
#	
#	
#	puts $DEC_RESULT_VAL2
#	#--将校验值插入校验结果表
#	set handle [aidb_open $conn]
#	set sql_buff "INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'F3',$DEC_RESULT_VAL1,$DEC_RESULT_VAL2,0,0); "
#	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
#		WriteTrace "$errmsg" 2005
#		aidb_close $handle
#		return -1
#	}
#	aidb_commit $conn
#	aidb_close $handle
#	#--判断
#        #--异常
#        #--F3：12% ≤ 主叫显示业务收入/新业务收入 ≤ 35%超标
#        set DEC_RESULT [format "%.3f" [expr ${DEC_RESULT_VAL1} /1.00 /${DEC_RESULT_VAL2}]] 
#
#	if {${DEC_RESULT}<0.12 ||${DEC_RESULT}>0.35 } {
#	           set grade 2
#	           set alarmcontent "准确性指标F3超出集团考核范围"
#	           WriteAlarm $app_name $opmonth $grade ${alarmcontent}
#        } 
#         puts "F3 end"  
#
#         #--F4: 彩信业务收入占比
#         #--本月占比*100
#       set handle [aidb_open $conn]
#        set sqlbuf "SELECT SUM(T.SR)
#            FROM 
#            (
#              SELECT SUM(BIGINT(FEE_RECEIVABLE))AS SR
#              FROM  BASS1.G_S_03004_MONTH 
#              WHERE TIME_ID=$op_month AND ACCT_ITEM_ID IN ('0615','0637')
#              UNION
#              SELECT  SUM(BIGINT(INCM_AMT)) AS SR
#              FROM BASS1.G_S_03012_MONTH
#              WHERE TIME_ID=$op_month AND ACCT_ITEM_ID IN ('0615','0637')
#            )T"
#
#	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
#		WriteTrace $errmsg 001
#		return -1
#	}
#	if [catch {set DEC_RESULT_VAL1 [lindex [aidb_fetch $handle] 0]} errmsg ] {
#		WriteTrace $errmsg 1004
#		return -1
#	}
#	aidb_commit $conn
#	aidb_close $handle
#	puts "本月彩信业务收入占比:$DEC_RESULT_VAL1"
#	#set DEC_CHECK_VALUE_1 "50"
#	set DEC_RESULT_VAL1 [format "%.2f" [expr (${DEC_RESULT_VAL1} /1.00 /${DEC_CHECK_VALUE_1} * 100)]] 
#	
#	#--上月占比*100.0
#       
#        set handle [aidb_open $conn]
#        set sqlbuf "SELECT SUM(T.SR)
#            FROM 
#            (
#              SELECT SUM(BIGINT(FEE_RECEIVABLE))AS SR
#              FROM  BASS1.G_S_03004_MONTH 
#              WHERE TIME_ID=INT($last_month) AND ACCT_ITEM_ID IN ('0615','0637')
#              UNION
#              SELECT  SUM(BIGINT(INCM_AMT)) AS SR
#              FROM BASS1.G_S_03012_MONTH
#              WHERE TIME_ID=INT($last_month) AND ACCT_ITEM_ID IN ('0615','0637')
#            )T"
#
#	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
#		WriteTrace $errmsg 001
#		return -1
#	}
#	if [catch {set DEC_RESULT_VAL2 [lindex [aidb_fetch $handle] 0]} errmsg ] {
#		WriteTrace $errmsg 1004
#		return -1
#	}
#	aidb_commit $conn
#	aidb_close $handle
#	
#	
#	puts "上月彩信业务收入占比:$DEC_RESULT_VAL2"
#	set DEC_RESULT_VAL2 [format "%.2f" [expr (${DEC_RESULT_VAL2} /1.00 /${DEC_CHECK_VALUE_3} * 100)]]  
#	
#	
#	puts $DEC_RESULT_VAL2
#	#--将校验值插入校验结果表
#	set handle [aidb_open $conn]
#	set sql_buff "INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'F4',$DEC_RESULT_VAL1,$DEC_RESULT_VAL2,0,0); "
#	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
#		WriteTrace "$errmsg" 2005
#		aidb_close $handle
#		return -1
#	}
#	aidb_commit $conn
#	aidb_close $handle
#	#--判断
#        #--异常
#        #--1：（彩信业务业务收入/新业务收入）月变动率 ≤ 30%超标
#        set DEC_RESULT [format "%.3f" [expr ${DEC_RESULT_VAL1} /1.00 /${DEC_RESULT_VAL2} - 1]] 
#
#	if {${DEC_RESULT}<-0.30 ||${DEC_RESULT}>0.30 } {
#	           set grade 2
#	           set alarmcontent "准确性指标F4超出集团考核范围"
#	           WriteAlarm $app_name $opmonth $grade ${alarmcontent}
#        } 
#         puts "F4 end"                   
#         
#         #--F5: 彩铃收入占比
#      #--本月占比*100
#       set handle [aidb_open $conn]
#        set sqlbuf "SELECT SUM(T.SR)
#            FROM 
#            (
#              SELECT SUM(BIGINT(FEE_RECEIVABLE))AS SR
#              FROM  BASS1.G_S_03004_MONTH 
#              WHERE TIME_ID=$op_month AND ACCT_ITEM_ID IN ('0519','0639')
#              UNION
#              SELECT  SUM(BIGINT(INCM_AMT)) AS SR
#              FROM BASS1.G_S_03012_MONTH
#              WHERE TIME_ID=$op_month AND ACCT_ITEM_ID IN ('0519','0639')
#            )T"
#        puts $sqlbuf
#	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
#		WriteTrace $errmsg 001
#		return -1
#	}
#	if [catch {set DEC_RESULT_VAL1 [lindex [aidb_fetch $handle] 0]} errmsg ] {
#		WriteTrace $errmsg 1004
#		return -1
#	}
#	aidb_commit $conn
#	aidb_close $handle
#	puts "本月彩铃收入：$DEC_RESULT_VAL1"
#	#set DEC_CHECK_VALUE_1 "50"
#	
#	set DEC_RESULT_VAL1 [format "%.2f" [expr (${DEC_RESULT_VAL1} /1.00 /${DEC_CHECK_VALUE_1} * 100.00)]] 
#	
#	#--上月占比*100.0
#       
#        set handle [aidb_open $conn]
#        set sqlbuf "SELECT SUM(T.SR)
#            FROM 
#            (
#              SELECT SUM(BIGINT(FEE_RECEIVABLE))AS SR
#              FROM  BASS1.G_S_03004_MONTH 
#              WHERE TIME_ID=INT($last_month) AND ACCT_ITEM_ID IN ('0519','0639')
#              UNION
#              SELECT  SUM(BIGINT(INCM_AMT)) AS SR
#              FROM BASS1.G_S_03012_MONTH
#              WHERE TIME_ID=INT($last_month) AND ACCT_ITEM_ID IN ('0615','0637')
#            )T"
#        puts $sqlbuf
#	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
#		WriteTrace $errmsg 001
#		return -1
#	}
#	if [catch {set DEC_RESULT_VAL2 [lindex [aidb_fetch $handle] 0]} errmsg ] {
#		WriteTrace $errmsg 1004
#		return -1
#	}
#	aidb_commit $conn	
#	aidb_close $handle
#	
#	puts "上月彩铃收入：$DEC_RESULT_VAL2"
#	set DEC_RESULT_VAL2 [format "%.2f" [expr (${DEC_RESULT_VAL2} /1.00 /${DEC_CHECK_VALUE_3} * 100)]]     
#	
#	
#	#--将校验值插入校验结果表
#	set handle [aidb_open $conn]
#	set sql_buff "INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'F5',$DEC_RESULT_VAL1,$DEC_RESULT_VAL2,0,0); "
#	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
#		WriteTrace "$errmsg" 2005
#		aidb_close $handle
#		return -1
#	}
#	aidb_commit $conn
#	aidb_close $handle
#	#--判断
#        #--异常
#        #--1：（彩铃业务收入/新业务收入）月变动率 ≤ 30%超标
#        set DEC_RESULT [format "%.3f" [expr ${DEC_RESULT_VAL1} /1.00 /${DEC_RESULT_VAL2} - 1]] 
#
#	if {${DEC_RESULT}<-0.30 ||${DEC_RESULT}>0.30 } {
#	           set grade 2
#	           set alarmcontent "准确性指标F5超出集团考核范围"
#	           WriteAlarm $app_name $opmonth $grade ${alarmcontent}
#        } 
#         puts "F5 end"     
#
#         #--F6: （GPRS业务收入/新业务收入）月变动率 ≤ 30%
#         #--本月占比*100
#       set handle [aidb_open $conn]
#        set sqlbuf "SELECT SUM(T.SR)
#            FROM 
#            (
#              SELECT SUM(BIGINT(FEE_RECEIVABLE))AS SR
#              FROM  BASS1.G_S_03004_MONTH 
#              WHERE TIME_ID=$op_month AND ACCT_ITEM_ID IN ('0617','0621','0622','0623','0624')
#              UNION
#              SELECT  SUM(BIGINT(INCM_AMT)) AS SR
#              FROM BASS1.G_S_03012_MONTH
#              WHERE TIME_ID=$op_month AND ACCT_ITEM_ID IN ('0617','0621','0622','0623','0624')
#            )T"
#
#	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
#		WriteTrace $errmsg 001
#		return -1
#	}
#	if [catch {set DEC_RESULT_VAL1 [lindex [aidb_fetch $handle] 0]} errmsg ] {
#		WriteTrace $errmsg 1004
#		return -1
#	}
#	aidb_commit $conn
#	aidb_close $handle
#	set DEC_RESULT_VAL1 [format "%.2f" [expr (${DEC_RESULT_VAL1} /1.00 /${DEC_CHECK_VALUE_1} * 100)]] 
#	
#	#--上月占比*100.0
#       
#        set handle [aidb_open $conn]
#        set sqlbuf "SELECT SUM(T.SR)
#            FROM 
#            (
#              SELECT SUM(BIGINT(FEE_RECEIVABLE))AS SR
#              FROM  BASS1.G_S_03004_MONTH 
#              WHERE TIME_ID=INT($last_month) AND ACCT_ITEM_ID IN ('0617','0621','0622','0623','0624')
#              UNION
#              SELECT  SUM(BIGINT(INCM_AMT)) AS SR
#              FROM BASS1.G_S_03012_MONTH
#              WHERE TIME_ID=INT($last_month) AND ACCT_ITEM_ID IN ('0617','0621','0622','0623','0624')
#            )T"
#
#	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
#		WriteTrace $errmsg 001
#		return -1
#	}
#	if [catch {set DEC_RESULT_VAL2 [lindex [aidb_fetch $handle] 0]} errmsg ] {
#		WriteTrace $errmsg 1004
#		return -1
#	}
#	aidb_commit $conn
#	aidb_close $handle
#	
#	
#	puts $DEC_RESULT_VAL2
#	set DEC_RESULT_VAL2 [format "%.2f" [expr (${DEC_RESULT_VAL2} /1.00 /${DEC_CHECK_VALUE_3} * 100)]]  
#
#	
#	
#	puts $DEC_RESULT_VAL2
#	#--将校验值插入校验结果表
#	set handle [aidb_open $conn]
#	set sql_buff "INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'F6',$DEC_RESULT_VAL1,$DEC_RESULT_VAL2,0,0); "
#	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
#		WriteTrace "$errmsg" 2005
#		aidb_close $handle
#		return -1
#	}
#	aidb_commit $conn
#	aidb_close $handle
#	#--判断
#        #--异常
#        #--1：（GPRS业务收入/新业务收入）月变动率 ≤ 30%超标
#        set DEC_RESULT [format "%.3f" [expr ${DEC_RESULT_VAL1} /1.00 /${DEC_RESULT_VAL2} - 1]] 
#
#	if {${DEC_RESULT}<-0.30 ||${DEC_RESULT}>0.30 } {
#	           set grade 2
#	           set alarmcontent "准确性指标F6超出集团考核范围"
#	           WriteAlarm $app_name $opmonth $grade ${alarmcontent}
#        } 
#         puts "F6 end"
#        
        #--51: 业务收入月变动率
        #--本月日平均业务收入
        set DEC_RESULT_VAL1 [format "%.3f" [expr ${DEC_CHECK_VALUE_2} /1.00 /${this_month_days}/100.00]] 
        #--上月日平均业务收入
        set DEC_RESULT_VAL2 [format "%.3f" [expr ${DEC_CHECK_VALUE_4} /1.00 /${last_month_days}/100.00]] 
        
        #--将校验值插入校验结果表
	set handle [aidb_open $conn]
	set sql_buff "INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'51',$DEC_RESULT_VAL1,$DEC_RESULT_VAL2,0,0); "
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
	#--判断
        #--异常
        #--1：| (本月日平均业务收入 / 上月日平均业务收入 - 1) x 100% | ≤ 10%超标
        set DEC_RESULT [format "%.3f" [expr ${DEC_RESULT_VAL1} /1.00 /${DEC_RESULT_VAL2} - 1]] 

	if {${DEC_RESULT}<-0.30 ||${DEC_RESULT}>0.30 } {
	           set grade 2
	           set alarmcontent "准确性指标51超出集团考核范围"
	           WriteAlarm $app_name $opmonth $grade ${alarmcontent}
        } 
         puts "51 end"        

         #--52: 全球通业务收入月变动率
         #--本月全球通日平均业务收入
         set handle [aidb_open $conn]
        set sqlbuf "SELECT
            SUM(T.CNT)
            FROM 
               (
                 SELECT A.CNT FROM
                 (
            	   SELECT USER_ID,SUM(BIGINT(SHOULD_FEE)) AS CNT FROM BASS1.G_S_03005_MONTH
                   WHERE TIME_ID = $op_month
                   GROUP BY USER_ID
                  ) A,
                  (
 	                 SELECT A.TIME_ID,A.USER_ID,A.USERTYPE_ID,A.SIM_CODE,A.BRAND_ID
                     FROM BASS1.G_A_02004_DAY A,
                          (
                            SELECT USER_ID,MAX(TIME_ID) AS TIME_ID FROM BASS1.G_A_02004_DAY
                            WHERE TIME_ID <=$this_month_last_day
                            GROUP BY USER_ID
                           )B
                     WHERE A.USER_ID = B.USER_ID
                     AND A.TIME_ID = B.TIME_ID
 	               )B
	              WHERE A.USER_ID = B.USER_ID
                    AND B.BRAND_ID='1'
 	           )T"

	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
		WriteTrace $errmsg 001
		return -1
	}
	if [catch {set DEC_RESULT_VAL1 [lindex [aidb_fetch $handle] 0]} errmsg ] {
		WriteTrace $errmsg 1004
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
	puts "$DEC_RESULT_VAL1"
	#set DEC_RESULT_VAL1 "50"
	set DEC_RESULT_VAL1 [format "%.2f" [expr (${DEC_RESULT_VAL1} /1.00 /${this_month_days}/100)]] 
         
        #上月全球通日平均业务收入
        set handle [aidb_open $conn]
        set sqlbuf "SELECT
            SUM(T.CNT)
            FROM 
               (
                 SELECT A.CNT FROM
                 (
            	   SELECT USER_ID,SUM(BIGINT(SHOULD_FEE)) AS CNT FROM BASS1.G_S_03005_MONTH
                   WHERE TIME_ID = INT($last_month)
                   GROUP BY USER_ID
                  ) A,
                  (
 	                 SELECT A.TIME_ID,A.USER_ID,A.USERTYPE_ID,A.SIM_CODE,A.BRAND_ID
                     FROM BASS1.G_A_02004_DAY A,
                          (
                            SELECT USER_ID,MAX(TIME_ID) AS TIME_ID FROM BASS1.G_A_02004_DAY
                            WHERE TIME_ID <=$last_month_last_day
                            GROUP BY USER_ID
                           )B
                     WHERE A.USER_ID = B.USER_ID
                     AND A.TIME_ID = B.TIME_ID
 	               )B
	              WHERE A.USER_ID = B.USER_ID
                    AND B.BRAND_ID='1'
 	           )T"
 	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
		WriteTrace $errmsg 001
		return -1
	}
	if [catch {set DEC_RESULT_VAL2 [lindex [aidb_fetch $handle] 0]} errmsg ] {
		WriteTrace $errmsg 1004
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
	puts "$DEC_RESULT_VAL2"
	#set DEC_RESULT_VAL2 "50"
	set DEC_RESULT_VAL2 [format "%.2f" [expr (${DEC_RESULT_VAL2} /1.00 /${last_month_days}/100)]] 
	 #--将校验值插入校验结果表
 	set handle [aidb_open $conn]
 	set sql_buff "INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'52',$DEC_RESULT_VAL1,$DEC_RESULT_VAL2,0,0); "
 	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
 		WriteTrace "$errmsg" 2005
 		aidb_close $handle
 		return -1
 	}
 	aidb_commit $conn
	aidb_close $handle
 	#--判断
         #--异常
         #--1：| (本月全球通日平均业务收入 / 上月全球通日平均业务收入 - 1) x 100% | ≤ 10%超标
         set DEC_RESULT [format "%.3f" [expr ${DEC_RESULT_VAL1} /1.00 /${DEC_RESULT_VAL2} - 1]] 

 	if {${DEC_RESULT}<-0.10 ||${DEC_RESULT}>0.10 } {
	           set grade 2
	           set alarmcontent "准确性指标52超出集团考核范围"
	           WriteAlarm $app_name $opmonth $grade ${alarmcontent}
         } 
          puts "52 end"
 
         #--53: 神州行业务收入月变动率
         #--本月神州行日平均业务收入
         set handle [aidb_open $conn]
        set sqlbuf "SELECT
            SUM(A.CNT)
            FROM 
               (
            	SELECT USER_ID,COALESCE(SUM(BIGINT(SHOULD_FEE)),0) AS CNT FROM BASS1.G_S_03005_MONTH
                WHERE TIME_ID = $op_month
                GROUP BY USER_ID   
 	            UNION ALL
            	SELECT USER_ID,SUM(BIGINT(INCM_AMT)) AS CNT FROM BASS1.G_S_03012_MONTH
                WHERE TIME_ID = $op_month
                GROUP BY USER_ID
               ) A,
              (
 	            SELECT A.TIME_ID,A.USER_ID,A.USERTYPE_ID,A.SIM_CODE,A.BRAND_ID
                FROM BASS1.G_A_02004_DAY A,
                  (
                    SELECT USER_ID,MAX(TIME_ID) AS TIME_ID FROM BASS1.G_A_02004_DAY
                    WHERE TIME_ID <=$this_month_last_day
                    GROUP BY USER_ID
                   )B
                 WHERE A.USER_ID = B.USER_ID
                     AND A.TIME_ID = B.TIME_ID
 	           )B
	          WHERE A.USER_ID = B.USER_ID
                    AND B.BRAND_ID='2';"

	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
		WriteTrace $errmsg 001
		return -1
	}
	if [catch {set DEC_RESULT_VAL1 [lindex [aidb_fetch $handle] 0]} errmsg ] {
		WriteTrace $errmsg 1004
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
	puts "$DEC_RESULT_VAL1"
	#set DEC_RESULT_VAL1 "50"
	set DEC_RESULT_VAL1 [format "%.2f" [expr (${DEC_RESULT_VAL1} /1.00 /${this_month_days}/100)]] 
         
        #--上月神州行日平均业务收入
        set handle [aidb_open $conn]
        set sqlbuf "SELECT
            SUM(A.CNT)
            FROM 
               (
            	SELECT USER_ID,COALESCE(SUM(BIGINT(SHOULD_FEE)),0) AS CNT FROM BASS1.G_S_03005_MONTH
                WHERE TIME_ID = INT($last_month)
                GROUP BY USER_ID   
 	            UNION ALL
            	SELECT USER_ID,SUM(BIGINT(INCM_AMT)) AS CNT FROM BASS1.G_S_03012_MONTH
                WHERE TIME_ID = INT($last_month)
                GROUP BY USER_ID
               ) A,
              (
 	            SELECT A.TIME_ID,A.USER_ID,A.USERTYPE_ID,A.SIM_CODE,A.BRAND_ID
                FROM BASS1.G_A_02004_DAY A,
                  (
                    SELECT USER_ID,MAX(TIME_ID) AS TIME_ID FROM BASS1.G_A_02004_DAY
                    WHERE TIME_ID <=$last_month_last_day
                    GROUP BY USER_ID
                   )B
                 WHERE A.USER_ID = B.USER_ID
                     AND A.TIME_ID = B.TIME_ID
 	           )B
	          WHERE A.USER_ID = B.USER_ID
                    AND B.BRAND_ID='2';"
 	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
		WriteTrace $errmsg 001
		return -1
	}
	if [catch {set DEC_RESULT_VAL2 [lindex [aidb_fetch $handle] 0]} errmsg ] {
		WriteTrace $errmsg 1004
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
	puts "$DEC_RESULT_VAL2"
	#set DEC_RESULT_VAL2 "50"
	set DEC_RESULT_VAL2 [format "%.2f" [expr (${DEC_RESULT_VAL2} /1.00 /${last_month_days}/100)]] 
	 #--将校验值插入校验结果表
 	set handle [aidb_open $conn]
 	set sql_buff "INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'53',$DEC_RESULT_VAL1,$DEC_RESULT_VAL2,0,0); "
 	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
 		WriteTrace "$errmsg" 2005
 		aidb_close $handle
 		return -1
 	}
 	aidb_commit $conn
	aidb_close $handle
 	#--判断
         #--异常
         #--1：| (本月神州行日平均业务收入 / 上月神州行日平均业务收入 - 1) x 100% | ≤ 10%超标
         set DEC_RESULT [format "%.3f" [expr ${DEC_RESULT_VAL1} /1.00 /${DEC_RESULT_VAL2} - 1]]  

 	if {${DEC_RESULT}<-0.10 ||${DEC_RESULT}>0.10 } {
	           set grade 2
	           set alarmcontent "准确性指标53超出集团考核范围"
	           WriteAlarm $app_name $opmonth $grade ${alarmcontent}
         } 
          puts "53 end"
          
          #--54: 动感地带业务收入月变动率
         #--本月动感地带日平均业务收入
         set handle [aidb_open $conn]
        set sqlbuf "SELECT
            SUM(T.CNT)
            FROM 
               (
                 SELECT A.CNT FROM
                 (
            	   SELECT USER_ID,SUM(BIGINT(SHOULD_FEE)) AS CNT FROM BASS1.G_S_03005_MONTH
                   WHERE TIME_ID = $op_month
                   GROUP BY USER_ID
                  ) A,
                  (
 	                 SELECT A.TIME_ID,A.USER_ID,A.USERTYPE_ID,A.SIM_CODE,A.BRAND_ID
                     FROM BASS1.G_A_02004_DAY A,
                          (
                            SELECT USER_ID,MAX(TIME_ID) AS TIME_ID FROM BASS1.G_A_02004_DAY
                            WHERE TIME_ID <=$this_month_last_day
                            GROUP BY USER_ID
                           )B
                     WHERE A.USER_ID = B.USER_ID
                     AND A.TIME_ID = B.TIME_ID
 	               )B
	              WHERE A.USER_ID = B.USER_ID
                    AND B.BRAND_ID='3'
 	           )T"

	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
		WriteTrace $errmsg 001
		return -1
	}
	if [catch {set DEC_RESULT_VAL1 [lindex [aidb_fetch $handle] 0]} errmsg ] {
		WriteTrace $errmsg 1004
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
	puts "$DEC_RESULT_VAL1"
	#set DEC_RESULT_VAL1 "50"
	set DEC_RESULT_VAL1 [format "%.2f" [expr (${DEC_RESULT_VAL1} /1.00 /${this_month_days}/100)]] 
         
        #----上月动感地带日平均业务收入
        set handle [aidb_open $conn]
        set sqlbuf "SELECT
            SUM(T.CNT)
            FROM 
               (
                 SELECT A.CNT FROM
                 (
            	   SELECT USER_ID,SUM(BIGINT(SHOULD_FEE)) AS CNT FROM BASS1.G_S_03005_MONTH
                   WHERE TIME_ID = $last_month
                   GROUP BY USER_ID
                  ) A,
                  (
 	                 SELECT A.TIME_ID,A.USER_ID,A.USERTYPE_ID,A.SIM_CODE,A.BRAND_ID
                     FROM BASS1.G_A_02004_DAY A,
                          (
                            SELECT USER_ID,MAX(TIME_ID) AS TIME_ID FROM BASS1.G_A_02004_DAY
                            WHERE TIME_ID <=$last_month_last_day
                            GROUP BY USER_ID
                           )B
                     WHERE A.USER_ID = B.USER_ID
                     AND A.TIME_ID = B.TIME_ID
 	               )B
	              WHERE A.USER_ID = B.USER_ID
                    AND B.BRAND_ID='3'
 	           )T"
 	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
		WriteTrace $errmsg 001
		return -1
	}
	if [catch {set DEC_RESULT_VAL2 [lindex [aidb_fetch $handle] 0]} errmsg ] {
		WriteTrace $errmsg 1004
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
	puts "$DEC_RESULT_VAL2"
	#set DEC_RESULT_VAL2 "50"
	set DEC_RESULT_VAL2 [format "%.2f" [expr (${DEC_RESULT_VAL2} /1.00 /${last_month_days}/100)]] 
	 #--将校验值插入校验结果表
 	set handle [aidb_open $conn]
 	set sql_buff "INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'54',$DEC_RESULT_VAL1,$DEC_RESULT_VAL2,0,0); "
 	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
 		WriteTrace "$errmsg" 2005
 		aidb_close $handle
 		return -1
 	}
 	aidb_commit $conn
	aidb_close $handle
 	#--判断
         #--异常
         #--1： (本月动感地带日平均业务收入 / 上月动感地带日平均业务收入 - 1) x 100% | ≤ 20%超标
         set DEC_RESULT [format "%.3f" [expr ${DEC_RESULT_VAL1} /1.00 /${DEC_RESULT_VAL2} - 1]] 

 	if {${DEC_RESULT}<-0.20 ||${DEC_RESULT}>0.20 } {
	           set grade 2
	           set alarmcontent "准确性指标54超出集团考核范围"
	           WriteAlarm $app_name $opmonth $grade ${alarmcontent}
         } 
          puts "54 end"
          
           #--55: 新业务收入占比
           #--本月新业务收入
           #set DEC_CHECK_VALUE_1 "50"
           #set DEC_CHECK_VALUE_2 "50"
           set DEC_RESULT_VAL1 [format "%.2f" [expr (${DEC_CHECK_VALUE_1} /1.00 /${this_month_days}/100)]] 
           #--本月业务收入
           set DEC_RESULT_VAL2 [format "%.2f" [expr (${DEC_CHECK_VALUE_2} /1.00 /${last_month_days}/100)]] 
	 #--将校验值插入校验结果表
 	set handle [aidb_open $conn]
 	set sql_buff "INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'55',$DEC_RESULT_VAL1,$DEC_RESULT_VAL2,0,0); "
 	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
 		WriteTrace "$errmsg" 2005
 		aidb_close $handle
 		return -1
 	}
 	aidb_commit $conn
	aidb_close $handle
 	 #--判断
         #--异常
         #--1： (15％ ≤ (本月新业务收入 / 本月业务收入) ≤ 25%超标
         set DEC_RESULT [format "%.3f" [expr ${DEC_RESULT_VAL1} /1.00 /${DEC_RESULT_VAL2} ]] 

 	if {${DEC_RESULT}<0.15 ||${DEC_RESULT}>0.25 } {
	           set grade 2
	           set alarmcontent "准确性指标55超出集团考核范围"
	           WriteAlarm $app_name $opmonth $grade ${alarmcontent}
         } 
          puts "55 end"
          
          #--56: 短信业务收入占比
         #--本月短信业务收入
         set handle [aidb_open $conn]
        set sqlbuf "SELECT
            (A.CNT+B.CNT)/100.0
          FROM 
            (
      		   SELECT COALESCE(SUM(BIGINT(FEE_RECEIVABLE)),0) AS CNT FROM BASS1.G_S_03004_MONTH
      		   WHERE TIME_ID =$op_month
      	           AND (ACCT_ITEM_ID IN ('0405','0407') OR INT (ACCT_ITEM_ID)/100 IN (5,6,7))
      	           AND ACCT_ITEM_ID IN ('0601','0603','0605','0606','0607','0609','0611','0613')
      	     ) A,
            (
               SELECT COALESCE(SUM(BIGINT(INCM_AMT)),0) AS CNT FROM BASS1.G_S_03012_MONTH
      		   WHERE TIME_ID =$op_month
      	            AND (ACCT_ITEM_ID IN ('0405','0407')OR INT (ACCT_ITEM_ID)/100 IN (5,6,7))
      	            AND ACCT_ITEM_ID IN ('0601','0603','0605','0606','0607','0609','0611','0613')
      	    ) B"

	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
		WriteTrace $errmsg 001
		return -1
	}
	if [catch {set DEC_RESULT_VAL1 [lindex [aidb_fetch $handle] 0]} errmsg ] {
		WriteTrace $errmsg 1004
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
	puts "$DEC_RESULT_VAL1"
	         
        #----本月新业务收入
	set DEC_RESULT_VAL2 [format "%.2f" [expr (${DEC_CHECK_VALUE_1} /1.00 /100)]] 
	 #--将校验值插入校验结果表
 	set handle [aidb_open $conn]
 	set sql_buff "INSERT INTO BASS1.G_RULE_CHECK VALUES ($op_month,'56',$DEC_RESULT_VAL1,$DEC_RESULT_VAL2,0,0); "
 	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
 		WriteTrace "$errmsg" 2005
 		aidb_close $handle
 		return -1
 	}
 	aidb_commit $conn
	aidb_close $handle
 	#--判断
         #--异常
         #--1： 20％ ≤ (本月短信业务收入 / 本月新业务收入) ≤ 65%超标
         set DEC_RESULT [format "%.3f" [expr ${DEC_RESULT_VAL1} /1.00 /${DEC_RESULT_VAL2} ]] 

 	if {${DEC_RESULT}<0.2  ||${DEC_RESULT}>0.65 } {
	           set grade 2
	           set alarmcontent "准确性指标56超出集团考核范围"
	           WriteAlarm $app_name $opmonth $grade ${alarmcontent}
         } 
          puts "56 end"
            
#################################################      
	return 0
}