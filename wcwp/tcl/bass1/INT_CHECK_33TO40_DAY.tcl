#################################################################
#程序名称: INT_CHECK_33TO40_DAY.tcl
#功能描述: 
#规则编号：33,34,35,38,39,40
#规则属性：指标异动
#规则类型：日
#运行粒度：日
#指标摘要：33：全球通月离网率
#          34：神州行月离网率
#          35：动感地带月离网率
#          38: 全球通用户月新增率
#          39: 神州行用户月新增率
#          40: 动感地带用户月新增率
#规则描述：33：定义：全球通月离网率 = 本月全球通用户离网数 /（本月天数×12/本年天数）/ 本月全球通用户到达数 x 100%
#              规则：全球通月离网率 ≤ 5%
#          34：定义：神州行月离网率 = 本月神州行用户离网数/（本月天数×12/本年天数）/ 本月神州行用户到达数 x 100%
#			    规则：神州行月离网率 ≤ 10%
#          35：定义：动感地带月离网率 = 本月动感地带离网用户数/（本月天数×12/本年天数）/ 本月动感地带用户到达数 x 100%
#              规则：动感地带月离网率 ≤ 15%
#          38: 定义：全球通用户月新增率 = (本月全球通用户新增数 / 本月全球通用户到达数) x 100%
#              规则：全球通用户月新增率 ≤ 5%
#          39: 定义：神州行用户月新增率 = (本月神州行用户新增数 / 本月神州行用户到达数) x 100%
#              规则：神州行用户月新增率 ≤ 10%
#          40: 定义：动感地带用户月新增率 = (本月动感地带用户新增数 / 本月动感地带用户到达数) x 100%
#              规则：动感地带用户月新增率 ≤ 20%
#校验对象：1.BASS1.G_A_02004_DAY  用户
#          2.BASS1.G_A_02008_DAY  用户状态
#输入参数:							
#输出参数: 返回值:0 成功;-1 失败							
#编 写 人：王琦							
#编写时间：2007-03-22							
#问题记录：1.							
#修改历史: 1.	20091125 取消现有规则校验
#################################################################							
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

	global env

	set Optime $op_time
	set p_optime $op_time
	set Timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
	set op_month [string range $op_time 0 3][string range $op_time 5 6]
        append op_time_month ${optime_month}-01
        set db_user $env(DB_USER)
        #求上个月 格式 yyyymm
        set last_month [GetLastMonth [string range $Timestamp 0 5]]
        #----求上月最后一天---#,格式 yyyymmdd
        set last_month_day [GetLastDay [string range $Timestamp 0 5]01]
        #puts $last_month
        
        #puts $last_day_month
         #----求本月天数-----#
        set thismonthdays [GetThisMonthDays ${op_month}01]
        puts $thismonthdays
        #----求本年天数-----#
        set thisyeardays [GetThisYearDays ${op_month}01]
        puts $thisyeardays
        set app_name "INT_CHECK_33TO40_DAY.tcl"
        set day [string range $op_time 8 9]
     
        #--删除本期数据
        set handle [aidb_open $conn]
	set sql_buff "\
		DELETE FROM $db_user.G_RULE_CHECK WHERE TIME_ID=$Timestamp
        AND RULE_CODE IN ('33','34','35','38','39','40')"
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	
	
#####	#判断日期
#####        if { $day < 26 } {
#####        	    	puts "今天 $day 号，未到23号，暂不处理"
#####        	    	return 0
#####        	        }
#####	
#####	#--DEC_CHECK_VALUE_4:本月全球通用户到达数
#####	set handle [aidb_open $conn]
#####        set sqlbuf "SELECT 
#####    		COUNT(*) 
#####    	FROM 
#####    		(SELECT
#####    		 	A.TIME_ID,
#####    		 	A.USER_ID,
#####    		 	A.USERTYPE_ID,
#####    		 	A.SIM_CODE
#####    		FROM BASS1.G_A_02004_DAY  A,
#####    		(SELECT USER_ID,MAX(TIME_ID) AS TIME_ID FROM BASS1.G_A_02004_DAY
#####    	 	 WHERE TIME_ID <= ${Timestamp} 
#####    	  	 GROUP BY USER_ID
#####    	     ) B
#####    	    WHERE A.USER_ID = B.USER_ID AND A.TIME_ID = B.TIME_ID AND A.BRAND_ID='1'    
#####    	    )T, 	
#####    	    
#####    		(SELECT
#####    	 		A.TIME_ID,
#####    	 		A.USER_ID,
#####    	 		A.USERTYPE_ID
#####    		 FROM BASS1.G_A_02008_DAY A,
#####    		(SELECT USER_ID,MAX(TIME_ID) AS TIME_ID FROM BASS1.G_A_02008_DAY
#####    		 WHERE TIME_ID <= ${Timestamp} 
#####    		 GROUP BY USER_ID
#####    	     ) B
#####    		WHERE A.USER_ID = B.USER_ID AND A.TIME_ID = B.TIME_ID 
#####    		) M		
#####   		WHERE T.USER_ID = M.USER_ID
#####    		  AND T.TIME_ID <= ${Timestamp}		
#####    		  AND M.TIME_ID <= ${Timestamp}		
#####    		  AND T.USERTYPE_ID <> '3'		
#####    		  AND T.SIM_CODE <> '1'		
#####    		  AND M.USERTYPE_ID NOT IN ('2010','2020','2030','1040','1021','9000');"
#####
#####	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
#####		WriteTrace $errmsg 001
#####		return -1
#####	}
#####	if [catch {set DEC_CHECK_VALUE_4 [lindex [aidb_fetch $handle] 0]} errmsg ] {
#####		WriteTrace $errmsg 1004
#####		return -1
#####	}
#####	aidb_commit $conn
#####	puts "$DEC_CHECK_VALUE_4"
#####	
#####	#--DEC_CHECK_VALUE_5:本月神州行用户到达数;
#####	set handle [aidb_open $conn]
#####        set sqlbuf "SELECT 
#####    		COUNT(*) 
#####    	FROM 
#####    		(SELECT
#####    		 	A.TIME_ID,
#####    		 	A.USER_ID,
#####    		 	A.USERTYPE_ID,
#####    		 	A.SIM_CODE
#####    		FROM BASS1.G_A_02004_DAY  A,
#####    		(SELECT USER_ID,MAX(TIME_ID) AS TIME_ID FROM BASS1.G_A_02004_DAY
#####    	 	 WHERE TIME_ID <= ${Timestamp} 
#####    	  	 GROUP BY USER_ID
#####    	     ) B
#####    	    WHERE A.USER_ID = B.USER_ID AND A.TIME_ID = B.TIME_ID AND A.BRAND_ID='2'    
#####    	    )T, 	
#####    	    
#####    		(SELECT
#####    	 		A.TIME_ID,
#####    	 		A.USER_ID,
#####    	 		A.USERTYPE_ID
#####    		 FROM BASS1.G_A_02008_DAY A,
#####    		(SELECT USER_ID,MAX(TIME_ID) AS TIME_ID FROM BASS1.G_A_02008_DAY
#####    		 WHERE TIME_ID <= ${Timestamp} 
#####    		 GROUP BY USER_ID
#####    	     ) B
#####    		WHERE A.USER_ID = B.USER_ID AND A.TIME_ID = B.TIME_ID 
#####    		) M		
#####   		WHERE T.USER_ID = M.USER_ID
#####    		  AND T.TIME_ID <= ${Timestamp}		
#####    		  AND M.TIME_ID <= ${Timestamp}		
#####    		  AND T.USERTYPE_ID <> '3'		
#####    		  AND T.SIM_CODE <> '1'		
#####    		  AND M.USERTYPE_ID NOT IN ('2010','2020','2030','1040','1021','9000');"
#####
#####	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
#####		WriteTrace $errmsg 001
#####		return -1
#####	}
#####	if [catch {set DEC_CHECK_VALUE_5 [lindex [aidb_fetch $handle] 0]} errmsg ] {
#####		WriteTrace $errmsg 1004
#####		return -1
#####	}
#####	aidb_commit $conn
#####	puts $DEC_CHECK_VALUE_5
#####	
#####	#--DEC_CHECK_VALUE_6:本月动感地带用户到达数;
#####	set handle [aidb_open $conn]
#####        set sqlbuf "SELECT 
#####    		COUNT(*) 
#####    	FROM 
#####    		(SELECT
#####    		 	A.TIME_ID,
#####    		 	A.USER_ID,
#####    		 	A.USERTYPE_ID,
#####    		 	A.SIM_CODE
#####    		FROM BASS1.G_A_02004_DAY  A,
#####    		(SELECT USER_ID,MAX(TIME_ID) AS TIME_ID FROM BASS1.G_A_02004_DAY
#####    	 	 WHERE TIME_ID <= ${Timestamp} 
#####    	  	 GROUP BY USER_ID
#####    	     ) B
#####    	    WHERE A.USER_ID = B.USER_ID AND A.TIME_ID = B.TIME_ID AND A.BRAND_ID='3'    
#####    	    )T, 	
#####    	    
#####    		(SELECT
#####    	 		A.TIME_ID,
#####    	 		A.USER_ID,
#####    	 		A.USERTYPE_ID
#####    		 FROM BASS1.G_A_02008_DAY A,
#####    		(SELECT USER_ID,MAX(TIME_ID) AS TIME_ID FROM BASS1.G_A_02008_DAY
#####    		 WHERE TIME_ID <= ${Timestamp} 
#####    		 GROUP BY USER_ID
#####    	     ) B
#####    		WHERE A.USER_ID = B.USER_ID AND A.TIME_ID = B.TIME_ID 
#####    		) M		
#####   		WHERE T.USER_ID = M.USER_ID
#####    		  AND T.TIME_ID <= ${Timestamp}		
#####    		  AND M.TIME_ID <= ${Timestamp}		
#####    		  AND T.USERTYPE_ID <> '3'		
#####    		  AND T.SIM_CODE <> '1'		
#####    		  AND M.USERTYPE_ID NOT IN ('2010','2020','2030','1040','1021','9000');"
#####
#####	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
#####		WriteTrace $errmsg 001
#####		return -1
#####	}
#####	if [catch {set DEC_CHECK_VALUE_6 [lindex [aidb_fetch $handle] 0]} errmsg ] {
#####		WriteTrace $errmsg 1004
#####		return -1
#####	}
#####	aidb_commit $conn
#####	puts $DEC_CHECK_VALUE_6
#####	#----------------------------------
#####	#--33：全球通月离网率
#####         #--本月全球通离网数
#####         set handle [aidb_open $conn]
#####
#####	 set sqlbuf "SELECT
##### 	    COUNT(DISTINCT A.USER_ID)
#####		FROM 
#####		    (
#####      	     SELECT
#####              A.TIME_ID,
#####              A.USER_ID,
#####              A.CREATE_DATE,
#####              A.PRODUCT_NO,
#####              A.USERTYPE_ID,
#####              A.SIM_CODE,
#####              A.BRAND_ID
#####             FROM BASS1.G_A_02004_DAY A,
#####             (SELECT USER_ID,MAX(TIME_ID) AS TIME_ID FROM BASS1.g_a_02004_day 
#####              WHERE TIME_ID<= ${Timestamp}
#####              GROUP BY USER_ID
#####             ) B
#####            WHERE A.USER_ID = B.USER_ID
#####     	      AND A.TIME_ID = B.TIME_ID
#####     	      AND A.BRAND_ID='1' 
#####	        ) A,
#####            BASS1.G_A_02008_DAY B
#####       WHERE A.USER_ID = B.USER_ID
#####          AND B.TIME_ID/100 = ${op_month}		
#####          AND A.USERTYPE_ID <> '3'		
#####          AND A.SIM_CODE <> '1'	
#####          AND B.USERTYPE_ID  IN ('2010','2020','2030');"
#####
#####        if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
#####		WriteTrace $errmsg 1001
#####		return -1
#####	}
#####
#####	if [catch {set DEC_CHECK_VALUE_1 [lindex [aidb_fetch $handle] 0]} errmsg ] {
#####		WriteTrace $errmsg 1002
#####		return -1
#####	}
#####
#####	aidb_commit $conn
#####	
#####        set DEC_RESULT_VAL1 [format "%.2f" [expr (${DEC_CHECK_VALUE_1}/1.0000/(${thismonthdays} *12.0 /${thisyeardays}))]]
#####        
#####        #--数据清零
#####        set DEC_CHECK_VALUE_1 "0"
#####        #--本月全球通用户到达数
#####        set DEC_RESULT_VAL2 "$DEC_CHECK_VALUE_4"
#####        #set DEC_RESULT_VAL2 "100"
#####	
#####        #--本地统计校验值
#####       	
#####	set DEC_TARGET_VAL1 [format "%.2f" [expr (${DEC_RESULT_VAL1} /1.00000/ ${DEC_RESULT_VAL2})]]
#####	
#####
#####	puts ${DEC_TARGET_VAL1}
#####
#####	#--将校验值插入校验结果表
#####	set handle [aidb_open $conn]
#####	set sql_buff "INSERT INTO BASS1.G_RULE_CHECK VALUES (INT($Timestamp),'33',$DEC_RESULT_VAL1,$DEC_RESULT_VAL2,0,0); "
#####	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
#####		WriteTrace "$errmsg" 2005
#####		aidb_close $handle
#####		return -1
#####	}
#####	aidb_commit $conn
#####	#--判断
#####        #--异常：
#####        #1：全球通月离网率 ≤ 5%
#####        
#####        #set DEC_TARGET_VAL1 "0.19"
#####        set DEC_MM [format "%.2f" [expr (0.05 * ${day}/${thismonthdays}/1.0)]]
#####        
#####	if {${DEC_TARGET_VAL1}>${DEC_MM} } {
#####		set grade 2
#####	        set alarmcontent "准确性指标33：全球通月离网率超出集团考核范围"
#####	        WriteAlarm $app_name $p_optime $grade ${alarmcontent}
#####	         } 
#####	puts "33 finish"
#####	
#####	#---------------------------
#####	 #--34：神州行月离网率
#####          #--本月神州行离网数
#####         set handle [aidb_open $conn]
#####
#####	 set sqlbuf "SELECT
##### 	    COUNT(DISTINCT A.USER_ID)
#####		FROM 
#####		    (
#####      	     SELECT
#####              A.TIME_ID,
#####              A.USER_ID,
#####              A.CREATE_DATE,
#####              A.PRODUCT_NO,
#####              A.USERTYPE_ID,
#####              A.SIM_CODE,
#####              A.BRAND_ID
#####             FROM BASS1.G_A_02004_DAY A,
#####             (SELECT USER_ID,MAX(TIME_ID) AS TIME_ID FROM BASS1.g_a_02004_day 
#####              WHERE TIME_ID<= ${Timestamp}
#####              GROUP BY USER_ID
#####             ) B
#####            WHERE A.USER_ID = B.USER_ID
#####     	      AND A.TIME_ID = B.TIME_ID
#####     	      AND A.BRAND_ID='2' 
#####	        ) A,
#####            BASS1.G_A_02008_DAY B
#####       WHERE A.USER_ID = B.USER_ID
#####          AND B.TIME_ID/100 = ${op_month}		
#####          AND A.USERTYPE_ID <> '3'		
#####          AND A.SIM_CODE <> '1'	
#####          AND B.USERTYPE_ID  IN ('2010','2020','2030');"
#####
#####        if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
#####		WriteTrace $errmsg 1001
#####		return -1
#####	}
#####
#####	if [catch {set DEC_CHECK_VALUE_1 [lindex [aidb_fetch $handle] 0]} errmsg ] {
#####		WriteTrace $errmsg 1002
#####		return -1
#####	}
#####
#####	aidb_commit $conn
#####	
#####        set DEC_RESULT_VAL1 [format "%.2f" [expr (${DEC_CHECK_VALUE_1}/1.0000/(${thismonthdays} *12.0 /${thisyeardays}))]]
#####        
#####        #--数据清零
#####        set DEC_CHECK_VALUE_1 "0"
#####        #--本月神州行用户到达数
#####        set DEC_RESULT_VAL2 "$DEC_CHECK_VALUE_5"
#####        #set DEC_RESULT_VAL2 "100"
#####	
#####        #--本地统计校验值
#####       	
#####	set DEC_TARGET_VAL1 [format "%.2f" [expr (${DEC_RESULT_VAL1} /1.00000/ ${DEC_RESULT_VAL2})]]
#####	
#####
#####	puts ${DEC_TARGET_VAL1}
#####
#####	#--将校验值插入校验结果表
#####	set handle [aidb_open $conn]
#####	set sql_buff "INSERT INTO BASS1.G_RULE_CHECK VALUES (INT($Timestamp),'34',$DEC_RESULT_VAL1,$DEC_RESULT_VAL2,0,0); "
#####	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
#####		WriteTrace "$errmsg" 2005
#####		aidb_close $handle
#####		return -1
#####	}
#####	aidb_commit $conn
#####	#--判断
#####        #--异常：
#####        #1：神州行月离网率 ≤ 10%
#####        
#####        #set DEC_TARGET_VAL1 "0.19"
#####        set DEC_MM [format "%.2f" [expr (0.1 * ${day}/${thismonthdays}/1.0)]]
#####        
#####	if {${DEC_TARGET_VAL1}>${DEC_MM} } {
#####		set grade 2
#####	        set alarmcontent "准确性指标34：神州行月离网率超出集团考核范围"
#####	        WriteAlarm $app_name $p_optime $grade ${alarmcontent}
#####		
#####         } 
#####	puts "34 finish"
#####	#-----------------------------------------------
#####	#--35：动感地带月离网率
#####        #--本月动感地带离网数
#####         set handle [aidb_open $conn]
#####
#####	 set sqlbuf "SELECT
##### 	    COUNT(DISTINCT A.USER_ID)
#####		FROM 
#####		    (
#####      	     SELECT
#####              A.TIME_ID,
#####              A.USER_ID,
#####              A.CREATE_DATE,
#####              A.PRODUCT_NO,
#####              A.USERTYPE_ID,
#####              A.SIM_CODE,
#####              A.BRAND_ID
#####             FROM BASS1.G_A_02004_DAY A,
#####             (SELECT USER_ID,MAX(TIME_ID) AS TIME_ID FROM BASS1.g_a_02004_day 
#####              WHERE TIME_ID<= ${Timestamp}
#####              GROUP BY USER_ID
#####             ) B
#####            WHERE A.USER_ID = B.USER_ID
#####     	      AND A.TIME_ID = B.TIME_ID
#####     	      AND A.BRAND_ID='3' 
#####	        ) A,
#####            BASS1.G_A_02008_DAY B
#####       WHERE A.USER_ID = B.USER_ID
#####          AND B.TIME_ID/100 = ${op_month}		
#####          AND A.USERTYPE_ID <> '3'		
#####          AND A.SIM_CODE <> '1'	
#####          AND B.USERTYPE_ID  IN ('2010','2020','2030');"
#####
#####        if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
#####		WriteTrace $errmsg 1001
#####		return -1
#####	}
#####
#####	if [catch {set DEC_CHECK_VALUE_1 [lindex [aidb_fetch $handle] 0]} errmsg ] {
#####		WriteTrace $errmsg 1002
#####		return -1
#####	}
#####
#####	aidb_commit $conn
#####	
#####        set DEC_RESULT_VAL1 [format "%.2f" [expr (${DEC_CHECK_VALUE_1}/1.0000/(${thismonthdays} *12.0 /${thisyeardays}))]]
#####        
#####        #--数据清零
#####        set DEC_CHECK_VALUE_1 "0"
#####        #--本月动感地带用户到达数
#####        set DEC_RESULT_VAL2 "${DEC_CHECK_VALUE_6}"
#####        #set DEC_RESULT_VAL2 "100"
#####	
#####        #--本地统计校验值
#####       	
#####	set DEC_TARGET_VAL1 [format "%.2f" [expr (${DEC_RESULT_VAL1} /1.00000/ ${DEC_RESULT_VAL2})]]
#####	
#####
#####	puts ${DEC_TARGET_VAL1}
#####
#####	#--将校验值插入校验结果表
#####	set handle [aidb_open $conn]
#####	set sql_buff "INSERT INTO BASS1.G_RULE_CHECK VALUES (INT($Timestamp),'35',$DEC_RESULT_VAL1,$DEC_RESULT_VAL2,0,0); "
#####	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
#####		WriteTrace "$errmsg" 2005
#####		aidb_close $handle
#####		return -1
#####	}
#####	aidb_commit $conn
#####	#--判断
#####        #--异常：
#####        #1：动感地带月离网率 ≤ 10%
#####        
#####        #set DEC_TARGET_VAL1 "0.19"
#####        set DEC_MM [format "%.2f" [expr (0.15 * ${day}/${thismonthdays}/1.0)]]
#####        
#####	if {${DEC_TARGET_VAL1}>${DEC_MM} } {
#####		set grade 2
#####	        set alarmcontent "准确性指标35：动感地带月离网率超出集团考核范围"
#####	        WriteAlarm $app_name $p_optime $grade ${alarmcontent}
#####	          } 
#####	puts "35 finish"
#####	#------------------------------
#####	#--38: 全球通用户月新增率
#####   #--本月全球通新增用户数
#####   set handle [aidb_open $conn]
#####
#####	 set sqlbuf "SELECT
#####             COUNT(DISTINCT A.USER_ID)
#####           FROM 
#####           (
#####              SELECT
#####                A.TIME_ID,
#####                A.USER_ID,
#####                A.CREATE_DATE,
#####                A.PRODUCT_NO,
#####                A.USERTYPE_ID,
#####                A.SIM_CODE,
#####                A.BRAND_ID
#####              FROM BASS1.g_a_02004_day A,
#####             (SELECT USER_ID,MAX(TIME_ID) AS TIME_ID FROM BASS1.g_a_02004_day 
#####              WHERE TIME_ID <= ${Timestamp}
#####              GROUP BY USER_ID
#####             ) B
#####            WHERE A.USER_ID = B.USER_ID
#####           	 AND A.TIME_ID = B.TIME_ID AND A.BRAND_ID='1' 
#####           ) A
#####          WHERE A.TIME_ID/100 = ${op_month}			
#####           	AND A.USERTYPE_ID <> '3'			
#####           	AND A.SIM_CODE <> '1'		
#####           	AND INT(A.CREATE_DATE)/100 =${op_month};"
#####
#####        if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
#####		WriteTrace $errmsg 1001
#####		return -1
#####	}
#####
#####	if [catch {set DEC_RESULT_VAL1 [lindex [aidb_fetch $handle] 0]} errmsg ] {
#####		WriteTrace $errmsg 1002
#####		return -1
#####	}
#####
#####	
#####        #----本月全球通用户到达数
#####        set DEC_RESULT_VAL2 ${DEC_CHECK_VALUE_4}
#####        #set DEC_RESULT_VAL2 "100"
#####	
#####        #--本地统计校验值
#####       	
#####	set DEC_TARGET_VAL1 [format "%.2f" [expr (${DEC_RESULT_VAL1} /1.00000/ ${DEC_RESULT_VAL2})]]
#####	
#####
#####	puts ${DEC_TARGET_VAL1}
#####
#####	#--将校验值插入校验结果表
#####	set handle [aidb_open $conn]
#####	set sql_buff "INSERT INTO BASS1.G_RULE_CHECK VALUES (INT($Timestamp),'38',$DEC_RESULT_VAL1,$DEC_RESULT_VAL2,0,0); "
#####	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
#####		WriteTrace "$errmsg" 2005
#####		aidb_close $handle
#####		return -1
#####	}
#####	aidb_commit $conn
#####	
#####        set DEC_MM [format "%.2f" [expr (0.05 * ${day}/${thismonthdays}/1.0)]]
#####        
#####	if {${DEC_TARGET_VAL1}>${DEC_MM} } {
#####		set grade 2
#####	        set alarmcontent "准确性指标38：全球通用户月新增率超出集团考核范围"
#####	        WriteAlarm $app_name $p_optime $grade ${alarmcontent}
#####	         } 
#####	puts "38 finish"
#####	#------------------------
#####	#--39: 神州行用户月新增率
#####        #--本月神州行新增用户数
#####   set handle [aidb_open $conn]
#####
#####	 set sqlbuf "SELECT
#####             COUNT(DISTINCT A.USER_ID)
#####           FROM 
#####           (
#####              SELECT
#####                A.TIME_ID,
#####                A.USER_ID,
#####                A.CREATE_DATE,
#####                A.PRODUCT_NO,
#####                A.USERTYPE_ID,
#####                A.SIM_CODE,
#####                A.BRAND_ID
#####              FROM BASS1.g_a_02004_day A,
#####             (SELECT USER_ID,MAX(TIME_ID) AS TIME_ID FROM BASS1.g_a_02004_day 
#####              WHERE TIME_ID <= ${Timestamp}
#####              GROUP BY USER_ID
#####             ) B
#####            WHERE A.USER_ID = B.USER_ID
#####           	 AND A.TIME_ID = B.TIME_ID AND A.BRAND_ID='2' 
#####           ) A
#####          WHERE A.TIME_ID/100 = ${op_month}			
#####           	AND A.USERTYPE_ID <> '3'			
#####           	AND A.SIM_CODE <> '1'		
#####           	AND INT(A.CREATE_DATE)/100 =${op_month};"
#####
#####        if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
#####		WriteTrace $errmsg 1001
#####		return -1
#####	}
#####
#####	if [catch {set DEC_RESULT_VAL1 [lindex [aidb_fetch $handle] 0]} errmsg ] {
#####		WriteTrace $errmsg 1002
#####		return -1
#####	}
#####
#####	
#####        #----本月神州行用户到达数
#####        set DEC_RESULT_VAL2 ${DEC_CHECK_VALUE_5}
#####        #set DEC_RESULT_VAL2 "100"
#####	
#####        #--本地统计校验值
#####       	
#####	set DEC_TARGET_VAL1 [format "%.2f" [expr (${DEC_RESULT_VAL1} /1.00000/ ${DEC_RESULT_VAL2})]]
#####	
#####
#####	puts ${DEC_TARGET_VAL1}
#####
#####	#--将校验值插入校验结果表
#####	set handle [aidb_open $conn]
#####	set sql_buff "INSERT INTO BASS1.G_RULE_CHECK VALUES (INT($Timestamp),'39',$DEC_RESULT_VAL1,$DEC_RESULT_VAL2,0,0); "
#####	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
#####		WriteTrace "$errmsg" 2005
#####		aidb_close $handle
#####		return -1
#####	}
#####	aidb_commit $conn
#####	
#####        set DEC_MM [format "%.2f" [expr (0.1 * ${day}/${thismonthdays}/1.0)]]
#####        
#####	if {${DEC_TARGET_VAL1}>${DEC_MM} } {
#####		set grade 2
#####	        set alarmcontent "准确性指标39：神州行用户月新增率超出集团考核范围"
#####	        WriteAlarm $app_name $p_optime $grade ${alarmcontent}
#####	                 } 
#####	puts "39 finish"
#####	#---------------------------
#####	 #--40: 动感地带用户月新增率
#####   #--本月动感地带新增用户数
#####   set handle [aidb_open $conn]
#####
#####	 set sqlbuf "SELECT
#####             COUNT(DISTINCT A.USER_ID)
#####           FROM 
#####           (
#####              SELECT
#####                A.TIME_ID,
#####                A.USER_ID,
#####                A.CREATE_DATE,
#####                A.PRODUCT_NO,
#####                A.USERTYPE_ID,
#####                A.SIM_CODE,
#####                A.BRAND_ID
#####              FROM BASS1.g_a_02004_day A,
#####             (SELECT USER_ID,MAX(TIME_ID) AS TIME_ID FROM BASS1.g_a_02004_day 
#####              WHERE TIME_ID <= ${Timestamp}
#####              GROUP BY USER_ID
#####             ) B
#####            WHERE A.USER_ID = B.USER_ID
#####           	 AND A.TIME_ID = B.TIME_ID AND A.BRAND_ID='3' 
#####           ) A
#####          WHERE A.TIME_ID/100 = ${op_month}			
#####           	AND A.USERTYPE_ID <> '3'			
#####           	AND A.SIM_CODE <> '1'		
#####           	AND INT(A.CREATE_DATE)/100 =${op_month};"
#####
#####        if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
#####		WriteTrace $errmsg 1001
#####		return -1
#####	}
#####
#####	if [catch {set DEC_RESULT_VAL1 [lindex [aidb_fetch $handle] 0]} errmsg ] {
#####		WriteTrace $errmsg 1002
#####		return -1
#####	}
#####
#####	
#####        #----本月动感地带用户到达数
#####        set DEC_RESULT_VAL2 ${DEC_CHECK_VALUE_6}
#####        #set DEC_RESULT_VAL2 "100"
#####	
#####        #--本地统计校验值
#####       	
#####	set DEC_TARGET_VAL1 [format "%.2f" [expr (${DEC_RESULT_VAL1} /1.00000/ ${DEC_RESULT_VAL2})]]
#####	
#####
#####	puts ${DEC_TARGET_VAL1}
#####
#####	#--将校验值插入校验结果表
#####	set handle [aidb_open $conn]
#####	set sql_buff "INSERT INTO BASS1.G_RULE_CHECK VALUES (INT($Timestamp),'40',$DEC_RESULT_VAL1,$DEC_RESULT_VAL2,0,0); "
#####	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
#####		WriteTrace "$errmsg" 2005
#####		aidb_close $handle
#####		return -1
#####	}
#####	aidb_commit $conn
#####	
#####        set DEC_MM [format "%.2f" [expr (0.2 * ${day}/${thismonthdays}/1.0)]]
#####        
#####	if {${DEC_TARGET_VAL1}>${DEC_MM} } {
#####		set grade 2
#####	        set alarmcontent "准确性指标40：动感地带用户月新增率超出集团考核范围"
#####	        WriteAlarm $app_name $p_optime $grade ${alarmcontent}
#####		    } 
#####	puts "40 finish"
#####	
#####	
#####       
#####
#####	aidb_close $handle
#####
	return 0
}
