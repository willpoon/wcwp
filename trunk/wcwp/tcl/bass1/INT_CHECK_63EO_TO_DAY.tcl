#################################################################
# 程序名称: INT_CHECK_63EO_TO_DAY.tcl
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
#规则编号：63,E0
#规则属性：业务逻辑
#规则类型：日
#指标摘要：63: 人均普通短信通信量
#          E0：点对点短信普及率
#规则描述：63: 45 ≤ 本月普通短信通信量/本月普通短信用户数 ≤ 200 （条/户）
#          E0：40％ < (本月点对点短信使用用户数 / 本月到达用户数) < 90%，
#              且普及率较上月变动幅度 ≤ 10％
#校验对象：
#          BASS1.G_A_02004_DAY       用户
#          BASS1.G_A_02008_DAY       用户状态
#          BASS1.G_S_21008_TO_DAY    普通短信业务月使用(日使用中间表)
#输出参数:
# 返回值:   0 成功; -1 失败
#################################################################
proc Deal { op_time p_optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

	global env
	set p_optime $op_time
	set p_timestamp [string range $op_time 0 3][string range $op_time 5 6][string range $op_time 8 9]
	set this_month [string range $op_time 0 3][string range $op_time 5 6]
	set db_user $env(DB_USER)
###	#求上个月 格式 yyyymm
###        set last_month [GetLastMonth [string range $p_timestamp 0 5]]
###        #puts $last_month
###        #----求上月最后一天---#,格式 yyyymmdd
###        set last_day_month [GetLastDay [string range $p_timestamp 0 5]01]
###        #puts $last_day_month
###         ##--求昨天，格式yyyymmdd--##
###        set yesterday [GetLastDay ${p_timestamp}]
###        #puts $yesterday
###        
###        ##今天的日期，格式dd(例：输入20070411 返回11)
###        set today_dd [format "%.0f" [string range $p_timestamp 6 7]]
###        #puts $today_dd
###
###        #程序名称
###        set APP_NAME "INT_CHECK_63EO_TO_DAY.tcl"
###
###
####删除本期数据
###        set handle [aidb_open $conn]
###	set sql_buff "
###		DELETE FROM $db_user.G_RULE_CHECK WHERE TIME_ID=$p_timestamp
###    	              AND RULE_CODE IN ('63','E0') "
###	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
###		WriteTrace "$errmsg" 2005
###		aidb_close $handle
###		return -1
###	}
###	aidb_commit $conn
###	aidb_close $handle
###	
####################################63: 人均普通短信通信量############
###	#本月普通短信通信量
###	if { $today_dd>=25 } {
###	     set handle [aidb_open $conn]
###	     set sqlbuf "
###                    SELECT 
###               	      SUM(BIGINT(SMS_COUNTS)) AS TXL,
###               	      COUNT(DISTINCT PRODUCT_NO)
###                    FROM BASS1.G_S_21008_TO_DAY
###                    WHERE TIME_ID/100=$this_month
###                       AND END_STATUS='0'
###                       AND CDR_TYPE_ID IN('00','01','10','11','21','28')"
###
###	     if [catch {aidb_sql $handle $sqlbuf} errmsg] {
###		WriteTrace $errmsg 001
###		return -1
###	      }
###	      while { [set p_row [aidb_fetch $handle]] != "" } {
###		set CHECK_VAL1 [lindex $p_row 0]
###		set CHECK_VAL2 [lindex $p_row 1]
###		puts $CHECK_VAL1
###		puts $CHECK_VAL2
###	      }
###	      aidb_commit $conn
###	      aidb_close $handle
###	      
###	     set RESULT_VAL1 [format "%.2f" [expr (${CHECK_VAL1}/1.000/${CHECK_VAL2})]]
###	     puts $RESULT_VAL1
###             
###             
###             #将校验值插入校验结果表
###             set handle [aidb_open $conn]
###             set sqlbuf "INSERT INTO bass1.G_RULE_CHECK VALUES
###             		($p_timestamp ,
###             		'63',
###             		cast ($CHECK_VAL1 as  DECIMAL(18, 5) ),
###             		cast ($CHECK_VAL2 as  DECIMAL(18, 5) ),
###             		cast ($RESULT_VAL1 as  DECIMAL(18, 5) ),
###             		0)"
###             if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
###             	WriteTrace $errmsg 003
###             	return -1
###             }
###             aidb_commit $conn
###             aidb_close $handle
###             
###             #判断63: 45 ≤ 本月普通短信通信量/本月普通短信用户数 ≤ 200 （条/户）超标
###	     if { $RESULT_VAL1<45 || $RESULT_VAL1>200 } {
###	        set grade 2
###	        set alarmcontent "准确性指标63超出集团考核范围"
###	        WriteAlarm $APP_NAME $p_optime $grade ${alarmcontent}
###	     } elseif { $RESULT_VAL1<=50 || $RESULT_VAL1>=190 } {
###	        set grade 3
###	        set alarmcontent "准确性指标63接近集团考核范围"
###	        WriteAlarm $APP_NAME $p_optime $grade ${alarmcontent}
###	           
###	    }
###	          
####################################E0：点对点短信普及率######################
#####本月点对点使用用户数(本月占比乘100) 
###	    set handle [aidb_open $conn]
###	    set sqlbuf "
###                    SELECT 
###             	      COUNT(DISTINCT PRODUCT_NO)
###                    FROM BASS1.G_S_21008_TO_DAY
###                    WHERE TIME_ID/100=$this_month
###                      AND END_STATUS='0'
###                      AND SVC_TYPE_ID IN ('11','12','13')
###                      AND CDR_TYPE_ID IN ('00','01','10','11')"
###            puts $sqlbuf
###	    if [catch {aidb_sql $handle $sqlbuf} errmsg] {
###	    	WriteTrace $errmsg 001
###	    	return -1
###	    }
###	    while { [set p_row [aidb_fetch $handle]] != "" } {
###	    	set CHECK_VAL1 [lindex $p_row 0]
###	    }
###	    aidb_commit $conn
###	    aidb_close $handle	
###	    puts "本月点对点使用用户数:$CHECK_VAL1"
####本月到达用户数 
###	    set handle [aidb_open $conn]
###	    set sqlbuf "
###     	          SELECT
###    		COUNT(*)
###    	FROM
###    		(SELECT
###    		 	A.TIME_ID,
###    		 	A.USER_ID,
###    		 	A.USERTYPE_ID,
###    		 	A.SIM_CODE
###    		FROM BASS1.G_A_02004_DAY  A,
###    		(SELECT USER_ID,MAX(TIME_ID) AS TIME_ID FROM BASS1.G_A_02004_DAY
###    	 	 WHERE TIME_ID <= ${p_timestamp}
###    	  	 GROUP BY USER_ID
###    	     ) B
###    	    WHERE A.USER_ID = B.USER_ID AND A.TIME_ID = B.TIME_ID
###    	    )T,
###
###    		(SELECT
###    	 		A.TIME_ID,
###    	 		A.USER_ID,
###    	 		A.USERTYPE_ID
###    		 FROM BASS1.G_A_02008_DAY A,
###    		(SELECT USER_ID,MAX(TIME_ID) AS TIME_ID FROM BASS1.G_A_02008_DAY
###    		 WHERE TIME_ID <= ${p_timestamp}
###    		 GROUP BY USER_ID
###    	     ) B
###    		WHERE A.USER_ID = B.USER_ID AND A.TIME_ID = B.TIME_ID
###    		) M
###   		WHERE T.USER_ID = M.USER_ID
###    		  AND T.TIME_ID <=  ${p_timestamp}
###    		  AND M.TIME_ID <=  ${p_timestamp}
###    		  AND T.USERTYPE_ID <> '3'
###    		  AND T.SIM_CODE <> '1'
###    		  AND M.USERTYPE_ID NOT IN ('2010','2020','2030','1040','1021','9000')"
###            puts  $sqlbuf
###	    if [catch {aidb_sql $handle $sqlbuf} errmsg] {
###	    	WriteTrace $errmsg 001
###	    	return -1
###	    }
###	    while { [set p_row [aidb_fetch $handle]] != "" } {
###	    	set CHECK_VAL2 [lindex $p_row 0]
###	    }
###	    aidb_commit $conn
###	    aidb_close $handle	
####本月点对点短信普及率
####注意：正式程序要屏蔽 
###           puts "本月到达用户数:$CHECK_VAL2"
###    	   set RESULT_VAL1 [format "%.2f" [expr (${CHECK_VAL1}*100.000/${CHECK_VAL2})]]
###       	   puts $RESULT_VAL1
####数据清零
###           set CHECK_VAL1 0.00
###           set CHECK_VAL2 0.00
###        
#####上月点对点使用用户数(上月占比乘100)
####	    set handle [aidb_open $conn]
####	    set sqlbuf "
####                    SELECT 
####             	      COUNT(DISTINCT PRODUCT_NO)
####                    FROM BASS1.G_S_21008_TO_DAY
####                    WHERE TIME_ID/100=$last_month
####                      AND END_STATUS='0'
####                      AND SVC_TYPE_ID IN ('11','12','13')
####                      AND CDR_TYPE_ID IN ('00','01','10','11')"
####            
####	    if [catch {aidb_sql $handle $sqlbuf} errmsg] {
####	    	WriteTrace $errmsg 001
####	    	return -1
####	    }
####	    while { [set p_row [aidb_fetch $handle]] != "" } {
####	    	set CHECK_VAL1 [lindex $p_row 0]
####	    }
####	    aidb_commit $conn
####	    aidb_close $handle	
#####上月到达用户数
#####注意：这段程序要用GETKPI来完成
####           #set CHECK_VAL1 102323
####           set CHECK_VAL2 [exec get_kpi.sh ${last_day_month} 2 2]
###
###	    set handle [aidb_open $conn]
###	    set sqlbuf "
###                    SELECT 
###                      TARGET1 
###   	            FROM 
###   	              BASS1.G_RULE_CHECK 
###   	            WHERE 
###   	              TIME_ID=$last_day_month
###                      AND RULE_CODE='E0'"
###            puts $sqlbuf
###	    if [catch {aidb_sql $handle $sqlbuf} errmsg] {
###	    	WriteTrace $errmsg 001
###	    	return -1
###	    }
###	    while { [set p_row [aidb_fetch $handle]] != "" } {
###	    	set CHECK_VAL2 [lindex $p_row 0]
###	    }
###	    puts "上月点对点短信普及率： $CHECK_VAL2"
###	    aidb_commit $conn
###	    aidb_close $handle	
###             
####上月点对点短信普及率
###           set RESULT_VAL2 $CHECK_VAL2
###           puts "上月点对点短信普及率：$RESULT_VAL2"
###
###           set RESULT [format "%.2f" [expr ($RESULT_VAL1-$RESULT_VAL2)]]
###           puts $RESULT
###           
###           #将校验值插入校验结果表
###           set handle [aidb_open $conn]
###           set sqlbuf "INSERT INTO bass1.G_RULE_CHECK VALUES
###             		($p_timestamp ,
###             		'E0',
###             		cast ($RESULT_VAL1 as  DECIMAL(18, 5) ),
###             		cast ($RESULT_VAL2 as  DECIMAL(18, 5) ),
###             		cast ($RESULT as  DECIMAL(18, 5) ),
###             		0)"
###           if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
###             	WriteTrace $errmsg 003
###             	return -1
###           }
###           aidb_commit $conn
###           aidb_close $handle
###           
###
###           
###           #判断： E0：40％ < (本月点对点短信使用用户数 / 本月到达用户数) < 90%,
###           #            且普及率较上月变动幅度 ≤ 10％超标
###	     if { $RESULT_VAL1<=40 || $RESULT_VAL1>=90 || ($RESULT>10 || $RESULT<-10) } {
###	     	set grade 2
###	        set alarmcontent "准确性指标E0超出集团考核范围"
###	        WriteAlarm $APP_NAME $p_optime $grade ${alarmcontent}	
###	     	   		
###	     } elseif { $RESULT_VAL1<=45 || $RESULT_VAL1>=85 || ($RESULT>8.5 || $RESULT<-8.5) } {
###	     	set grade 3
###	        set alarmcontent "准确性指标E0接近集团考核范围"
###	        WriteAlarm $APP_NAME $p_optime $grade ${alarmcontent}	
###	           
###	    }
###	    
###        }
##################################END#######################
	return 0
}