#################################################################
# 程序名称: INT_CHECK_E1_DAY.tcl
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
#规则编号：E1
#规则属性：业务逻辑
#规则类型：日
#指标摘要：E1：点对点短信普及率改为彩铃用户数占比
#规则描述：E1: 10% ≤ 彩铃用户数/用户到达数 ≤ 50%
#校验对象：
#          1.BASS1.G_A_02004_DAY     用户
#          2.BASS1.G_A_02008_DAY     用户状态
#          3.BASS1.G_A_02011_DAY     用户开通业务功能历史
#输入参数:
#输出参数: 返回值:0 成功;-1 失败
#编 写 人：王琦
#编写时间：2007-03-22
#问题记录：1.
#修改历史: 1.
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
        #本月第一天 yyyymmdd
        set this_month_first_day [string range $optime_month 0 3][string range $optime_month 5 6]01
        #下月第一天 yyyymmdd
        set next_month_first_day [string range $this_month 0 5]01
        ##今天的日期，格式dd(例：输入20070411 返回11)
        set today_dd [format "%.0f" [string range $p_timestamp 6 7]]
        #puts $today_dd

        #程序名称
        set app_name "INT_CHECK_E1_DAY.tcl"


##删除本期数据
#  set handle [aidb_open $conn]
#	set sql_buff "
#		DELETE FROM $db_user.G_RULE_CHECK WHERE TIME_ID=$p_timestamp
#    	              AND RULE_CODE='E1' "
#	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
#		WriteTrace "$errmsg" 2005
#		aidb_close $handle
#		return -1
#	}
#	aidb_commit $conn
#	aidb_close $handle
###################################E1：点对点短信普及率改为彩铃用户数占比################
#	#1-24号直接返回，25号开始判断
#	#彩铃用户数
#	if { $today_dd>=20 } {
#	
#	
#	
#	set handle [aidb_open $conn]
#	set sql_buff "ALTER TABLE bass1.check_02011_e1_mid ACTIVATE NOT LOGGED INITIALLY WITH EMPTY TABLE"
#	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
#		WriteTrace "$errmsg" 2005
#		aidb_close $handle
#		return -1
#	}
#	aidb_commit $conn
#	aidb_close $handle
#	
#	
#	set handle [aidb_open $conn]
#	
#	set sqlbuf "insert into bass1.check_02011_e1_mid 
#	SELECT 
#               	     A.USER_ID
#                     FROM bass1.G_A_02011_DAY A,
#                    (
#                      SELECT USER_ID,MAX(TIME_ID) AS TIME_ID FROM bass1.G_A_02011_DAY
#	    			  WHERE TIME_ID<=$p_timestamp 
#	    			       AND int(VALID_DATE)<$this_month_first_day
#	    			       AND int(INVALID_DATE)>=$next_month_first_day
#	                   GROUP  BY USER_ID
#                     )B
#                     WHERE  A.TIME_ID = B.TIME_ID
#                        AND A.USER_ID = B.USER_ID
#                        AND BUSI_CODE IN ('370')  with ur"
#              puts $sqlbuf
#	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
#		WriteTrace "$errmsg" 2005
#		aidb_close $handle
#		return -1
#	}
#	aidb_commit $conn
#	aidb_close $handle
#	
#	
#		set handle [aidb_open $conn]
#	
#	  set sqlbuf "ALTER TABLE bass1.check_02004_e1_mid ACTIVATE NOT LOGGED INITIALLY WITH EMPTY TABLE"
#              puts $sqlbuf
#	 	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
#		WriteTrace "$errmsg" 2005
#		aidb_close $handle
#		return -1
#	}
#	aidb_commit $conn
#	aidb_close $handle
#	
#	set handle [aidb_open $conn]
#	
#	set sqlbuf "insert into bass1.check_02004_e1_mid 
#	SELECT
#    		 	    A.TIME_ID,
#    		 	    A.USER_ID,
#    		 	    A.USERTYPE_ID,
#    		 	    A.SIM_CODE
#    		       FROM bass1.G_A_02004_DAY  A,
#    		      (
#    		       SELECT USER_ID,MAX(TIME_ID) AS TIME_ID FROM bass1.G_A_02004_DAY
#    	 	       WHERE TIME_ID <=$p_timestamp 
#    	  	       GROUP BY USER_ID
#    	         ) B
#    	              WHERE A.USER_ID = B.USER_ID AND A.TIME_ID = B.TIME_ID with ur"
#              puts $sqlbuf
#	 	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
#		WriteTrace "$errmsg" 2005
#		aidb_close $handle
#		return -1
#	}
#	aidb_commit $conn
#	aidb_close $handle
#	      
#
#		set handle [aidb_open $conn]
#	
#	  set sqlbuf "ALTER TABLE bass1.check_02008_e1_mid ACTIVATE NOT LOGGED INITIALLY WITH EMPTY TABLE"
#              puts $sqlbuf
#	  	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
#		WriteTrace "$errmsg" 2005
#		aidb_close $handle
#		return -1
#	}
#	aidb_commit $conn
#	aidb_close $handle
#	
#	set handle [aidb_open $conn]
#	
#	set sqlbuf "insert into bass1.check_02008_e1_mid 
#	 SELECT
#    	 		    A.TIME_ID,
#    	 		    A.USER_ID,
#    	 		    A.USERTYPE_ID
#    		      FROM bass1.G_A_02008_DAY A,
#    		      ( SELECT USER_ID,MAX(TIME_ID) AS TIME_ID FROM bass1.G_A_02008_DAY
#    		        WHERE TIME_ID <=$p_timestamp
#    		        GROUP BY USER_ID
#    	         ) B
#    		     WHERE A.USER_ID = B.USER_ID AND A.TIME_ID = B.TIME_ID  with ur"
#              puts $sqlbuf
#	  	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
#		WriteTrace "$errmsg" 2005
#		aidb_close $handle
#		return -1
#	}
#	aidb_commit $conn
#	aidb_close $handle
#	      	
#	     set handle [aidb_open $conn]
#	     set sqlbuf "
#               SELECT COUNT(DISTINCT P.USER_ID) FROM 
#                  bass1.check_02011_e1_mid P,  
#		             ( SELECT  T.USER_ID 
#    	                       FROM  bass1.check_02004_e1_mid T, 	    
#    		                           bass1.check_02008_e1_mid  M		
#   		             WHERE T.USER_ID = M.USER_ID
#    		              AND T.TIME_ID <=$p_timestamp 
#    		              AND M.TIME_ID <=$p_timestamp 
#    		              AND T.USERTYPE_ID <> '3'
#    		              AND T.SIM_CODE <> '1'		
#    		              AND M.USERTYPE_ID NOT IN ('2010','2020','2030','1040','1021','9000')
#    		         )L
#          WHERE P.USER_ID=L.USER_ID"
#              puts $sqlbuf
# if [catch {aidb_sql $handle $sqlbuf} errmsg] {
#		WriteTrace $errmsg 001
#		return -1
#	      }
#	      while { [set p_row [aidb_fetch $handle]] != "" } {
#		set CHECK_VAL1 [lindex $p_row 0]
#	      }
#	      aidb_commit $conn
#	      aidb_close $handle
#	      
##用户到达数	      
#	     set handle [aidb_open $conn]
#	     set sqlbuf "
#               SELECT 
#       	       	 COUNT(*) 
#       	       FROM 
#       	       	bass1.check_02004_e1_mid T, 	
#       	        bass1.check_02008_e1_mid M		
#      	       	WHERE T.USER_ID = M.USER_ID
#       	       	  AND T.TIME_ID <=$p_timestamp
#       	       	  AND M.TIME_ID <=$p_timestamp
#       	       	  AND T.USERTYPE_ID <> '3'
#       	       	  AND T.SIM_CODE <> '1'
#       	       	  AND M.USERTYPE_ID NOT IN ('2010','2020','2030','1040','1021','9000')"
#
#	     if [catch {aidb_sql $handle $sqlbuf} errmsg] {
#		WriteTrace $errmsg 001
#		return -1
#	      }
#	      while { [set p_row [aidb_fetch $handle]] != "" } {
#		set CHECK_VAL2 [lindex $p_row 0]
#	      }
#	      aidb_commit $conn
#	      aidb_close $handle
#	      
#	      set RESULT_VAL1 $CHECK_VAL1
#	      set RESULT_VAL2 $CHECK_VAL2
#	      
#             #将校验值插入校验结果表
#             set handle [aidb_open $conn]
#             set sqlbuf "INSERT INTO bass1.G_RULE_CHECK VALUES
#             		($p_timestamp ,
#             		'E1',
#             		cast ($RESULT_VAL1 as  DECIMAL(18, 5) ),
#             		cast ($RESULT_VAL2 as  DECIMAL(18, 5) ),
#             		0,
#             		0)"
#             if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
#             	WriteTrace $errmsg 003
#             	return -1
#             }
#             aidb_commit $conn
#             aidb_close $handle
#            
#            
#            set RESULT [format "%.2f" [expr (${RESULT_VAL1}/1.000/${RESULT_VAL2})]]
#             #判断E1:10% ≤ 彩铃用户数/用户到达数 ≤ 50%超标
#	     if { $RESULT<0.10 || $RESULT>0.50 } {	
#	     set grade 2
#	     set alarmcontent "准确性指标E1超出集团考核范围"
#	     WriteAlarm $app_name $op_time $grade ${alarmcontent}     	
#             }
#       }
#       puts "E1结束"
###################################END#######################



  #进行02011主键唯一性检查
  set handle [aidb_open $conn]
	set sql_buff "select count(*) from 
	            (
	             select busi_code,user_id,count(*) cnt from bass1.g_a_02011_day
	              where time_id =$p_timestamp
	             group by busi_code,user_id
	             having count(*)>1
	            ) as a
	            "
	puts $sql_buff
	 if [catch { aidb_sql $handle $sql_buff  } errmsg ] {
		WriteTrace $errmsg 1001
		return -1
	}
	if [catch {set DEC_RESULT_VAL2 [lindex [aidb_fetch $handle] 0]} errmsg ] {
		WriteTrace $errmsg 1002
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
	
	set DEC_RESULT_VAL2 [format "%.3f" [expr ${DEC_RESULT_VAL2} /1.00]]

	puts $DEC_RESULT_VAL2

	if {[format %.3f [expr ${DEC_RESULT_VAL2} ]]>0 } {
		set grade 2
	        set alarmcontent "02011接口主键唯一性校验未通过"
	        WriteAlarm $app_name $op_time $grade ${alarmcontent}
	   }











	return 0
}

