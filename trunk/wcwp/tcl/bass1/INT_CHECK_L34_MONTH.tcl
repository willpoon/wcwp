#################################################################
#程序名称: INT_CHECK_L34_MONTH.tcl
#功能描述:
#输入参数:
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
#规则编号：L3,L4
#规则属性：业务逻辑
#规则类型：日
#指标摘要：L3：集团个人客户数占比
#          L4：集团高价值客户保有率
#规则描述：L3：10% ≤ 集团个人客户数/用户到达数 ≤ 60%    
#          L4：20% ≤ 集团个人客户中的高价值客户数/高价值客户数 ≤ 80%
#校验对象：
#          BASS1.G_A_02004_DAY    用户
#          BASS1.G_A_02008_DAT    用户状态
#          BASS1.G_I_02049_MONTH  集团用户成员
#          BASS1.G_S_03005_MONTH  综合帐单
#          BASS1.G_S_03012_MONTH  智能网用户明细收入
#输出参数:
# 返回值:   0 成功; -1 失败 
##################################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

        #本月 yyyymm
        set op_month [string range $optime_month 0 3][string range $optime_month 5 6]
        #本月 yyyy-mm
        set opmonth $optime_month
        #上月  yyyymm
        set last_month [GetLastMonth [string range $op_month 0 5]]
        #本月天数
        set this_month_days [GetThisMonthDays ${op_month}01]
        #本月最后一天 yyyymmdd
        set this_month_last_day [string range $op_month 0 5][GetThisMonthDays [string range $op_month 0 5]01]        
        #程序名称
        set app_name "INT_CHECK_L34_MONTH.tcl"

#删除本期数据
        set handle [aidb_open $conn]
	set sql_buff "
		delete from bass1.g_rule_check where time_id=$op_month
    	              and rule_code  in ('L3','L4') "
    	puts $sql_buff
	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
		WriteTrace "$errmsg" 2005
		aidb_close $handle
		return -1
	}
	aidb_commit $conn
	aidb_close $handle        

############################L3：集团个人客户数占比#############
       #集团个人客户数
	set handle [aidb_open $conn]
	set sqlbuf "
	     SELECT
               COUNT(DISTINCT A.USER_ID)
             FROM 
            (SELECT USER_ID  AS USER_ID FROM BASS1.G_I_02049_MONTH
             WHERE TIME_ID=$op_month
             ) A,      
	     ( SELECT
	        A.TIME_ID,
    		A.USER_ID,
    		A.USERTYPE_ID,
    		A.SIM_CODE
    	       FROM BASS1.G_A_02004_DAY  A,
    	       (SELECT USER_ID,MAX(TIME_ID) AS TIME_ID FROM BASS1.G_A_02004_DAY
    	 	WHERE TIME_ID <=$this_month_last_day
    	  	GROUP BY USER_ID
    	       ) B
    	       WHERE A.USER_ID = B.USER_ID AND A.TIME_ID = B.TIME_ID 
    	    )T				
            WHERE A.USER_ID = T.USER_ID
    		AND T.TIME_ID <=$this_month_last_day
    		AND T.USERTYPE_ID <> '3'
    		AND T.SIM_CODE <> '1'"
        puts $sqlbuf
	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
		WriteTrace $errmsg 001
		return -1
	}
	while { [set p_row [aidb_fetch $handle]] != "" } {
		set RESULT_VAL1 [lindex $p_row 0]
	}
	aidb_commit $conn
	aidb_close $handle
	
	#用户到达数
	set RESULT_VAL2 [exec get_kpi.sh $this_month_last_day 2 2]
        puts $RESULT_VAL2
        #将校验值插入校验结果表
	set handle [aidb_open $conn]
	set sqlbuf "INSERT INTO bass1.G_RULE_CHECK VALUES
			($op_month,
			'L3',
			cast ($RESULT_VAL1 as  DECIMAL(18, 5) ),
			cast ($RESULT_VAL2 as  DECIMAL(18, 5) ),
			0,
			0)"
        puts $sqlbuf			
	if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
		WriteTrace $errmsg 003
		return -1
	}
	aidb_commit $conn
	aidb_close $handle
        
        set RESULT [format "%.2f" [expr (${RESULT_VAL1} /1.00000/ ${RESULT_VAL2})]]
        
	#判断L3：10% ≤ 集团个人客户数/用户到达数 ≤ 60%超标
	#根据2007年校验规则L3的范围从10%-60%调整到8%-23% @20070517 By TYM
	if { $RESULT<0.08 ||$RESULT>0.23  } {	
	     	set grade 2
	        set alarmcontent "准确性指标L3超出集团考核范围"
	        WriteAlarm $app_name $opmonth $grade ${alarmcontent}
        }

############################L4：集团高价值客户保有率#############
       #集团个人客户中的高价值客户数
	set handle [aidb_open $conn]
	set sqlbuf "
	    SELECT
               COUNT(DISTINCT A.USER_ID)
            FROM 
            (
        	SELECT USER_ID AS USER_ID FROM BASS1.G_I_02049_MONTH
        	WHERE TIME_ID =$op_month
            ) A,      
            ( SELECT  USER_ID AS USER_ID FROM ( 
	                            SELECT USER_ID FROM bass1.G_S_03005_MONTH
 	                            WHERE TIME_ID =$op_month
                                    GROUP BY USER_ID
                                    HAVING SUM(INT(SHOULD_FEE))/700 >=$this_month_days
                                    UNION 
                                    SELECT USER_ID AS USER_ID  FROM bass1.G_S_03012_MONTH
                                    WHERE TIME_ID = $op_month
                                    GROUP BY USER_ID
                                    HAVING SUM(INT(INCM_AMT))/700 >= $this_month_days
                                   )T	    
          )B
          WHERE A.USER_ID = B.USER_ID"
        puts $sqlbuf
	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
		WriteTrace $errmsg 001
		return -1
	}
	while { [set p_row [aidb_fetch $handle]] != "" } {
		set RESULT_VAL1 [lindex $p_row 0]
	}
	aidb_commit $conn
	aidb_close $handle
	
	#高价值客户数
	set handle [aidb_open $conn]
	set sqlbuf "
		SELECT  
            	COUNT(DISTINCT A.USER_ID)
                FROM 
	           (
	            SELECT USER_ID FROM BASS1.G_S_03005_MONTH
 	            WHERE TIME_ID =$op_month
                    GROUP BY USER_ID
                    HAVING SUM(INT(SHOULD_FEE))/700 >=$this_month_days
	            UNION ALL
	            SELECT USER_ID FROM BASS1.G_S_03012_MONTH
 	            WHERE TIME_ID =$op_month
                    GROUP BY USER_ID
                    HAVING SUM(INT(INCM_AMT))/700 >=$this_month_days
	           )A,
		   ( SELECT
		       A.TIME_ID,
    		       A.USER_ID,
    		       A.USERTYPE_ID,
    		       A.SIM_CODE
    		     FROM BASS1.G_A_02004_DAY  A,
    		           (SELECT USER_ID,MAX(TIME_ID) AS TIME_ID FROM BASS1.G_A_02004_DAY
    	 	            WHERE TIME_ID <= $this_month_last_day
    	  	            GROUP BY USER_ID
    	                    )B
    	             WHERE A.USER_ID = B.USER_ID AND A.TIME_ID = B.TIME_ID 
    	            )T					
                WHERE A.USER_ID = T.USER_ID
    		  AND T.TIME_ID <= $this_month_last_day
    		  AND T.USERTYPE_ID <> '3'
    		  AND T.SIM_CODE <> '1'"
        puts $sqlbuf
	if [catch {aidb_sql $handle $sqlbuf} errmsg] {
		WriteTrace $errmsg 001
		return -1
	}
	while { [set p_row [aidb_fetch $handle]] != "" } {
		set RESULT_VAL2 [lindex $p_row 0]
	}
	aidb_commit $conn
	aidb_close $handle


        #将校验值插入校验结果表
	set handle [aidb_open $conn]
	set sqlbuf "INSERT INTO bass1.G_RULE_CHECK VALUES
			($op_month ,
			'L4',
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
        
        set RESULT [format "%.2f" [expr (${RESULT_VAL1} /1.00000/ ${RESULT_VAL2})]]
        
	#判断L4：20% ≤ 集团个人客户中的高价值客户数/高价值客户数 ≤ 80%超标
	if { $RESULT<0.20 ||$RESULT>0.80  } {	
	     	set grade 2
	        set alarmcontent "准确性指标L4超出集团考核范围"
	        WriteAlarm $app_name $opmonth $grade ${alarmcontent}
        }
##################################END#######################
	return 0
}