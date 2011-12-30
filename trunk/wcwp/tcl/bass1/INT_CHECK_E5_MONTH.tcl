#################################################################
#程序名称: INT_CHECK_E5_MONTH.tcl
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
#规则编号：E5
#规则属性：业务逻辑
#规则类型：月
#指标摘要：E5: WAP付费用户数占比
#规则描述：E5: WAP付费用户数/新业务付费用户数>0，且占比较上月变动率<30%
#校验对象：
#          BASS1.G_S_04006_DAY     梦网WAP话单
#          BASS1.G_A_02004_DAY     用户
#          BASS1.G_S_03004_MONTH   明细帐单
#          BASS1.G_S_03012_MONTH   智能网用户明细收入
#输出参数:
#返 回 值:   0 成功; -1 失败
#修改:   指标考核不正确
#        将 if { $RESULT_VAL1<=0 || $RESULT<0.30 ||$RESULT>-0.30 } 改为
#  	     if { $RESULT_VAL1<=0 || $RESULT<-0.30||$RESULT>0.30  }
#
#################################################################
proc Deal { op_time optime_month province_id redo_number trace_fd bass1_dir temp_data_dir semi_data_dir final_data_dir conn conn_ctl src_data obj_data final_data } {

        #本月 yyyymm
        set op_month [string range $optime_month 0 3][string range $optime_month 5 6]
        #本月 yyyy-mm
        set opmonth $optime_month
        #本月最后一天 yyyymmdd
        set this_month_last_day [string range $op_month 0 5][GetThisMonthDays [string range $op_month 0 5]01]        
        #本月第一天 yyyymmdd
        set this_month_first_day [string range $optime_month 0 3][string range $optime_month 5 6]01
        #上月  yyyymm
        set last_month [GetLastMonth [string range $op_month 0 5]]
        #上月最后一天 yyyymmdd
        set last_month [GetLastMonth [string range $op_month 0 5]]
        set last_month_last_day [GetLastDay [string range $this_month_first_day 0 7]]
        #程序名称
        set app_name "INT_CHECK_E5_MONTH.tcl"
        
#####删除本期数据
####        set handle [aidb_open $conn]
####	set sql_buff "\
####		delete from bass1.g_rule_check where time_id=$op_month
####    	              and rule_code='E5' "
####	if [catch { aidb_sql $handle $sql_buff } errmsg ] {
####		WriteTrace "$errmsg" 2005
####		aidb_close $handle
####		return -1
####	}
####	aidb_commit $conn
####	aidb_close $handle
####
####        #本月占比乘100	
####        #WAP付费用户数
####	     set handle [aidb_open $conn]
####	     set sqlbuf "
####	           SELECT 
####             	     COUNT(DISTINCT A.PRODUCT_NO)
####                   FROM  bass1.G_S_04006_DAY A, 
####                   (
####             	     SELECT A.TIME_ID,A.PRODUCT_NO,A.USERTYPE_ID
####                     FROM bass1.G_A_02004_DAY A,
####                          (    
####            		    SELECT PRODUCT_NO,MAX(TIME_ID) AS TIME_ID FROM bass1.G_A_02004_DAY
####                            WHERE TIME_ID <=$this_month_last_day
####                            GROUP BY PRODUCT_NO
####            		  ) B
####                     WHERE A.PRODUCT_NO = B.PRODUCT_NO
####                        AND A.TIME_ID = B.TIME_ID
####             	   )B
####            	   WHERE A.TIME_ID/100=$op_month
####           	        AND A.PRODUCT_NO=B.PRODUCT_NO 
####            	        AND BIGINT(A.WAP_BASEFEE)+BIGINT(A.INFO_FEE)+BIGINT(A.MONTH_FEE)>0
####            	        AND B.USERTYPE_ID <> '3'"
####             puts $sqlbuf
####	     if [catch {aidb_sql $handle $sqlbuf} errmsg] {
####		WriteTrace $errmsg 001
####		return -1
####	      }
####	      while { [set p_row [aidb_fetch $handle]] != "" } {
####		set CHECK_VAL1 [lindex $p_row 0]
####	      }
####	      aidb_commit $conn
####	      aidb_close $handle
####	      
####	     puts "本月WAP付费用户数=$CHECK_VAL1"
####       #新业务付费用户数
####	     set handle [aidb_open $conn]
####	     set sqlbuf "
####                   SELECT
####                     COUNT(DISTINCT A.USER_ID)
####                   FROM 
####                   (
####             	    SELECT A.USER_ID FROM BASS1.G_S_03004_MONTH A
####             	    WHERE BIGINT(A.FEE_RECEIVABLE)>0
####             	        AND A.TIME_ID =$op_month
####             	        AND (A.ACCT_ITEM_ID IN ('0405','0407') OR INT(A.ACCT_ITEM_ID)/100 IN (5,6,7))
####             	    UNION ALL
####                    SELECT A.USER_ID FROM BASS1.G_S_03012_MONTH A
####	            WHERE A.TIME_ID =$op_month
####			AND BIGINT(A.INCM_AMT)>0
####			AND (A.ACCT_ITEM_ID IN ('0405','0407') OR INT(A.ACCT_ITEM_ID)/100 IN (5,6,7))
####	           ) A, 
####	            
####                  (
####             	   SELECT A.TIME_ID,A.USER_ID,A.USERTYPE_ID,A.SIM_CODE
####                   FROM BASS1.G_A_02004_DAY A,
####                   (    
####            	      SELECT USER_ID,MAX(TIME_ID) AS TIME_ID FROM BASS1.G_A_02004_DAY
####                      WHERE TIME_ID <=$this_month_last_day
####                      GROUP BY USER_ID
####                   ) B
####                   WHERE A.USER_ID = B.USER_ID
####                    AND A.TIME_ID = B.TIME_ID
####                 )B
####                 WHERE A.USER_ID = B.USER_ID
####            	    AND B.USERTYPE_ID <> '3'
####            	    AND B.SIM_CODE <> '1'"
####             puts $sqlbuf
####	     if [catch {aidb_sql $handle $sqlbuf} errmsg] {
####		WriteTrace $errmsg 001
####		return -1
####	      }
####	      while { [set p_row [aidb_fetch $handle]] != "" } {
####		set CHECK_VAL2 [lindex $p_row 0]
####	      }
####	      aidb_commit $conn
####	      aidb_close $handle
####	      
####	     puts "本月新业务付费用户数=$CHECK_VAL2"
####	    set RESULT_VAL1 [format "%.5f" [expr (${CHECK_VAL1}/1.0000/${CHECK_VAL2}*100.00000)]]
####	
####           #数据清零
####           set CHECK_VAL1 0.00
####           set CHECK_VAL2 0.00
####
####        #上月占比乘100	
####        #WAP付费用户数
####	     set handle [aidb_open $conn]
####	     set sqlbuf "
####	           SELECT 
####             	     COUNT(DISTINCT A.PRODUCT_NO)
####                   FROM  bass1.G_S_04006_DAY A, 
####                   (
####             	     SELECT A.TIME_ID,A.PRODUCT_NO,A.USERTYPE_ID
####                     FROM bass1.G_A_02004_DAY A,
####                          (    
####            		    SELECT PRODUCT_NO,MAX(TIME_ID) AS TIME_ID FROM bass1.G_A_02004_DAY
####                            WHERE TIME_ID <=$last_month_last_day
####                            GROUP BY PRODUCT_NO
####            		  ) B
####                     WHERE A.PRODUCT_NO = B.PRODUCT_NO
####                        AND A.TIME_ID = B.TIME_ID
####             	   )B
####            	   WHERE A.TIME_ID/100=$last_month
####           	        AND A.PRODUCT_NO=B.PRODUCT_NO 
####            	        AND BIGINT(A.WAP_BASEFEE)+BIGINT(A.INFO_FEE)+BIGINT(A.MONTH_FEE)>0
####            	        AND B.USERTYPE_ID <> '3'"
####             puts $sqlbuf
####	     if [catch {aidb_sql $handle $sqlbuf} errmsg] {
####		WriteTrace $errmsg 001
####		return -1
####	      }
####	      while { [set p_row [aidb_fetch $handle]] != "" } {
####		set CHECK_VAL1 [lindex $p_row 0]
####	      }
####	      aidb_commit $conn
####	      aidb_close $handle
####	      
####	      puts "上月WAP付费用户数=$CHECK_VAL1"
####       #新业务付费用户数
####	     set handle [aidb_open $conn]
####	     set sqlbuf "
####                   SELECT
####                     COUNT(DISTINCT A.USER_ID)
####                   FROM 
####                   (
####             	    SELECT A.USER_ID FROM BASS1.G_S_03004_MONTH A
####             	    WHERE BIGINT(A.FEE_RECEIVABLE)>0
####             	        AND A.TIME_ID =$last_month
####             	        AND (A.ACCT_ITEM_ID IN ('0405','0407') OR INT(A.ACCT_ITEM_ID)/100 IN (5,6,7))
####             	    UNION ALL
####                    SELECT A.USER_ID FROM BASS1.G_S_03012_MONTH A
####	            WHERE A.TIME_ID =$last_month
####			AND BIGINT(A.INCM_AMT)>0
####			AND (A.ACCT_ITEM_ID IN ('0405','0407') OR INT(A.ACCT_ITEM_ID)/100 IN (5,6,7))
####	           ) A, 
####	            
####                  (
####             	   SELECT A.TIME_ID,A.USER_ID,A.USERTYPE_ID,A.SIM_CODE
####                   FROM BASS1.G_A_02004_DAY A,
####                   (    
####            	      SELECT USER_ID,MAX(TIME_ID) AS TIME_ID FROM BASS1.G_A_02004_DAY
####                      WHERE TIME_ID <=$last_month_last_day
####                      GROUP BY USER_ID
####                   ) B
####                   WHERE A.USER_ID = B.USER_ID
####                    AND A.TIME_ID = B.TIME_ID
####                 )B
####                 WHERE A.USER_ID = B.USER_ID
####            	    AND B.USERTYPE_ID <> '3'
####            	    AND B.SIM_CODE <> '1'"
####              puts $sqlbuf
####	     if [catch {aidb_sql $handle $sqlbuf} errmsg] {
####		WriteTrace $errmsg 001
####		return -1
####	      }
####	      while { [set p_row [aidb_fetch $handle]] != "" } {
####		set CHECK_VAL2 [lindex $p_row 0]
####	      }
####	      aidb_commit $conn
####	      aidb_close $handle
####	      
####	     puts "本月新业务付费用户数=$CHECK_VAL2"
####	    set RESULT_VAL2 [format "%.5f" [expr (${CHECK_VAL1}/1.00000/${CHECK_VAL2}*100.00000)]]
####
####             #将校验值插入校验结果表
####             set handle [aidb_open $conn]
####             set sqlbuf "INSERT INTO bass1.G_RULE_CHECK VALUES
####             		($op_month,
####             		'E5',
####             		cast ($RESULT_VAL1 as  DECIMAL(18, 5) ),
####             		cast ($RESULT_VAL2 as  DECIMAL(18, 5) ),
####             		0.00,
####             		0.00)"
####            puts $sqlbuf
####             if [catch { aidb_sql $handle $sqlbuf } errmsg ] {
####             	WriteTrace $errmsg 003
####             	return -1
####             }
####             aidb_commit $conn
####             aidb_close $handle	
####             
####             set RESULT [format "%.5f" [expr (${RESULT_VAL1}/1.00000/${RESULT_VAL2}-1.00000)]]
####             puts $RESULT
####             
####             #判断E5：WAP付费用户数/新业务付费用户数>0，且占比较上月变动率<30%超标
####	     if { $RESULT_VAL1<=0 || $RESULT<-0.30||$RESULT>0.30  } {	
####	     	puts '1111'
####	     	set grade 2
####	        set alarmcontent "准确性指标E5超出集团考核范围"
####	        WriteAlarm $app_name $opmonth $grade ${alarmcontent}  
####	        puts '222'  		
####	     }            
######################################END#######################
	return 0
}	    	     